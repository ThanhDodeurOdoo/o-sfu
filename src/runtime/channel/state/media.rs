use std::collections::BTreeMap;

use o_sfu_router::{
    ConsumerCapability, MediaCapabilities, MediaKind as RouterMediaKind,
    RtpParameters as RouterRtpParameters, StreamType as RouterStreamType, can_consume,
    negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::shared::{DownloadStates, SessionId, SessionInfo, StreamType};

use super::super::{
    ChannelEventMessage, ChannelEventRequest, SessionOutbound, TrackBindingUpdate,
    outbound::{MessageFanout, OutboundSender},
    topology::RoutedProducerId,
};
use super::{
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    shared::{
        ChannelState, ConsumerKey, ConsumerState, ProducerKey, PublishedProducer,
        TransportMediaRemoval,
    },
};

#[allow(
    clippy::struct_field_names,
    reason = "postfix _id is intentional because the fields are all identity values"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ProducerRouteTarget {
    producer_id: ProducerRuntimeId,
    owner_connection_id: u64,
    routed_producer_id: RoutedProducerId,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrapTarget {
    consumer_session_id: SessionId,
    consumer_connection_id: u64,
    producer_session_id: SessionId,
    producer_connection_id: u64,
    producer_id: ProducerRuntimeId,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PreparedConsumerBootstrap {
    consumer_rtp_parameters: RouterRtpParameters,
    sender: OutboundSender,
    producer_owner_session_id: SessionId,
    producer_connection_id: u64,
    producer_stream_type: StreamType,
    producer_media_kind: RouterMediaKind,
    producer_routed_id: RoutedProducerId,
    producer_id: ProducerRuntimeId,
    producer_active: bool,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PreparedPublishedTrack {
    owner_session_id: SessionId,
    owner_connection_id: u64,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
    consumable_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrap {
    sender: OutboundSender,
    bootstrap: RemoteTrackBootstrap,
    producer_owner_session_id: SessionId,
    producer_connection_id: u64,
    producer_stream_type: StreamType,
    producer_media_kind: RouterMediaKind,
    producer_routed_id: RoutedProducerId,
    producer_id: ProducerRuntimeId,
    producer_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteTrackBootstrap {
    consumer_id: ConsumerRuntimeId,
    media_kind: RouterMediaKind,
    producer_id: ProducerRuntimeId,
    rtp_parameters: RouterRtpParameters,
    session_id: SessionId,
    active: bool,
    stream_type: StreamType,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::channel) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PublishPrerequisites {
    connection_id: u64,
    router_capabilities: MediaCapabilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerRouteUpdate {
    consumer_state: ConsumerState,
    stream_type: StreamType,
    active: bool,
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct ProducerActivityOutcome {
    pub(in crate::runtime::channel) transport_media_id: TransportMediaId,
    pub(in crate::runtime::channel) active: bool,
    pub(in crate::runtime::channel) fanout: MessageFanout,
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct UnpublishTrackOutcome {
    recipients: Vec<OutboundSender>,
    pub(in crate::runtime::channel) transport_removals: Vec<TransportMediaRemoval>,
    session_info_snapshot: Option<BTreeMap<SessionId, SessionInfo>>,
}

impl ChannelState {
    pub(in crate::runtime::channel) fn publish_prerequisites(
        &self,
        session_id: &SessionId,
    ) -> Option<PublishPrerequisites> {
        let session = self.sessions.get(session_id)?;
        if !session.negotiation.can_publish() {
            return None;
        }
        Some(PublishPrerequisites {
            connection_id: session.connection_id,
            router_capabilities: self.topology.rtp_capabilities().clone(),
        })
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn missing_consumer_targets(
        &self,
        session_id: &SessionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        let Some(session) = self.sessions.get(session_id) else {
            return Vec::new();
        };
        if !session.negotiation.can_consume() {
            return Vec::new();
        }
        self.collect_missing_consumer_targets(session_id, session.connection_id)
    }

    pub(in crate::runtime::channel) fn missing_consumer_targets_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> Option<Vec<PendingConsumerBootstrapTarget>> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        if !session.negotiation.can_consume() {
            return Some(Vec::new());
        }
        Some(self.collect_missing_consumer_targets(session_id, connection_id))
    }

    fn collect_missing_consumer_targets(
        &self,
        session_id: &SessionId,
        consumer_connection_id: u64,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                if producer.owner_session_id == *session_id {
                    return None;
                }
                if self.consumer_index.contains_key(&ConsumerKey {
                    consumer_session_id: session_id.clone(),
                    producer_session_id: producer.owner_session_id.clone(),
                    stream_type: producer.stream_type,
                }) {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: session_id.clone(),
                    consumer_connection_id,
                    producer_session_id: producer.owner_session_id.clone(),
                    producer_connection_id: producer.owner_connection_id,
                    producer_id: *producer_id,
                    stream_type: producer.stream_type,
                    media_kind: producer.media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(in crate::runtime::channel) fn publish_consumer_targets(
        &self,
        producer_session_id: &SessionId,
        producer_connection_id: u64,
        producer_id: ProducerRuntimeId,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
        transport_media_id: TransportMediaId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.sessions
            .iter()
            .filter_map(|(peer_session_id, peer_session)| {
                if peer_session_id == producer_session_id || !peer_session.negotiation.can_consume()
                {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: peer_session_id.clone(),
                    consumer_connection_id: peer_session.connection_id,
                    producer_session_id: producer_session_id.clone(),
                    producer_connection_id,
                    producer_id,
                    stream_type,
                    media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(in crate::runtime::channel) fn prepare_published_track(
        &self,
        session_id: &SessionId,
        publisher_connection_id: u64,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<PreparedPublishedTrack> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != publisher_connection_id || !session.negotiation.can_publish() {
            return None;
        }
        Some(PreparedPublishedTrack {
            owner_session_id: session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type,
            media_kind,
            consumable_rtp_parameters,
        })
    }

    pub(in crate::runtime::channel) fn commit_published_track(
        &mut self,
        pending: PreparedPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(ProducerRuntimeId, Vec<PendingConsumerBootstrapTarget>)> {
        let session = self.sessions.get(&pending.owner_session_id)?;
        if session.connection_id != pending.owner_connection_id
            || !session.negotiation.can_publish()
        {
            return None;
        }
        let producer_id = ProducerRuntimeId::allocate(&mut self.next_producer_id);
        let routed_producer_id = match self.topology.add_producer(
            &pending.owner_session_id,
            pending.media_kind,
            to_router_stream_type(pending.stream_type),
        ) {
            Ok(producer_id) => producer_id,
            Err(_error) => {
                error!(
                    session_id = ?pending.owner_session_id,
                    "failed to mirror publish request into channel router producer state"
                );
                return None;
            }
        };
        self.producers.insert(
            producer_id,
            PublishedProducer {
                owner_session_id: pending.owner_session_id.clone(),
                owner_connection_id: pending.owner_connection_id,
                stream_type: pending.stream_type,
                media_kind: pending.media_kind,
                consumable_rtp_parameters: pending.consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                source_packet_selection: None,
                active: true,
            },
        );
        self.producer_ids_by_owner_stream.insert(
            ProducerKey::new(&pending.owner_session_id, pending.stream_type),
            producer_id,
        );
        self.producer_stream_types_by_transport_media_id
            .insert(transport_media_id, pending.stream_type);
        let consumer_targets = self.publish_consumer_targets(
            &pending.owner_session_id,
            pending.owner_connection_id,
            producer_id,
            pending.stream_type,
            pending.media_kind,
            transport_media_id,
        );
        Some((producer_id, consumer_targets))
    }

    #[must_use]
    pub(in crate::runtime::channel) fn producer_stream_type_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.producer_stream_types_by_transport_media_id
            .get(&transport_media_id)
            .copied()
    }

    #[must_use]
    pub(in crate::runtime::channel) fn producer_route_target(
        &self,
        owner_session_id: &SessionId,
        owner_connection_id: u64,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let producer_id = *self
            .producer_ids_by_owner_stream
            .get(&ProducerKey::new(owner_session_id, stream_type))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id != owner_connection_id {
            return None;
        }
        let transport_media_id = producer.transport_media_id?;
        Some(ProducerRouteTarget {
            producer_id,
            owner_connection_id: producer.owner_connection_id,
            routed_producer_id: producer.routed_producer_id,
            transport_media_id,
        })
    }

    pub(in crate::runtime::channel) fn producer_route_target_for_session(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let connection_id = self.session_connection_id(session_id)?;
        self.producer_route_target(session_id, connection_id, stream_type)
    }

    pub(in crate::runtime::channel) fn unpublish_transport_removals(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
    ) -> Option<Vec<TransportMediaRemoval>> {
        let producer_target = self.producer_route_target(session_id, connection_id, stream_type)?;
        let mut transport_removals = vec![TransportMediaRemoval {
            session: session_id.clone(),
            connection: connection_id,
            transport_media: producer_target.transport_media_id,
        }];
        transport_removals.extend(self.consumer_index.iter().filter_map(
            |(key, consumer_state)| {
                if key.producer_session_id != *session_id || key.stream_type != stream_type {
                    return None;
                }
                Some(TransportMediaRemoval {
                    session: key.consumer_session_id.clone(),
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            },
        ));
        Some(transport_removals)
    }

    pub(in crate::runtime::channel) fn unpublish_track(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
        transport_removals: Vec<TransportMediaRemoval>,
    ) -> Option<UnpublishTrackOutcome> {
        let producer_target = self.producer_route_target(session_id, connection_id, stream_type)?;
        if self
            .topology
            .remove_producer(producer_target.routed_producer_id)
            .is_err()
        {
            error!(
                ?session_id,
                ?stream_type,
                "failed to remove published track from channel router"
            );
            return None;
        }
        self.consumer_index.retain(|key, _consumer_state| {
            key.producer_session_id != *session_id || key.stream_type != stream_type
        });
        let removed_producer = self.producers.remove(&producer_target.producer_id)?;
        self.producer_ids_by_owner_stream
            .remove(&ProducerKey::new(session_id, stream_type));
        if let Some(transport_media_id) = removed_producer.transport_media_id {
            self.producer_stream_types_by_transport_media_id
                .remove(&transport_media_id);
        }
        let session_info_snapshot = match stream_type {
            StreamType::Camera | StreamType::Screen => {
                Some(BTreeMap::from([self.session_info_snapshot(session_id)?]))
            }
            StreamType::Audio => None,
        };
        Some(UnpublishTrackOutcome {
            recipients: self
                .sessions
                .values()
                .map(|session| session.sender.clone())
                .collect(),
            transport_removals,
            session_info_snapshot,
        })
    }

    pub(in crate::runtime::channel) fn prepare_consumer_bootstrap_transaction(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
    ) -> Option<PendingConsumerBootstrap> {
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.negotiation.can_consume()
        {
            return None;
        }
        let producer = self.producers.get(&prepared.producer_id)?;
        if producer.owner_session_id != prepared.producer_owner_session_id
            || producer.owner_connection_id != prepared.producer_connection_id
            || producer.stream_type != prepared.producer_stream_type
            || producer.media_kind != prepared.producer_media_kind
            || producer.transport_media_id != Some(target.transport_media_id)
            || producer.routed_producer_id != prepared.producer_routed_id
            || producer.active != prepared.producer_active
        {
            return None;
        }
        Some(PendingConsumerBootstrap {
            sender: prepared.sender.clone(),
            bootstrap: RemoteTrackBootstrap {
                consumer_id: ConsumerRuntimeId::allocate(&mut self.next_consumer_id),
                media_kind: prepared.producer_media_kind,
                producer_id: prepared.producer_id,
                rtp_parameters: prepared.consumer_rtp_parameters.clone(),
                session_id: prepared.producer_owner_session_id.clone(),
                active: prepared.producer_active,
                stream_type: prepared.producer_stream_type,
            },
            producer_owner_session_id: prepared.producer_owner_session_id.clone(),
            producer_connection_id: prepared.producer_connection_id,
            producer_stream_type: prepared.producer_stream_type,
            producer_media_kind: prepared.producer_media_kind,
            producer_routed_id: prepared.producer_routed_id,
            producer_id: prepared.producer_id,
            producer_active: prepared.producer_active,
        })
    }

    pub(in crate::runtime::channel) fn prepare_consumer_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<PreparedConsumerBootstrap> {
        let (sender, client_capabilities) = {
            let session = self.sessions.get(&target.consumer_session_id)?;
            if session.connection_id != target.consumer_connection_id
                || !session.negotiation.can_consume()
            {
                return None;
            }
            (
                session.sender.clone(),
                session.parsed_client_rtp_capabilities.as_ref()?,
            )
        };
        let producer = self.producers.get(&target.producer_id)?;
        if producer.owner_session_id != target.producer_session_id
            || producer.owner_connection_id != target.producer_connection_id
            || producer.stream_type != target.stream_type
            || producer.media_kind != target.media_kind
            || producer.transport_media_id != Some(target.transport_media_id)
        {
            return None;
        }
        let producer_owner_session_id = producer.owner_session_id.clone();
        let producer_stream_type = producer.stream_type;
        let producer_media_kind = producer.media_kind;
        let producer_routed_id = producer.routed_producer_id;
        let producer_consumable_rtp_parameters = producer.consumable_rtp_parameters.clone();
        let producer_active = producer.active;

        if !can_consume(&producer_consumable_rtp_parameters, client_capabilities) {
            return None;
        }
        let negotiated_rtp_parameters = negotiate_consumer_rtp_parameters(
            &producer_consumable_rtp_parameters,
            client_capabilities,
        )
        .ok()?;
        Some(PreparedConsumerBootstrap {
            consumer_rtp_parameters: negotiated_rtp_parameters,
            sender,
            producer_owner_session_id,
            producer_connection_id: producer.owner_connection_id,
            producer_stream_type,
            producer_media_kind,
            producer_routed_id,
            producer_id: target.producer_id,
            producer_active,
        })
    }

    pub(in crate::runtime::channel) fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
    ) -> Option<(OutboundSender, RemoteTrackBootstrap)> {
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.negotiation.can_consume()
        {
            return None;
        }
        let producer = self.producers.get(&pending.producer_id)?;
        if producer.owner_session_id != pending.producer_owner_session_id
            || producer.owner_connection_id != pending.producer_connection_id
            || producer.stream_type != pending.producer_stream_type
            || producer.media_kind != pending.producer_media_kind
            || producer.transport_media_id != Some(target.transport_media_id)
            || producer.routed_producer_id != pending.producer_routed_id
            || producer.active != pending.producer_active
        {
            return None;
        }
        let routed_consumer_id = match self.topology.add_consumer(
            &target.consumer_session_id,
            pending.producer_routed_id,
            pending.producer_media_kind,
            to_router_stream_type(pending.producer_stream_type),
            ConsumerCapability::Compatible,
        ) {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_id = %pending.producer_id,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        self.consumer_index.insert(
            ConsumerKey {
                consumer_session_id: target.consumer_session_id.clone(),
                producer_session_id: pending.producer_owner_session_id.clone(),
                stream_type: pending.producer_stream_type,
            },
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.consumer_connection_id,
                source_connection_id: pending.producer_connection_id,
                source_media: target.transport_media_id,
                consumer_media: consumer_transport_media_id,
            },
        );
        Some((pending.sender, pending.bootstrap))
    }

    pub(in crate::runtime::channel) fn apply_producer_activity(
        &mut self,
        session_id: &SessionId,
        producer_target: &ProducerRouteTarget,
        stream_type: StreamType,
        active: bool,
    ) -> Option<ProducerActivityOutcome> {
        let current_connection_id = self.session_connection_id(session_id);
        let producer = self.producers.get(&producer_target.producer_id)?;
        if producer.owner_connection_id != producer_target.owner_connection_id
            || Some(producer.owner_connection_id) != current_connection_id
            || producer.routed_producer_id != producer_target.routed_producer_id
            || producer.transport_media_id != Some(producer_target.transport_media_id)
        {
            return None;
        }
        let paused = !active;
        if self
            .topology
            .set_producer_paused(producer_target.routed_producer_id, paused)
            .is_err()
        {
            error!(
                ?session_id,
                ?stream_type,
                "failed to set producer pause state in channel router"
            );
            return None;
        }
        {
            let producer = self.producers.get_mut(&producer_target.producer_id)?;
            producer.active = active;
        }
        let snapshot = BTreeMap::from([self.session_info_snapshot(session_id)?]);
        Some(ProducerActivityOutcome {
            transport_media_id: producer_target.transport_media_id,
            active,
            fanout: self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)),
        })
    }

    pub(in crate::runtime::channel) fn download_route_updates(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) -> Vec<ConsumerRouteUpdate> {
        let consumer_connection_id = self.session_connection_id(session_id);
        let mut route_updates = Vec::new();
        for (stream_type, active) in states.iter() {
            let key = ConsumerKey {
                consumer_session_id: session_id.clone(),
                producer_session_id: target_session_id.clone(),
                stream_type,
            };
            let Some(consumer_state) = self.consumer_index.get(&key).copied() else {
                continue;
            };
            if Some(consumer_state.consumer_connection_id) != consumer_connection_id {
                continue;
            }
            route_updates.push(ConsumerRouteUpdate {
                consumer_state,
                stream_type,
                active,
            });
        }
        route_updates
    }

    pub(in crate::runtime::channel) fn commit_download_route_updates(
        &mut self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        committed_updates: impl IntoIterator<Item = ConsumerRouteUpdate>,
    ) -> Vec<ConsumerRouteUpdate> {
        let consumer_connection_id = self.session_connection_id(session_id);
        let mut accepted_updates = Vec::new();
        for route_update in committed_updates {
            let key = ConsumerKey {
                consumer_session_id: session_id.clone(),
                producer_session_id: target_session_id.clone(),
                stream_type: route_update.stream_type,
            };
            let Some(current_consumer_state) = self.consumer_index.get(&key).copied() else {
                continue;
            };
            if current_consumer_state != route_update.consumer_state
                || Some(current_consumer_state.consumer_connection_id) != consumer_connection_id
            {
                continue;
            }
            let paused = !route_update.active;
            if self
                .topology
                .set_consumer_paused(current_consumer_state.routed_consumer_id, paused)
                .is_err()
            {
                error!(
                    ?session_id,
                    ?target_session_id,
                    ?route_update.stream_type,
                    "failed to set consumer pause state in channel router"
                );
                continue;
            }
            accepted_updates.push(route_update);
        }
        accepted_updates
    }
}

impl PublishPrerequisites {
    pub(in crate::runtime::channel) const fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub(in crate::runtime::channel) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }
}

impl PendingConsumerBootstrapTarget {
    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> u64 {
        self.consumer_connection_id
    }

    pub(in crate::runtime::channel) fn consumer_session_id(&self) -> &SessionId {
        &self.consumer_session_id
    }

    pub(in crate::runtime::channel) const fn media_kind(&self) -> RouterMediaKind {
        self.media_kind
    }

    pub(in crate::runtime::channel) const fn producer_connection_id(&self) -> u64 {
        self.producer_connection_id
    }

    pub(in crate::runtime::channel) fn producer_session_id(&self) -> &SessionId {
        &self.producer_session_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }
}

impl PreparedConsumerBootstrap {
    pub(in crate::runtime::channel) fn consumer_rtp_parameters(&self) -> &RouterRtpParameters {
        &self.consumer_rtp_parameters
    }
}

impl ProducerRouteTarget {
    pub(in crate::runtime::channel) const fn owner_connection_id(&self) -> u64 {
        self.owner_connection_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }
}

impl RemoteTrackBootstrap {
    pub(crate) fn consumer_id(&self) -> String {
        self.consumer_id.into_wire_id()
    }

    pub(crate) const fn media_kind(&self) -> RouterMediaKind {
        self.media_kind
    }

    pub(crate) fn producer_id(&self) -> String {
        self.producer_id.into_wire_id()
    }

    pub(crate) fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub(crate) const fn active(&self) -> bool {
        self.active
    }

    pub(crate) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(crate) fn into_channel_event_request(self) -> ChannelEventRequest {
        ChannelEventRequest::BootstrapRemoteTrack(self)
    }
}

impl ConsumerRouteUpdate {
    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> u64 {
        self.consumer_state.consumer_connection_id
    }

    pub(in crate::runtime::channel) const fn consumer_media(&self) -> TransportMediaId {
        self.consumer_state.consumer_media
    }

    pub(in crate::runtime::channel) const fn source_connection_id(&self) -> u64 {
        self.consumer_state.source_connection_id
    }

    pub(in crate::runtime::channel) const fn source_media(&self) -> TransportMediaId {
        self.consumer_state.source_media
    }

    pub(in crate::runtime::channel) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(in crate::runtime::channel) const fn active(&self) -> bool {
        self.active
    }
}

impl UnpublishTrackOutcome {
    pub(in crate::runtime::channel) fn emit(self, session_id: &SessionId, stream_type: StreamType) {
        let track_update = SessionOutbound::TrackBindingUpdate(TrackBindingUpdate {
            session_id: session_id.clone(),
            stream_type,
            active: None,
        });
        for recipient in self.recipients {
            let _ = recipient.send(track_update.clone());
            if let Some(snapshot) = self.session_info_snapshot.as_ref() {
                let _ = recipient.send(SessionOutbound::Message(
                    ChannelEventMessage::SessionInfoChanged(snapshot.clone()),
                ));
            }
        }
    }
}

fn to_router_stream_type(stream_type: StreamType) -> RouterStreamType {
    match stream_type {
        StreamType::Audio => RouterStreamType::Audio,
        StreamType::Camera => RouterStreamType::Camera,
        StreamType::Screen => RouterStreamType::Screen,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use o_sfu_router::{
        ConsumerCapability, MediaKind as RouterMediaKind, ProducerId, RouterId, RtpParameters,
        StreamType as RouterStreamType,
    };
    use tokio::sync::mpsc;

    use super::*;
    use crate::config::MediaCodecFlags;
    use crate::runtime::channel::{
        ChannelAdmissionPolicy, rtp_capabilities::router_rtp_capabilities,
        state::ids::ProducerRuntimeId, topology::RoutedProducerId,
    };
    use crate::runtime::metrics::RuntimeMetrics;
    use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
    use crate::runtime::transport_adapter::TransportMediaId;
    use crate::signaling::shared::SessionPermissions;

    fn test_state() -> ChannelState {
        let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
        ChannelState::new(
            RouterId(1),
            ChannelAdmissionPolicy::new(4),
            router_rtp_capabilities(MediaCodecFlags::default()),
            Arc::new(RecordingService::new(
                0,
                media_source,
                Arc::new(RuntimeMetrics::default()),
            )),
        )
    }

    #[test]
    fn producer_activity_does_not_flip_channel_state_when_router_update_fails() {
        let mut state = test_state();
        let session_id = SessionId::Integer(1);
        let (sender, _rx) = mpsc::unbounded_channel();

        let join = state.apply_join(
            &session_id,
            None,
            SessionPermissions::default(),
            sender,
            false,
        );
        assert!(join.is_ok());
        let connection_id = state.session_connection_id(&session_id).unwrap_or(u64::MAX);

        let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
        let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(777));
        let transport_media_id = TransportMediaId::default();
        state.producer_ids_by_owner_stream.insert(
            ProducerKey::new(&session_id, StreamType::Camera),
            producer_id,
        );
        state.producers.insert(
            producer_id,
            PublishedProducer {
                owner_session_id: session_id.clone(),
                owner_connection_id: connection_id,
                stream_type: StreamType::Camera,
                media_kind: RouterMediaKind::Video,
                consumable_rtp_parameters: RtpParameters::new(vec![], vec![], vec![]),
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                source_packet_selection: None,
                active: true,
            },
        );

        let outcome = state.apply_producer_activity(
            &session_id,
            &ProducerRouteTarget {
                producer_id,
                owner_connection_id: connection_id,
                routed_producer_id,
                transport_media_id,
            },
            StreamType::Camera,
            false,
        );
        assert!(outcome.is_none());
        assert!(
            state
                .producers
                .get(&producer_id)
                .is_some_and(|producer| producer.active),
            "channel state must keep the previous activity flag when router pause propagation fails"
        );
    }

    #[test]
    #[allow(
        clippy::panic,
        reason = "test fixture wiring intentionally aborts on impossible setup failures"
    )]
    fn stale_download_route_updates_are_ignored_before_transport_commit() {
        let mut state = test_state();
        let producer_session_id = SessionId::Integer(1);
        let consumer_session_id = SessionId::Integer(2);
        let (producer_sender, _producer_rx) = mpsc::unbounded_channel();
        let (consumer_sender, _consumer_rx) = mpsc::unbounded_channel();

        assert!(
            state
                .apply_join(
                    &producer_session_id,
                    None,
                    SessionPermissions::default(),
                    producer_sender,
                    false,
                )
                .is_ok()
        );
        assert!(
            state
                .apply_join(
                    &consumer_session_id,
                    None,
                    SessionPermissions::default(),
                    consumer_sender,
                    false,
                )
                .is_ok()
        );

        let Some(producer_connection_id) = state.session_connection_id(&producer_session_id) else {
            panic!("producer session should have a connection id");
        };
        let Some(consumer_connection_id) = state.session_connection_id(&consumer_session_id) else {
            panic!("consumer session should have a connection id");
        };
        let routed_producer_id = match state.topology.add_producer(
            &producer_session_id,
            RouterMediaKind::Video,
            RouterStreamType::Camera,
        ) {
            Ok(routed_producer_id) => routed_producer_id,
            Err(error) => panic!("failed to create test producer route: {error:?}"),
        };
        let routed_consumer_id = match state.topology.add_consumer(
            &consumer_session_id,
            routed_producer_id,
            RouterMediaKind::Video,
            RouterStreamType::Camera,
            ConsumerCapability::Compatible,
        ) {
            Ok(routed_consumer_id) => routed_consumer_id,
            Err(error) => panic!("failed to create test consumer route: {error:?}"),
        };

        let consumer_media = TransportMediaId::new(2);
        let route_key = ConsumerKey {
            consumer_session_id: consumer_session_id.clone(),
            producer_session_id: producer_session_id.clone(),
            stream_type: StreamType::Camera,
        };
        let consumer_state = ConsumerState {
            routed_consumer_id,
            consumer_connection_id,
            source_connection_id: producer_connection_id,
            source_media: TransportMediaId::new(1),
            consumer_media,
        };
        state
            .consumer_index
            .insert(route_key.clone(), consumer_state);

        let route_updates = state.download_route_updates(
            &consumer_session_id,
            &producer_session_id,
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
        );
        assert_eq!(route_updates.len(), 1);

        state.consumer_index.insert(
            route_key,
            ConsumerState {
                consumer_connection_id: consumer_connection_id.saturating_add(1),
                ..consumer_state
            },
        );

        let committed_updates = state.commit_download_route_updates(
            &consumer_session_id,
            &producer_session_id,
            route_updates,
        );

        assert!(committed_updates.is_empty());
    }
}
