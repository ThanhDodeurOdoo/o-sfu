use std::collections::BTreeMap;

use o_sfu_router::{
    ConsumerCapability, MediaCapabilities, MediaKind as RouterMediaKind,
    RtpParameters as RouterRtpParameters, StreamType as RouterStreamType, can_consume,
    negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::{
    current_protocol::CurrentServerMessage,
    current_protocol::{CurrentRemoteTrackBootstrapPayload, CurrentServerRequest},
    ortc_mapper,
    shared::{DownloadStates, SessionId, StreamType},
    webrtc::{MediaKind as SignalingMediaKind, RtpParameters},
};

use super::super::{
    outbound::{MessageFanout, OutboundSender},
    topology::RoutedProducerId,
};
use super::{
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    shared::{ChannelState, ConsumerKey, ConsumerState, ProducerKey, PublishedProducer},
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
    media_kind: SignalingMediaKind,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PreparedConsumerBootstrap {
    consumer_rtp_parameters: RouterRtpParameters,
    consumer_wire_rtp_parameters: RtpParameters,
    sender: OutboundSender,
    producer_owner_session_id: SessionId,
    producer_connection_id: u64,
    producer_stream_type: StreamType,
    producer_media_kind: SignalingMediaKind,
    producer_routed_id: RoutedProducerId,
    producer_id: ProducerRuntimeId,
    producer_active: bool,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingPublishedTrack {
    producer_id: ProducerRuntimeId,
    owner_session_id: SessionId,
    owner_connection_id: u64,
    stream_type: StreamType,
    media_kind: SignalingMediaKind,
    consumable_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrap {
    sender: OutboundSender,
    bootstrap: RemoteTrackBootstrap,
    producer_owner_session_id: SessionId,
    producer_connection_id: u64,
    producer_stream_type: StreamType,
    producer_media_kind: SignalingMediaKind,
    producer_routed_id: RoutedProducerId,
    producer_id: ProducerRuntimeId,
    producer_active: bool,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct RemoteTrackBootstrap {
    consumer_id: ConsumerRuntimeId,
    media_kind: SignalingMediaKind,
    producer_id: ProducerRuntimeId,
    rtp_parameters: RtpParameters,
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

    pub(in crate::runtime::channel) fn late_join_consumer_targets(
        &self,
        session_id: &SessionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        let Some(session) = self.sessions.get(session_id) else {
            return Vec::new();
        };
        if !session.negotiation.can_consume() {
            return Vec::new();
        }

        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                if producer.owner_session_id == *session_id {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: session_id.clone(),
                    consumer_connection_id: session.connection_id,
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
        media_kind: SignalingMediaKind,
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
        &mut self,
        session_id: &SessionId,
        publisher_connection_id: u64,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<PendingPublishedTrack> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != publisher_connection_id || !session.negotiation.can_publish() {
            return None;
        }
        Some(PendingPublishedTrack {
            producer_id: ProducerRuntimeId::allocate(&mut self.next_producer_id),
            owner_session_id: session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type,
            media_kind,
            consumable_rtp_parameters,
        })
    }

    pub(in crate::runtime::channel) fn commit_published_track(
        &mut self,
        pending: PendingPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(ProducerRuntimeId, Vec<PendingConsumerBootstrapTarget>)> {
        let session = self.sessions.get(&pending.owner_session_id)?;
        if session.connection_id != pending.owner_connection_id
            || !session.negotiation.can_publish()
        {
            return None;
        }
        let routed_producer_id = match self.topology.add_producer(
            &pending.owner_session_id,
            to_router_media_kind(pending.media_kind),
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
            pending.producer_id,
            PublishedProducer {
                owner_session_id: pending.owner_session_id.clone(),
                owner_connection_id: pending.owner_connection_id,
                stream_type: pending.stream_type,
                media_kind: pending.media_kind,
                consumable_rtp_parameters: pending.consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                active: true,
            },
        );
        self.producer_ids_by_owner_stream.insert(
            ProducerKey::new(&pending.owner_session_id, pending.stream_type),
            pending.producer_id,
        );
        self.producer_stream_types_by_transport_media_id
            .insert(transport_media_id, pending.stream_type);
        let consumer_targets = self.publish_consumer_targets(
            &pending.owner_session_id,
            pending.owner_connection_id,
            pending.producer_id,
            pending.stream_type,
            pending.media_kind,
            transport_media_id,
        );
        Some((pending.producer_id, consumer_targets))
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
                rtp_parameters: prepared.consumer_wire_rtp_parameters.clone(),
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
        let consumer_wire_rtp_parameters = RtpParameters(ortc_mapper::serialize_rtp_parameters(
            &negotiated_rtp_parameters,
        ));
        Some(PreparedConsumerBootstrap {
            consumer_rtp_parameters: negotiated_rtp_parameters,
            consumer_wire_rtp_parameters,
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
            to_router_media_kind(pending.producer_media_kind),
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
    ) -> Option<MessageFanout> {
        let current_connection_id = self.session_connection_id(session_id);
        let producer = self.producers.get_mut(&producer_target.producer_id)?;
        if producer.owner_connection_id != producer_target.owner_connection_id
            || Some(producer.owner_connection_id) != current_connection_id
            || producer.routed_producer_id != producer_target.routed_producer_id
            || producer.transport_media_id != Some(producer_target.transport_media_id)
        {
            return None;
        }
        producer.active = active;
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
        let snapshot = BTreeMap::from([self.session_info_snapshot(session_id)?]);
        Some(self.fanout_all(&CurrentServerMessage::SessionInfoChanged(snapshot)))
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
    ) {
        let consumer_connection_id = self.session_connection_id(session_id);
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
            }
        }
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

    pub(in crate::runtime::channel) const fn media_kind(&self) -> SignalingMediaKind {
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
    pub(in crate::runtime::channel) fn into_current_server_request(self) -> CurrentServerRequest {
        CurrentServerRequest::BootstrapRemoteTrack(CurrentRemoteTrackBootstrapPayload {
            id: self.consumer_id.into_wire_id(),
            media_kind: self.media_kind,
            source_id: self.producer_id.into_wire_id(),
            rtp_parameters: self.rtp_parameters,
            session_id: self.session_id,
            active: self.active,
            stream_type: self.stream_type,
        })
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

fn to_router_media_kind(media_kind: SignalingMediaKind) -> RouterMediaKind {
    match media_kind {
        SignalingMediaKind::Audio => RouterMediaKind::Audio,
        SignalingMediaKind::Video => RouterMediaKind::Video,
    }
}

fn to_router_stream_type(stream_type: StreamType) -> RouterStreamType {
    match stream_type {
        StreamType::Audio => RouterStreamType::Audio,
        StreamType::Camera => RouterStreamType::Camera,
        StreamType::Screen => RouterStreamType::Screen,
    }
}
