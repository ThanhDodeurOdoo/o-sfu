use std::collections::BTreeMap;

use o_sfu_router::{
    MediaCapabilities, MediaKind as RouterMediaKind, RtpParameters as RouterRtpParameters,
};
use tracing::{error, warn};

use crate::runtime::transport_adapter::TransportMediaId;
use o_sfu_protocol::shared::{SessionId, SessionInfo, StreamType};

use super::super::{
    super::{
        ChannelEventMessage, SessionOutbound, TrackBindingUpdate,
        outbound::{MessageFanout, OutboundSender},
        topology::RoutedProducerId,
    },
    ids::ProducerRuntimeId,
    shared::{ChannelState, ConsumerKey, ProducerKey, PublishedProducer, TransportMediaRemoval},
};
use super::{bootstrap::PendingConsumerBootstrapTarget, router_stream_type::to_router_stream_type};

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
pub(in crate::runtime::channel) struct PublishPrerequisites {
    connection_id: u64,
    router_capabilities: MediaCapabilities,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PreparedPublishedTrack {
    owner_session_id: SessionId,
    owner_connection_id: u64,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
    consumable_rtp_parameters: RouterRtpParameters,
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

    pub(in crate::runtime::channel) fn prepare_published_track(
        &self,
        session_id: &SessionId,
        publisher_connection_id: u64,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<PreparedPublishedTrack> {
        let Some(session) = self.sessions.get(session_id) else {
            warn!(
                ?session_id,
                publisher_connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the session is missing from channel state"
            );
            return None;
        };
        if session.connection_id != publisher_connection_id {
            warn!(
                ?session_id,
                publisher_connection_id,
                current_connection_id = session.connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the connection is stale"
            );
            return None;
        }
        if !session.negotiation.can_publish() {
            warn!(
                ?session_id,
                publisher_connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the session is not publish-ready"
            );
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
        let Some(session) = self.sessions.get(&pending.owner_session_id) else {
            warn!(
                session_id = ?pending.owner_session_id,
                owner_connection_id = pending.owner_connection_id,
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because the session is missing from channel state"
            );
            return None;
        };
        if session.connection_id != pending.owner_connection_id
            || !session.negotiation.can_publish()
        {
            warn!(
                session_id = ?pending.owner_session_id,
                owner_connection_id = pending.owner_connection_id,
                current_connection_id = session.connection_id,
                publish_ready = session.negotiation.can_publish(),
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because the session state changed before commit"
            );
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
                if self.consumer_bootstrap_exists(&ConsumerKey::new(
                    peer_session_id,
                    producer_session_id,
                    stream_type,
                )) {
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
        self.pending_consumer_bootstraps
            .retain(|key| key.producer_session_id != *session_id || key.stream_type != stream_type);
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
}

impl PublishPrerequisites {
    pub(in crate::runtime::channel) const fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub(in crate::runtime::channel) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
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
