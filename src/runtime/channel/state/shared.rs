use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use o_sfu_router::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities, RouterId};

use crate::runtime::recording::RecordingService;
use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::{
    current_protocol::CurrentServerMessage,
    protocol::PeerSnapshot,
    shared::{RecordingState, SessionId, SessionInfo, SessionPermissions, StreamType},
    webrtc::MediaKind as SignalingMediaKind,
};

use super::super::{
    ChannelAdmissionPolicy,
    outbound::{MessageFanout, OutboundSender, fanout_all, fanout_all_except},
    session_negotiation::SessionNegotiation,
    topology::{ChannelTopology, RoutedConsumerId, RoutedProducerId},
};
use super::ids::ProducerRuntimeId;

/// Core mutable state for a single SFU channel (room).
///
/// Owns all session, producer, and consumer bookkeeping. Every mutation returns
/// an `*Outcome` value that carries deferred side-effects (fan-out messages,
/// kicked senders). The caller is responsible for calling `.emit()` on outcomes
/// after releasing any lock on this state so the critical section stays pure
/// and non-blocking.
///
/// The two-phase patterns (`prepare_*` / `commit_*`) allow async transport work
/// to happen between phases without holding the state lock.
#[derive(Debug)]
pub(in crate::runtime::channel) struct ChannelState {
    pub(super) admission_policy: ChannelAdmissionPolicy,
    pub(super) sessions: BTreeMap<SessionId, ActiveSession>,
    /// Monotonically increasing: each join, including re-joins, gets a fresh id
    /// so stale async callbacks from a previous connection are rejected.
    pub(super) next_connection_id: u64,
    pub(super) next_producer_id: u64,
    pub(super) next_consumer_id: u64,
    pub(super) recording_state: RecordingState,
    /// Keyed by typed runtime producer id. Compatibility wire ids are rendered at the edge.
    pub(super) producers: BTreeMap<ProducerRuntimeId, PublishedProducer>,
    /// Producer lookup keyed by the publisher session and stream type.
    pub(super) producer_ids_by_owner_stream: BTreeMap<ProducerKey, ProducerRuntimeId>,
    /// Control-plane lookup for bitrate snapshots keyed by transport-owned media ids.
    pub(super) producer_stream_types_by_transport_media_id: BTreeMap<TransportMediaId, StreamType>,
    pub(super) consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    /// Shadow of session/producer/consumer state inside the pure router core.
    pub(super) topology: ChannelTopology,
}

/// Uniquely identifies a consumer subscription: which session consumes which
/// other session's stream for a given stream type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime::channel) struct ConsumerKey {
    pub(super) consumer_session_id: SessionId,
    pub(super) producer_session_id: SessionId,
    pub(super) stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ProducerKey {
    owner_session_id: SessionId,
    stream_type: StreamType,
}

#[derive(Debug)]
pub(in crate::runtime::channel) struct ActiveSession {
    #[allow(
        dead_code,
        reason = "stored for future session display and recording metadata"
    )]
    pub(super) label: Option<String>,
    #[allow(dead_code, reason = "stored for future permission-gated actions")]
    pub(super) permissions: SessionPermissions,
    pub(super) info: SessionInfo,
    pub(super) negotiation: SessionNegotiation,
    pub(super) parsed_client_rtp_capabilities: Option<RouterRtpCapabilities>,
    pub(super) connection_id: u64,
    pub(super) sender: OutboundSender,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PublishedProducer {
    pub(super) owner_session_id: SessionId,
    pub(super) owner_connection_id: u64,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) consumable_rtp_parameters: o_sfu_router::RtpParameters,
    pub(super) routed_producer_id: RoutedProducerId,
    pub(super) transport_media_id: Option<TransportMediaId>,
    pub(super) active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerState {
    pub(super) routed_consumer_id: RoutedConsumerId,
    pub(super) consumer_connection_id: u64,
    pub(super) source_connection_id: u64,
    pub(super) source_media: TransportMediaId,
    pub(super) consumer_media: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct TransportMediaRemoval {
    session: SessionId,
    connection: u64,
    transport_media: TransportMediaId,
}

impl ChannelState {
    pub(in crate::runtime::channel) fn new(
        router_id: RouterId,
        admission_policy: ChannelAdmissionPolicy,
        recording_service: Arc<RecordingService>,
    ) -> Self {
        Self {
            admission_policy,
            sessions: BTreeMap::new(),
            next_connection_id: 0,
            next_producer_id: 1,
            next_consumer_id: 1,
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            producers: BTreeMap::new(),
            producer_ids_by_owner_stream: BTreeMap::new(),
            producer_stream_types_by_transport_media_id: BTreeMap::new(),
            consumer_index: BTreeMap::new(),
            topology: ChannelTopology::new_with_recording_service(router_id, recording_service),
        }
    }

    pub(in crate::runtime::channel) fn collect_consumer_transport_removals(
        &self,
        departing_session_ids: &BTreeSet<SessionId>,
    ) -> Vec<TransportMediaRemoval> {
        self.consumer_index
            .iter()
            .filter_map(|(key, consumer_state)| {
                if !departing_session_ids.contains(&key.producer_session_id)
                    || departing_session_ids.contains(&key.consumer_session_id)
                {
                    return None;
                }
                Some(TransportMediaRemoval {
                    session: key.consumer_session_id.clone(),
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            })
            .collect()
    }

    pub(in crate::runtime::channel) fn purge_session_media_state(
        &mut self,
        session_id: &SessionId,
    ) {
        let removed_producers = self
            .producers
            .values()
            .filter(|producer| producer.owner_session_id == *session_id)
            .map(|producer| {
                (
                    ProducerKey::new(&producer.owner_session_id, producer.stream_type),
                    producer.transport_media_id,
                )
            })
            .collect::<Vec<_>>();
        self.producers
            .retain(|_producer_id, producer| producer.owner_session_id != *session_id);
        for (producer_key, transport_media_id) in removed_producers {
            self.producer_ids_by_owner_stream.remove(&producer_key);
            if let Some(transport_media_id) = transport_media_id {
                self.producer_stream_types_by_transport_media_id
                    .remove(&transport_media_id);
            }
        }
        self.consumer_index.retain(|key, _consumer_state| {
            key.consumer_session_id != *session_id && key.producer_session_id != *session_id
        });
    }

    pub(in crate::runtime::channel) fn session_mut_for_connection(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> Option<&mut ActiveSession> {
        let session = self.sessions.get_mut(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        Some(session)
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.topology.session_permissions(session_id)
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn session_has_parsed_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> bool {
        self.sessions
            .get(session_id)
            .and_then(|session| session.parsed_client_rtp_capabilities.as_ref())
            .is_some()
    }

    pub(in crate::runtime::channel) fn recording_state(&self) -> RecordingState {
        self.recording_state.clone()
    }

    pub(in crate::runtime::channel) fn peer_snapshots_except(
        &self,
        excluded_session_id: &SessionId,
    ) -> Vec<PeerSnapshot> {
        self.sessions
            .iter()
            .filter(|(session_id, _session)| *session_id != excluded_session_id)
            .map(|(session_id, session)| PeerSnapshot {
                session_id: session_id.clone(),
                info: session.info.clone(),
            })
            .collect()
    }

    pub(in crate::runtime::channel) fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.topology.rtp_capabilities().clone()
    }

    pub(in crate::runtime::channel) fn transport_session_entries(&self) -> Vec<(SessionId, u64)> {
        self.sessions
            .iter()
            .map(|(session_id, session)| (session_id.clone(), session.connection_id))
            .collect()
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn producer_count(&self) -> usize {
        self.producers.len()
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn consumer_count(&self) -> usize {
        self.consumer_index.len()
    }

    pub(in crate::runtime::channel) fn topology_counts(&self) -> (u64, u64, u64) {
        (
            self.topology.session_count(),
            self.topology.camera_count(),
            self.topology.screen_count(),
        )
    }

    pub(in crate::runtime::channel) fn session_connection_id(
        &self,
        session_id: &SessionId,
    ) -> Option<u64> {
        self.sessions
            .get(session_id)
            .map(|session| session.connection_id)
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn session_count(&self) -> usize {
        self.sessions.len()
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn has_session(&self, session_id: &SessionId) -> bool {
        self.sessions.contains_key(session_id)
    }

    pub(in crate::runtime::channel) fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    pub(in crate::runtime::channel) fn fanout_all(
        &self,
        message: &CurrentServerMessage,
    ) -> MessageFanout {
        fanout_all(
            self.sessions.values().map(|session| session.sender.clone()),
            message,
        )
    }

    pub(in crate::runtime::channel) fn fanout_all_except(
        &self,
        message: &CurrentServerMessage,
        excluded_session_id: Option<&SessionId>,
    ) -> MessageFanout {
        fanout_all_except(
            self.sessions
                .iter()
                .filter(|(session_id, _session)| {
                    excluded_session_id.is_none_or(|excluded| excluded != *session_id)
                })
                .map(|(_session_id, session)| session.sender.clone()),
            message,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime::channel) fn first_published_transport_media_id(
        &self,
    ) -> Option<TransportMediaId> {
        self.producers
            .values()
            .find_map(|producer| producer.transport_media_id)
    }
}

impl TransportMediaRemoval {
    pub(in crate::runtime::channel) fn session(&self) -> &SessionId {
        &self.session
    }

    pub(in crate::runtime::channel) const fn connection(&self) -> u64 {
        self.connection
    }

    pub(in crate::runtime::channel) const fn transport_media(&self) -> TransportMediaId {
        self.transport_media
    }
}

impl ProducerKey {
    pub(super) fn new(owner_session_id: &SessionId, stream_type: StreamType) -> Self {
        Self {
            owner_session_id: owner_session_id.clone(),
            stream_type,
        }
    }
}
