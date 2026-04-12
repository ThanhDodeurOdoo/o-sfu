use std::collections::BTreeMap;
use std::sync::Arc;

use o_sfu_router::{
    MediaCapabilities as RouterRtpCapabilities, MediaKind as RouterMediaKind, RouterId,
    RtpParameters as RouterRtpParameters, StreamType as RouterStreamType, can_consume,
    negotiate_consumer_rtp_parameters,
};
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::runtime::recording::RecordingService;
use crate::runtime::transport_adapter::TransportConnectDirection;
use crate::signaling::{
    current_protocol::{
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentServerRequest,
        CurrentSessionDeparturePayload, CurrentSessionInfoSnapshotById,
    },
    ortc_mapper,
    protocol::WebSocketCloseCode,
    shared::{RecordingState, SessionId, SessionInfo, SessionPermissions, StreamType},
    webrtc::{
        MediaKind as SignalingMediaKind, RtpCapabilities as SignalingRtpCapabilities, RtpParameters,
    },
};

use super::{
    SessionOutbound,
    outbound::{MessageFanout, OutboundSender, fanout_all, fanout_all_except},
    session_negotiation::{SessionNegotiation, SessionNegotiationUpdate},
    topology::{ChannelTopology, RoutedConsumerId, RoutedProducerId},
};
use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::bundle_api::bundle_session_info_key;

/// Core mutable state for a single SFU channel (room).
///
/// Owns all session, producer, and consumer bookkeeping. Every mutation returns
/// an `*Outcome` value that carries deferred side-effects (fan-out messages,
/// kicked senders). The caller is responsible for calling `.emit()` on outcomes
/// **after** releasing any lock on this state, this keep the critical section
/// pure and non-blocking.
///
/// The two-phase patterns (`prepare_*` / `commit_*`) allow async transport work
/// (e.g. DTLS, SRTP setup) to happen between the phases without holding the
/// state lock.
///
/// ```text
///   ┌───────────────┐
///   │ ChannelState  │
///   ├───────────────┤
///   │ sessions      │──> per-session negotiation + outbound sender
///   │ producers     │──> published media tracks (keyed by wire id)
///   │ consumer_index│──> (consumer, producer, stream) → routed consumer
///   │ topology      │──> mirrors state itno the pure o-sfu-router core
///   └───────────────┘
/// ```
#[derive(Debug)]
pub(super) struct ChannelState {
    pub(super) sessions: BTreeMap<SessionId, ActiveSession>,
    /// Monotonically increasing, each join (including re-joins) gets a fresh id
    /// so stale async callbacks from a previous connection are rejected.
    pub(super) next_connection_id: u64,
    next_producer_id: u64,
    next_consumer_id: u64,
    pub(super) recording_state: RecordingState,
    /// Keyed by wire-format producer id (e.g. `"producer-3"`).
    pub(super) producers: BTreeMap<String, PublishedProducer>,
    /// Producer lookup keyed by the publisher session and stream type.
    producer_wire_ids_by_owner_stream: BTreeMap<ProducerKey, String>,
    /// Control-plane lookup for bitrate snapshots keyed by transport-owned media ids.
    /// This keeps stats and other bookkeeping out of linear scans over `producers`.
    producer_stream_types_by_transport_media_id: BTreeMap<TransportMediaId, StreamType>,
    pub(super) consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    /// Shadow of session/producer/consumer state inside the pure router core.
    /// Must be kept in sync with the maps above -- every mutating method updates
    /// both or rolls back.
    pub(super) topology: ChannelTopology,
}

/// Uniquely identifies a consumer subscription: which session is consuming
/// which other session's stream of a given type. Used as the key into
/// `ChannelState::consumer_index`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ConsumerKey {
    pub(super) consumer_session_id: SessionId,
    pub(super) producer_session_id: SessionId,
    pub(super) stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProducerKey {
    owner_session_id: SessionId,
    stream_type: StreamType,
}

/// A connected participant in the channel. Tracks negotiation progress
/// (RTP capabilities, transport readiness), permissions, and the outbound
/// message sender for pushing server events to this session's WebSocket.
#[derive(Debug)]
pub(super) struct ActiveSession {
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
    pub(super) sender: mpsc::UnboundedSender<SessionOutbound>,
}

#[derive(Debug, Clone)]
pub(super) struct PublishedProducer {
    pub(super) owner_session_id: SessionId,
    pub(super) owner_connection_id: u64,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) consumable_rtp_parameters: RouterRtpParameters,
    pub(super) routed_producer_id: RoutedProducerId,
    pub(super) transport_media_id: Option<TransportMediaId>,
    pub(super) active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConsumerState {
    pub(super) routed_consumer_id: RoutedConsumerId,
    pub(super) consumer_connection_id: u64,
    pub(super) source_connection_id: u64,
    pub(super) source_media: TransportMediaId,
    pub(super) consumer_media: TransportMediaId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProducerRouteTarget {
    pub(super) producer_wire_id: String,
    pub(super) owner_connection_id: u64,
    pub(super) routed_producer_id: RoutedProducerId,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(super) struct PendingConsumerBootstrapTarget {
    pub(super) consumer_session_id: SessionId,
    pub(super) consumer_connection_id: u64,
    pub(super) producer_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_wire_id: String,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedConsumerBootstrap {
    pub(super) consumer_rtp_parameters: RouterRtpParameters,
    pub(super) consumer_wire_rtp_parameters: RtpParameters,
    pub(super) sender: mpsc::UnboundedSender<SessionOutbound>,
    pub(super) producer_owner_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_stream_type: StreamType,
    pub(super) producer_media_kind: SignalingMediaKind,
    pub(super) producer_routed_id: RoutedProducerId,
    pub(super) producer_wire_id: String,
    pub(super) producer_active: bool,
}

#[derive(Debug, Clone)]
pub(super) struct PendingPublishedTrack {
    pub(super) producer_wire_id: String,
    pub(super) owner_session_id: SessionId,
    pub(super) owner_connection_id: u64,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) consumable_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(super) struct PendingConsumerBootstrap {
    pub(super) sender: mpsc::UnboundedSender<SessionOutbound>,
    pub(super) request: CurrentServerRequest,
    pub(super) producer_owner_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_stream_type: StreamType,
    pub(super) producer_media_kind: SignalingMediaKind,
    pub(super) producer_routed_id: RoutedProducerId,
    pub(super) producer_wire_id: String,
    pub(super) producer_active: bool,
}

#[derive(Debug)]
pub(super) struct JoinSessionOutcome {
    pub(super) connection_id: u64,
    replaced_sender: Option<OutboundSender>,
    departure_fanout: Option<MessageFanout>,
}

impl JoinSessionOutcome {
    pub(super) fn emit(self) {
        if let Some(sender) = self.replaced_sender {
            let _ = sender.send(SessionOutbound::Close(WebSocketCloseCode::Kicked));
        }
        if let Some(fanout) = self.departure_fanout {
            fanout.emit();
        }
    }
}

#[derive(Debug)]
pub(super) struct LeaveSessionOutcome {
    departure_fanout: MessageFanout,
}

impl LeaveSessionOutcome {
    pub(super) fn emit(self) {
        self.departure_fanout.emit();
    }
}

#[derive(Debug)]
pub(super) struct SessionInfoUpdateOutcome {
    fanout: MessageFanout,
}

impl SessionInfoUpdateOutcome {
    pub(super) fn emit(self) {
        self.fanout.emit();
    }
}

#[derive(Debug)]
pub(super) struct DisconnectSessionsOutcome {
    kicked_senders: Vec<OutboundSender>,
    departure_fanouts: Vec<MessageFanout>,
}

impl DisconnectSessionsOutcome {
    pub(super) fn emit(self) {
        for sender in self.kicked_senders {
            let _ = sender.send(SessionOutbound::Close(WebSocketCloseCode::Kicked));
        }
        for fanout in self.departure_fanouts {
            fanout.emit();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
}

impl ChannelState {
    pub(super) fn new(router_id: RouterId, recording_service: Arc<RecordingService>) -> Self {
        Self {
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
            producer_wire_ids_by_owner_stream: BTreeMap::new(),
            producer_stream_types_by_transport_media_id: BTreeMap::new(),
            consumer_index: BTreeMap::new(),
            topology: ChannelTopology::new_with_recording_service(router_id, recording_service),
        }
    }

    pub(super) fn purge_session_media_state(&mut self, session_id: &SessionId) {
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
            .retain(|_wire_id, producer| producer.owner_session_id != *session_id);
        for (producer_key, transport_media_id) in removed_producers {
            self.producer_wire_ids_by_owner_stream.remove(&producer_key);
            if let Some(transport_media_id) = transport_media_id {
                self.producer_stream_types_by_transport_media_id
                    .remove(&transport_media_id);
            }
        }
        self.consumer_index.retain(|key, _consumer_id| {
            key.consumer_session_id != *session_id && key.producer_session_id != *session_id
        });
    }

    pub(super) fn late_join_consumer_targets(
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
            .filter_map(|(producer_wire_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                if producer.owner_session_id == *session_id {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: session_id.clone(),
                    consumer_connection_id: session.connection_id,
                    producer_session_id: producer.owner_session_id.clone(),
                    producer_connection_id: producer.owner_connection_id,
                    producer_wire_id: producer_wire_id.clone(),
                    stream_type: producer.stream_type,
                    media_kind: producer.media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(super) fn apply_join(
        &mut self,
        session_id: &SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: OutboundSender,
        max_sessions: usize,
    ) -> Result<JoinSessionOutcome, super::ChannelJoinError> {
        let is_new = !self.sessions.contains_key(session_id);
        if is_new && self.sessions.len() >= max_sessions {
            return Err(super::ChannelJoinError::ChannelFull);
        }
        let connection_id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        if !is_new {
            if self.topology.apply_client_leave(session_id).is_err() {
                error!(
                    ?session_id,
                    "failed to reset replaced session in channel router"
                );
                return Err(super::ChannelJoinError::RouterState);
            }
            self.purge_session_media_state(session_id);
        }
        if self
            .topology
            .apply_client_join(session_id, connection_id, &permissions)
            .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session join into channel router"
            );
            return Err(super::ChannelJoinError::RouterState);
        }

        let previous_sender = if let Some(session) = self.sessions.get_mut(session_id) {
            let old_sender = session.sender.clone();
            session.label.clone_from(&label);
            session.permissions.clone_from(&permissions);
            session.info = SessionInfo::default();
            session.negotiation = SessionNegotiation::default();
            session.parsed_client_rtp_capabilities = None;
            session.connection_id = connection_id;
            session.sender = sender;
            Some(old_sender)
        } else {
            self.sessions.insert(
                session_id.clone(),
                ActiveSession {
                    label,
                    permissions,
                    info: SessionInfo::default(),
                    negotiation: SessionNegotiation::default(),
                    parsed_client_rtp_capabilities: None,
                    connection_id,
                    sender,
                },
            );
            None
        };

        let departure_fanout = previous_sender.as_ref().map(|_| {
            fanout_all_except(
                &self.sessions,
                &CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                    session_id: session_id.clone(),
                }),
                Some(session_id),
            )
        });
        Ok(JoinSessionOutcome {
            connection_id,
            replaced_sender: previous_sender,
            departure_fanout,
        })
    }

    pub(super) fn apply_leave(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> Option<LeaveSessionOutcome> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        if self.topology.apply_client_leave(session_id).is_err() {
            error!(
                ?session_id,
                "failed to mirror session leave into channel router"
            );
            return None;
        }
        self.sessions.remove(session_id);
        self.purge_session_media_state(session_id);
        Some(LeaveSessionOutcome {
            departure_fanout: fanout_all(
                &self.sessions,
                &CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                    session_id: session_id.clone(),
                }),
            ),
        })
    }

    pub(super) fn apply_update_session_info(
        &mut self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
    ) -> Option<SessionInfoUpdateOutcome> {
        let updated_info = {
            let session = self.sessions.get_mut(session_id)?;
            session.info = info;
            session.info.clone()
        };
        if self
            .topology
            .update_session_info(session_id, &updated_info)
            .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session info update into channel router"
            );
            return None;
        }
        let snapshot: CurrentSessionInfoSnapshotById = if need_refresh {
            self.sessions
                .iter()
                .map(|(id, session)| (bundle_session_info_key(id), session.info.clone()))
                .collect()
        } else {
            BTreeMap::from([(
                bundle_session_info_key(session_id),
                self.sessions
                    .get(session_id)
                    .map_or_else(SessionInfo::default, |session| session.info.clone()),
            )])
        };
        Some(SessionInfoUpdateOutcome {
            fanout: fanout_all(
                &self.sessions,
                &CurrentServerMessage::SessionInfoChanged(snapshot),
            ),
        })
    }

    pub(super) fn apply_disconnect_sessions(
        &mut self,
        session_ids: &[SessionId],
    ) -> DisconnectSessionsOutcome {
        let mut kicked_senders = Vec::new();
        let mut departed = Vec::new();
        for session_id in session_ids {
            if !self.sessions.contains_key(session_id) {
                continue;
            }
            if self.topology.apply_client_leave(session_id).is_err() {
                error!(
                    ?session_id,
                    "failed to mirror bulk disconnect into channel router"
                );
                continue;
            }
            if let Some(session) = self.sessions.remove(session_id) {
                self.purge_session_media_state(session_id);
                kicked_senders.push(session.sender);
                departed.push(session_id.clone());
            }
        }
        let departure_fanouts = departed
            .into_iter()
            .map(|departed_id| {
                fanout_all(
                    &self.sessions,
                    &CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                        session_id: departed_id,
                    }),
                )
            })
            .collect();
        DisconnectSessionsOutcome {
            kicked_senders,
            departure_fanouts,
        }
    }

    pub(super) fn set_client_rtp_capabilities(
        &mut self,
        session_id: &SessionId,
        capabilities: SignalingRtpCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities =
            ortc_mapper::parse_rtp_capabilities(&capabilities.0);
        session
            .negotiation
            .set_client_rtp_capabilities(capabilities)
    }

    pub(super) fn set_transport_connected(
        &mut self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.negotiation.set_transport_connected(direction)
    }

    pub(super) fn publish_consumer_targets(
        &self,
        producer_session_id: &SessionId,
        producer_connection_id: u64,
        producer_wire_id: &str,
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
                    producer_wire_id: producer_wire_id.to_owned(),
                    stream_type,
                    media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(super) fn prepare_published_track(
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
            producer_wire_id: allocate_wire_producer_id(&mut self.next_producer_id),
            owner_session_id: session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type,
            media_kind,
            consumable_rtp_parameters,
        })
    }

    pub(super) fn commit_published_track(
        &mut self,
        pending: PendingPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(String, Vec<PendingConsumerBootstrapTarget>)> {
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
            pending.producer_wire_id.clone(),
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
        self.producer_wire_ids_by_owner_stream.insert(
            ProducerKey::new(&pending.owner_session_id, pending.stream_type),
            pending.producer_wire_id.clone(),
        );
        self.producer_stream_types_by_transport_media_id
            .insert(transport_media_id, pending.stream_type);
        let consumer_targets = self.publish_consumer_targets(
            &pending.owner_session_id,
            pending.owner_connection_id,
            &pending.producer_wire_id,
            pending.stream_type,
            pending.media_kind,
            transport_media_id,
        );
        Some((pending.producer_wire_id, consumer_targets))
    }

    #[must_use]
    pub(super) fn producer_stream_type_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.producer_stream_types_by_transport_media_id
            .get(&transport_media_id)
            .copied()
    }

    #[must_use]
    pub(super) fn producer_route_target(
        &self,
        owner_session_id: &SessionId,
        owner_connection_id: u64,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let producer_wire_id = self
            .producer_wire_ids_by_owner_stream
            .get(&ProducerKey::new(owner_session_id, stream_type))?;
        let producer = self.producers.get(producer_wire_id)?;
        if producer.owner_connection_id != owner_connection_id {
            return None;
        }
        let transport_media_id = producer.transport_media_id?;
        Some(ProducerRouteTarget {
            producer_wire_id: producer_wire_id.clone(),
            owner_connection_id: producer.owner_connection_id,
            routed_producer_id: producer.routed_producer_id,
            transport_media_id,
        })
    }

    pub(super) fn prepare_consumer_bootstrap_transaction(
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
        let producer = self.producers.get(&prepared.producer_wire_id)?;
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
        let consumer_id = allocate_wire_consumer_id(&mut self.next_consumer_id);
        Some(PendingConsumerBootstrap {
            sender: prepared.sender.clone(),
            request: CurrentServerRequest::BootstrapRemoteTrack(
                CurrentRemoteTrackBootstrapPayload {
                    id: consumer_id,
                    media_kind: prepared.producer_media_kind,
                    source_id: prepared.producer_wire_id.clone(),
                    rtp_parameters: prepared.consumer_wire_rtp_parameters.clone(),
                    session_id: prepared.producer_owner_session_id.clone(),
                    active: prepared.producer_active,
                    stream_type: prepared.producer_stream_type,
                },
            ),
            producer_owner_session_id: prepared.producer_owner_session_id.clone(),
            producer_connection_id: prepared.producer_connection_id,
            producer_stream_type: prepared.producer_stream_type,
            producer_media_kind: prepared.producer_media_kind,
            producer_routed_id: prepared.producer_routed_id,
            producer_wire_id: prepared.producer_wire_id.clone(),
            producer_active: prepared.producer_active,
        })
    }

    pub(super) fn prepare_consumer_bootstrap(
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
        let producer = self.producers.get(&target.producer_wire_id)?;
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
            producer_wire_id: target.producer_wire_id.clone(),
            producer_active,
        })
    }

    pub(super) fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
    ) -> Option<(mpsc::UnboundedSender<SessionOutbound>, CurrentServerRequest)> {
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.negotiation.can_consume()
        {
            return None;
        }
        let producer = self.producers.get(&pending.producer_wire_id)?;
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
            true,
        ) {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_id = %pending.producer_wire_id,
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
        Some((pending.sender, pending.request))
    }

    #[cfg(test)]
    pub(super) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.topology.session_permissions(session_id)
    }

    #[cfg(test)]
    pub(super) fn session_has_parsed_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> bool {
        self.sessions
            .get(session_id)
            .and_then(|session| session.parsed_client_rtp_capabilities.as_ref())
            .is_some()
    }
}

pub(super) fn allocate_wire_producer_id(next_producer_id: &mut u64) -> String {
    let current = *next_producer_id;
    *next_producer_id = next_producer_id.saturating_add(1);
    format!("producer-{current}")
}

pub(super) fn allocate_wire_consumer_id(next_consumer_id: &mut u64) -> String {
    let current = *next_consumer_id;
    *next_consumer_id = next_consumer_id.saturating_add(1);
    format!("consumer-{current}")
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

impl ProducerKey {
    fn new(owner_session_id: &SessionId, stream_type: StreamType) -> Self {
        Self {
            owner_session_id: owner_session_id.clone(),
            stream_type,
        }
    }
}
