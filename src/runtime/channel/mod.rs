use std::collections::BTreeMap;
use std::fmt;

use o_sfu_router::{
    ConsumerId as RouterConsumerId, MediaKind as RouterMediaKind, ProducerId as RouterProducerId,
    RouterId, RtpParameters as RouterRtpParameters, StreamType as RouterStreamType, can_consume,
    derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{RwLock, mpsc};
use tracing::{error, warn};
use uuid::Uuid;

use super::transport_adapter::{RuntimeTransportAdapter, TransportMediaId};

use crate::signaling::{
    bundle_api::bundle_session_info_key,
    current_protocol::{
        CurrentBroadcastPayload, CurrentRemoteTrackBootstrapPayload, CurrentServerMessage,
        CurrentServerRequest, CurrentSessionDeparturePayload, CurrentSessionInfoSnapshotById,
        CurrentWebSocketCloseCode,
    },
    http::{ChannelStats, CreateChannelQuery, IncomingBitRateStats, SessionsStats},
    shared::{
        AvailableFeatures, DownloadStates, RecordingState, SessionId, SessionInfo,
        SessionPermissions, StreamType,
    },
    webrtc::{
        MediaKind as SignalingMediaKind, RtpCapabilities as SignalingRtpCapabilities, RtpParameters,
    },
};

mod manager;
mod router_state;
mod rtp_capabilities;
mod rtp_conversion;
#[cfg(test)]
mod tests;

pub use manager::ChannelManager;
use router_state::ChannelRouterState;

/// A message the server pushes to a connected session's WebSocket handler.
#[derive(Debug, Clone)]
pub enum SessionOutbound {
    /// A fire-and-forget server message wrapped in a Bus envelope by the handler.
    Message(CurrentServerMessage),
    /// A request-style server event wrapped in a Bus envelope by the handler.
    Request(Box<CurrentServerRequest>),
    /// Instruct the handler to close the WebSocket with the given code.
    Close(CurrentWebSocketCloseCode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelJoinError {
    ChannelFull,
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelManagerJoinError {
    MissingChannel,
    ChannelFull,
    RouterState,
}

/// A single discussion channel owning sessions, features, and recording state.
///
/// Identity fields (uuid, issuer, key, features) are immutable after creation.
/// Mutable state (sessions, recording) is behind an interior lock.
pub struct Channel {
    create_date: String,
    uuid: String,
    issuer: String,
    key: Option<String>,
    remote_address: String,
    web_rtc_enabled: bool,
    #[allow(dead_code, reason = "stored for future recording pipeline integration")]
    recording_address: Option<String>,
    state: RwLock<ChannelState>,
}

#[derive(Debug)]
struct ChannelState {
    sessions: BTreeMap<SessionId, ActiveSession>,
    next_connection_id: u64,
    next_producer_id: u64,
    next_consumer_id: u64,
    recording_state: RecordingState,
    producers: BTreeMap<String, PublishedProducer>,
    /// Maps `(consumer_session, producer_session, stream_type)` to router consumer ID.
    /// Populated during `publish_track` and used by `CONSUMPTION_CHANGE` to pause/resume
    /// individual consumers.
    consumer_index: BTreeMap<ConsumerKey, RouterConsumerId>,
    router: ChannelRouterState,
}

/// Composite key for looking up a consumer by consuming session, source session, and stream type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ConsumerKey {
    consumer_session_id: SessionId,
    producer_session_id: SessionId,
    stream_type: StreamType,
}

#[derive(Debug)]
struct ActiveSession {
    #[allow(
        dead_code,
        reason = "stored for future session display and recording metadata"
    )]
    label: Option<String>,
    #[allow(dead_code, reason = "stored for future permission-gated actions")]
    permissions: SessionPermissions,
    info: SessionInfo,
    client_rtp_capabilities: Option<SignalingRtpCapabilities>,
    upload_transport_connected: bool,
    download_transport_connected: bool,
    connection_id: u64,
    sender: mpsc::UnboundedSender<SessionOutbound>,
}

#[derive(Debug, Clone)]
struct PublishedProducer {
    owner_session_id: SessionId,
    stream_type: StreamType,
    media_kind: SignalingMediaKind,
    consumable_rtp_parameters: RouterRtpParameters,
    router_producer_id: RouterProducerId,
    transport_media_id: TransportMediaId,
    active: bool,
}

#[derive(Debug, Clone)]
struct PendingConsumerBootstrapTarget {
    consumer_session_id: SessionId,
    consumer_connection_id: u64,
    producer_session_id: SessionId,
    producer_wire_id: String,
    stream_type: StreamType,
    media_kind: SignalingMediaKind,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, Copy)]
enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
}

impl Channel {
    pub(super) fn new(
        router_id: RouterId,
        issuer: String,
        key: Option<String>,
        remote_address: String,
        query: &CreateChannelQuery,
    ) -> Self {
        Self {
            create_date: rfc3339_now(),
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
            remote_address,
            web_rtc_enabled: query.web_rtc_enabled(),
            recording_address: query.recording_address.clone(),
            state: RwLock::new(ChannelState::new(router_id)),
        }
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    #[cfg(test)]
    #[must_use]
    pub fn create_date(&self) -> &str {
        &self.create_date
    }

    #[must_use]
    pub fn available_features(&self) -> AvailableFeatures {
        AvailableFeatures {
            rtc: self.web_rtc_enabled,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state.clone()
    }

    pub async fn router_rtp_capabilities(&self) -> o_sfu_router::RtpCapabilities {
        self.state.read().await.router.rtp_capabilities().clone()
    }

    pub async fn stats(&self) -> ChannelStats {
        let state = self.state.read().await;
        ChannelStats {
            create_date: self.create_date.clone(),
            uuid: self.uuid.clone(),
            remote_address: self.remote_address.clone(),
            sessions_stats: SessionsStats {
                incoming_bit_rate: IncomingBitRateStats {
                    total: 0,
                    screen: 0,
                    audio: 0,
                    camera: 0,
                },
                count: state.router.session_count(),
                camera_count: state.router.camera_count(),
                screen_count: state.router.screen_count(),
            },
            web_rtc_enabled: self.web_rtc_enabled,
        }
    }

    /// Add a session to this channel. Returns an error if the channel is at capacity
    /// and the session ID is not already present (reconnections bypass the limit).
    ///
    /// A repeated join for the same session ID replaces the previous live connection,
    /// (as in the current odoo sfu in node)
    /// Returns a channel-scoped connection token that must be passed back to
    /// [`Self::leave_session`] so stale disconnects from replaced sockets do not
    /// remove the newer session entry.
    pub async fn join_session(
        &self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        max_sessions: usize,
    ) -> Result<u64, ChannelJoinError> {
        let mut state = self.state.write().await;
        let is_new = !state.sessions.contains_key(&session_id);
        if is_new && state.sessions.len() >= max_sessions {
            return Err(ChannelJoinError::ChannelFull);
        }
        let connection_id = state.next_connection_id;
        state.next_connection_id += 1;
        if is_new
            && state
                .router
                .ensure_session(&session_id, connection_id, &permissions)
                .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session join into channel router"
            );
            return Err(ChannelJoinError::RouterState);
        }
        if is_new && state.router.ensure_session_transports(&session_id).is_err() {
            error!(
                ?session_id,
                "failed to open default transports for joined session in channel router"
            );
            return Err(ChannelJoinError::RouterState);
        }
        let previous_sender = if let Some(session) = state.sessions.get_mut(&session_id) {
            let old_sender = session.sender.clone();
            session.label.clone_from(&label);
            session.permissions.clone_from(&permissions);
            session.info = SessionInfo::default();
            session.client_rtp_capabilities = None;
            session.upload_transport_connected = false;
            session.download_transport_connected = false;
            session.connection_id = connection_id;
            session.sender = sender;
            Some(old_sender)
        } else {
            state.sessions.insert(
                session_id.clone(),
                ActiveSession {
                    label,
                    permissions: permissions.clone(),
                    info: SessionInfo::default(),
                    client_rtp_capabilities: None,
                    upload_transport_connected: false,
                    download_transport_connected: false,
                    connection_id,
                    sender,
                },
            );
            None
        };
        if previous_sender.is_some()
            && state
                .router
                .update_session_permissions(&session_id, &permissions)
                .and_then(|()| {
                    state
                        .router
                        .update_session_info(&session_id, &SessionInfo::default())
                })
                .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session replacement into channel router"
            );
            return Err(ChannelJoinError::RouterState);
        }
        if let Some(old_sender) = previous_sender {
            let _ = old_sender.send(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked));
            let departure = CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                session_id: session_id.clone(),
            });
            send_to_all_except(&state.sessions, &departure, Some(&session_id));
        }
        drop(state);
        Ok(connection_id)
    }

    /// Remove a connection for the given session. When the last connection leaves,
    /// the session is fully removed and `SESSION_LEAVE` is sent to remaining peers.
    ///
    /// Returns `true` only when the currently active connection for `session_id`
    /// was removed. Returns `false` for stale/missing connections or router-sync
    /// failures.
    pub async fn leave_session(&self, session_id: &SessionId, connection_id: u64) -> bool {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get_mut(session_id) else {
            return false;
        };
        if session.connection_id != connection_id {
            return false;
        }
        if state.router.remove_session(session_id).is_err() {
            error!(
                ?session_id,
                "failed to mirror session leave into channel router"
            );
            return false;
        }
        state.sessions.remove(session_id);
        state.purge_session_media_state(session_id);
        let departure = CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
            session_id: session_id.clone(),
        });
        send_to_all(&state.sessions, &departure);
        true
    }

    /// Relay a broadcast message from one session to all other sessions in the channel.
    pub async fn broadcast(&self, sender_id: &SessionId, message: serde_json::Value) {
        let state = self.state.read().await;
        let msg = CurrentServerMessage::Broadcast(CurrentBroadcastPayload {
            sender_id: sender_id.clone(),
            message,
        });
        for (id, session) in &state.sessions {
            if id != sender_id {
                let _ = session.sender.send(SessionOutbound::Message(msg.clone()));
            }
        }
    }

    /// Update a session's info and broadcast the change to all sessions.
    ///
    /// When `need_refresh` is true, a full snapshot of every session's info is sent.
    /// Otherwise only the changed session's info is included.
    pub async fn update_session_info(
        &self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
    ) {
        let mut state = self.state.write().await;
        let updated_info = {
            let Some(session) = state.sessions.get_mut(session_id) else {
                return;
            };
            session.info = info;
            session.info.clone()
        };
        if state
            .router
            .update_session_info(session_id, &updated_info)
            .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session info update into channel router"
            );
            return;
        }
        let snapshot: CurrentSessionInfoSnapshotById = if need_refresh {
            state
                .sessions
                .iter()
                .map(|(id, session)| (bundle_session_info_key(id), session.info.clone()))
                .collect()
        } else {
            BTreeMap::from([(
                bundle_session_info_key(session_id),
                state
                    .sessions
                    .get(session_id)
                    .map_or_else(SessionInfo::default, |session| session.info.clone()),
            )])
        };
        let msg = CurrentServerMessage::SessionInfoChanged(snapshot);
        send_to_all(&state.sessions, &msg);
    }

    /// Disconnect specific sessions by ID. Sends `Close(Kicked)` to each removed
    /// session and `SESSION_LEAVE` to every remaining peer.
    pub async fn disconnect_sessions(&self, session_ids: &[SessionId]) {
        let mut state = self.state.write().await;
        let mut departed = Vec::new();
        for session_id in session_ids {
            if !state.sessions.contains_key(session_id) {
                continue;
            }
            if state.router.remove_session(session_id).is_err() {
                error!(
                    ?session_id,
                    "failed to mirror bulk disconnect into channel router"
                );
                continue;
            }
            if let Some(session) = state.sessions.remove(session_id) {
                state.purge_session_media_state(session_id);
                let _ = session
                    .sender
                    .send(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked));
                departed.push(session_id.clone());
            }
        }
        for departed_id in &departed {
            let departure = CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                session_id: departed_id.clone(),
            });
            send_to_all(&state.sessions, &departure);
        }
    }

    pub async fn set_client_rtp_capabilities(
        &self,
        session_id: &SessionId,
        capabilities: SignalingRtpCapabilities,
    ) -> bool {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get_mut(session_id) else {
            return false;
        };
        session.client_rtp_capabilities = Some(capabilities);
        drop(state);
        true
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "transport-connect state updates intentionally keep the channel lock for one short, contiguous critical section"
    )]
    pub async fn set_transport_connected(
        &self,
        session_id: &SessionId,
        direction: super::transport_adapter::TransportConnectDirection,
    ) -> bool {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get_mut(session_id) else {
            return false;
        };
        match direction {
            super::transport_adapter::TransportConnectDirection::Upload => {
                session.upload_transport_connected = true;
            }
            super::transport_adapter::TransportConnectDirection::Download => {
                session.download_transport_connected = true;
            }
        }
        true
    }

    /// Bootstrap consumers for all existing producers from other sessions onto a
    /// newly consumer-ready session.
    ///
    /// Precondiions: the session must have both download transport connected and
    /// RTP capabilities stored. If either is missing, this is a no-op.
    pub async fn bootstrap_late_join_consumers(
        &self,
        session_id: &SessionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let targets = {
            let state = self.state.read().await;
            state.late_join_consumer_targets(session_id)
        };

        for target in targets {
            self.bootstrap_consumer_target(
                &target,
                None,
                transport_adapter,
                ConsumerBootstrapOrigin::LateJoin,
            )
            .await;
        }
    }

    pub async fn publish_track(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        rtp_parameters: RtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let (publisher_connection_id, router_capabilities) = {
            let state = self.state.read().await;
            let session = state.sessions.get(session_id)?;
            if !session.upload_transport_connected {
                return None;
            }
            (
                session.connection_id,
                state.router.rtp_capabilities().clone(),
            )
        };

        let parsed_rtp_parameters = rtp_conversion::parse_rtp_parameters(&rtp_parameters.0)
            .or_else(|| {
                warn!(
                    ?session_id,
                    "failed to parse producer RTP parameters from wire format"
                );
                None
            })?;
        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&parsed_rtp_parameters, &router_capabilities)
                .map_err(|error| {
                    warn!(
                        ?session_id,
                        ?error,
                        "failed to derive consumable RTP parameters for producer"
                    );
                })
                .ok()?;

        let transport_media_id = match transport_adapter
            .publish_media(session_id, media_kind)
            .await
        {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    ?session_id,
                    "transport adapter rejected publish media declaration"
                );
                return None;
            }
        };
        let (producer_id, consumer_targets) = {
            let mut state = self.state.write().await;
            let result = state.commit_published_track(
                session_id,
                publisher_connection_id,
                stream_type,
                media_kind,
                transport_media_id,
                consumable_rtp_parameters,
            );
            drop(state);
            result?
        };

        for target in consumer_targets {
            self.bootstrap_consumer_target(
                &target,
                Some(&rtp_parameters),
                transport_adapter,
                ConsumerBootstrapOrigin::Publish,
            )
            .await;
        }
        Some(producer_id)
    }

    async fn bootstrap_consumer_target(
        &self,
        target: &PendingConsumerBootstrapTarget,
        source_rtp_fallback: Option<&RtpParameters>,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        if let Err(_error) = transport_adapter
            .consume_media(
                &target.consumer_session_id,
                target.media_kind,
                &target.producer_session_id,
                target.transport_media_id,
            )
            .await
        {
            warn!(
                consumer_session_id = ?target.consumer_session_id,
                producer_session_id = ?target.producer_session_id,
                ?origin,
                "transport adapter rejected consume media declaration"
            );
            return;
        }
        let outbound = {
            let mut state = self.state.write().await;
            state.commit_consumer_bootstrap(target, source_rtp_fallback)
        };
        let Some((sender, request)) = outbound else {
            return;
        };
        let _ = sender.send(SessionOutbound::Request(Box::new(request)));
    }

    /// Handle a `PRODUCTION_CHANGE` message: pause or resume the session's producer
    /// for the given stream type, update session info, and broadcast the change.
    ///
    /// Mirrors the current Node SFU behavior:
    /// 1. Find the producer owned by this session for the stream type.
    /// 2. Set the producer's pause state in the router (propagates to all consumers).
    /// 3. Update session info flags (isCameraOn / isScreenSharingOn).
    /// 4. Broadcast the updated info to all peers.
    pub async fn update_upload_state(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        active: bool,
    ) {
        let mut state = self.state.write().await;
        let producer = state
            .producers
            .values_mut()
            .find(|p| p.owner_session_id == *session_id && p.stream_type == stream_type);
        let Some(producer) = producer else {
            return;
        };
        producer.active = active;
        let router_producer_id = producer.router_producer_id;
        let paused = !active;
        if state
            .router
            .set_producer_paused(router_producer_id, paused)
            .is_err()
        {
            error!(
                ?session_id,
                ?stream_type,
                "failed to set producer pause state in channel router"
            );
            return;
        }
        let Some(session) = state.sessions.get_mut(session_id) else {
            return;
        };
        match stream_type {
            StreamType::Camera => session.info.is_camera_on = Some(active),
            StreamType::Screen => session.info.is_screen_sharing_on = Some(active),
            StreamType::Audio => {}
        }
        let updated_info = session.info.clone();
        if state
            .router
            .update_session_info(session_id, &updated_info)
            .is_err()
        {
            error!(
                ?session_id,
                "failed to mirror session info update into channel router after production change"
            );
        }
        let snapshot: CurrentSessionInfoSnapshotById =
            BTreeMap::from([(bundle_session_info_key(session_id), updated_info)]);
        let msg = CurrentServerMessage::SessionInfoChanged(snapshot);
        send_to_all(&state.sessions, &msg);
    }

    /// Handle a `CONSUMPTION_CHANGE` message: pause or resume specific consumers
    /// that this session has for a remote session's streams.
    ///
    /// Mirrors the current Node SFU behavior:
    /// for each (`stream_type`, `active`) pair in the states, find the consumer
    /// created for (this session, target session, `stream_type`) and set its
    /// local pause state in the router.
    pub async fn update_download_state(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) {
        let mut state = self.state.write().await;
        for (stream_type, active) in states.iter() {
            let key = ConsumerKey {
                consumer_session_id: session_id.clone(),
                producer_session_id: target_session_id.clone(),
                stream_type,
            };
            let Some(router_consumer_id) = state.consumer_index.get(&key).copied() else {
                continue;
            };
            let paused = !active;
            if state
                .router
                .set_consumer_paused(router_consumer_id, paused)
                .is_err()
            {
                error!(
                    ?session_id,
                    ?target_session_id,
                    ?stream_type,
                    "failed to set consumer pause state in channel router"
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn session_count(&self) -> usize {
        self.state.read().await.sessions.len()
    }

    #[cfg(test)]
    pub(super) async fn router_session_count(&self) -> usize {
        usize::try_from(self.state.read().await.router.session_count()).unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    pub(super) async fn router_session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.state
            .read()
            .await
            .router
            .session_permissions(session_id)
    }

    #[cfg(test)]
    pub(super) async fn client_rtp_capabilities(
        &self,
        session_id: &SessionId,
    ) -> Option<SignalingRtpCapabilities> {
        self.state
            .read()
            .await
            .sessions
            .get(session_id)
            .and_then(|session| session.client_rtp_capabilities.clone())
    }

    pub(super) async fn has_session(&self, session_id: &SessionId) -> bool {
        self.state.read().await.sessions.contains_key(session_id)
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.sessions.is_empty()
    }
}

impl ChannelState {
    fn new(router_id: RouterId) -> Self {
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
            consumer_index: BTreeMap::new(),
            router: ChannelRouterState::new(router_id),
        }
    }

    /// Remove all producer and consumer index entries associated with a departing session.
    ///
    /// This covers both directions: producers owned by the session and consumers
    /// where the session appears as either consumer or producer source.
    /// Must be called whenever a session is removed from `self.sessions` so the
    /// channel-level indexes stay consistent with the router's cascade cleanup.
    fn purge_session_media_state(&mut self, session_id: &SessionId) {
        self.producers
            .retain(|_wire_id, producer| producer.owner_session_id != *session_id);
        self.consumer_index.retain(|key, _consumer_id| {
            key.consumer_session_id != *session_id && key.producer_session_id != *session_id
        });
    }

    fn late_join_consumer_targets(
        &self,
        session_id: &SessionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        let Some(session) = self.sessions.get(session_id) else {
            return Vec::new();
        };
        if !session.download_transport_connected || session.client_rtp_capabilities.is_none() {
            return Vec::new();
        }

        self.producers
            .iter()
            .filter_map(|(producer_wire_id, producer)| {
                if producer.owner_session_id == *session_id {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: session_id.clone(),
                    consumer_connection_id: session.connection_id,
                    producer_session_id: producer.owner_session_id.clone(),
                    producer_wire_id: producer_wire_id.clone(),
                    stream_type: producer.stream_type,
                    media_kind: producer.media_kind,
                    transport_media_id: producer.transport_media_id,
                })
            })
            .collect()
    }

    fn publish_consumer_targets(
        &self,
        producer_session_id: &SessionId,
        producer_wire_id: &str,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        transport_media_id: TransportMediaId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.sessions
            .iter()
            .filter_map(|(peer_session_id, peer_session)| {
                if peer_session_id == producer_session_id
                    || !peer_session.download_transport_connected
                    || peer_session.client_rtp_capabilities.is_none()
                {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: peer_session_id.clone(),
                    consumer_connection_id: peer_session.connection_id,
                    producer_session_id: producer_session_id.clone(),
                    producer_wire_id: producer_wire_id.to_owned(),
                    stream_type,
                    media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    fn commit_published_track(
        &mut self,
        session_id: &SessionId,
        publisher_connection_id: u64,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        transport_media_id: TransportMediaId,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<(String, Vec<PendingConsumerBootstrapTarget>)> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != publisher_connection_id || !session.upload_transport_connected {
            return None;
        }
        let router_producer_id = match self.router.add_producer(
            session_id,
            to_router_media_kind(media_kind),
            to_router_stream_type(stream_type),
        ) {
            Ok(producer_id) => producer_id,
            Err(_error) => {
                error!(
                    ?session_id,
                    "failed to mirror publish request into channel router producer state"
                );
                return None;
            }
        };
        let producer_id = allocate_wire_producer_id(&mut self.next_producer_id);
        self.producers.insert(
            producer_id.clone(),
            PublishedProducer {
                owner_session_id: session_id.clone(),
                stream_type,
                media_kind,
                consumable_rtp_parameters,
                router_producer_id,
                transport_media_id,
                active: true,
            },
        );
        let consumer_targets = self.publish_consumer_targets(
            session_id,
            &producer_id,
            stream_type,
            media_kind,
            transport_media_id,
        );
        Some((producer_id, consumer_targets))
    }

    fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        source_rtp_fallback: Option<&RtpParameters>,
    ) -> Option<(mpsc::UnboundedSender<SessionOutbound>, CurrentServerRequest)> {
        let (sender, client_capabilities) = {
            let session = self.sessions.get(&target.consumer_session_id)?;
            if session.connection_id != target.consumer_connection_id
                || !session.download_transport_connected
            {
                return None;
            }
            (
                session.sender.clone(),
                session.client_rtp_capabilities.clone()?,
            )
        };
        let producer = self.producers.get(&target.producer_wire_id)?;
        if producer.owner_session_id != target.producer_session_id
            || producer.stream_type != target.stream_type
            || producer.media_kind != target.media_kind
            || producer.transport_media_id != target.transport_media_id
        {
            return None;
        }
        let producer_owner_session_id = producer.owner_session_id.clone();
        let producer_stream_type = producer.stream_type;
        let producer_media_kind = producer.media_kind;
        let producer_router_producer_id = producer.router_producer_id;
        let producer_consumable_rtp_parameters = producer.consumable_rtp_parameters.clone();
        let producer_active = producer.active;

        let parsed_capabilities = rtp_conversion::parse_rtp_capabilities(&client_capabilities.0);
        let capable = parsed_capabilities
            .as_ref()
            .is_some_and(|caps| can_consume(&producer_consumable_rtp_parameters, caps));
        let negotiated_wire_params = parsed_capabilities
            .as_ref()
            .and_then(|caps| {
                negotiate_consumer_rtp_parameters(&producer_consumable_rtp_parameters, caps).ok()
            })
            .map(|negotiated| RtpParameters(rtp_conversion::serialize_rtp_parameters(&negotiated)));
        let consumer_rtp_parameters = negotiated_wire_params.unwrap_or_else(|| {
            source_rtp_fallback.cloned().unwrap_or_else(|| {
                RtpParameters(rtp_conversion::serialize_rtp_parameters(
                    &producer_consumable_rtp_parameters,
                ))
            })
        });
        let consumer_id = allocate_wire_consumer_id(&mut self.next_consumer_id);
        let router_consumer_id = match self.router.add_consumer(
            &target.consumer_session_id,
            producer_router_producer_id,
            to_router_media_kind(producer_media_kind),
            to_router_stream_type(producer_stream_type),
            capable,
        ) {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_id = %target.producer_wire_id,
                    ?capable,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        self.consumer_index.insert(
            ConsumerKey {
                consumer_session_id: target.consumer_session_id.clone(),
                producer_session_id: producer_owner_session_id.clone(),
                stream_type: producer_stream_type,
            },
            router_consumer_id,
        );
        Some((
            sender,
            CurrentServerRequest::BootstrapRemoteTrack(CurrentRemoteTrackBootstrapPayload {
                id: consumer_id,
                media_kind: producer_media_kind,
                source_id: target.producer_wire_id.clone(),
                rtp_parameters: consumer_rtp_parameters,
                session_id: producer_owner_session_id,
                active: producer_active,
                stream_type: producer_stream_type,
            }),
        ))
    }
}

fn allocate_wire_producer_id(next_producer_id: &mut u64) -> String {
    let current = *next_producer_id;
    *next_producer_id = next_producer_id.saturating_add(1);
    format!("producer-{current}")
}

fn allocate_wire_consumer_id(next_consumer_id: &mut u64) -> String {
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

fn send_to_all(sessions: &BTreeMap<SessionId, ActiveSession>, msg: &CurrentServerMessage) {
    for session in sessions.values() {
        let _ = session.sender.send(SessionOutbound::Message(msg.clone()));
    }
}

fn send_to_all_except(
    sessions: &BTreeMap<SessionId, ActiveSession>,
    msg: &CurrentServerMessage,
    excluded_session_id: Option<&SessionId>,
) {
    for (session_id, session) in sessions {
        if excluded_session_id.is_some_and(|excluded| excluded == session_id) {
            continue;
        }
        let _ = session.sender.send(SessionOutbound::Message(msg.clone()));
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("create_date", &self.create_date)
            .field("uuid", &self.uuid)
            .field("issuer", &self.issuer)
            .field("remote_address", &self.remote_address)
            .field("web_rtc_enabled", &self.web_rtc_enabled)
            .finish_non_exhaustive()
    }
}

fn rfc3339_now() -> String {
    match OffsetDateTime::now_utc().format(&Rfc3339) {
        Ok(timestamp) => timestamp,
        Err(_error) => String::from("1970-01-01T00:00:00Z"),
    }
}
