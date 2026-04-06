use std::collections::BTreeMap;
use std::fmt;

use o_sfu_router::{
    MediaKind as RouterMediaKind, ProducerId as RouterProducerId, RouterId,
    StreamType as RouterStreamType,
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
        AvailableFeatures, RecordingState, SessionId, SessionInfo, SessionPermissions, StreamType,
    },
    webrtc::{
        MediaKind as SignalingMediaKind, RtpCapabilities as SignalingRtpCapabilities, RtpParameters,
    },
};

mod manager;
mod router_state;
mod rtp_capabilities;
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
    #[allow(
        dead_code,
        reason = "published producer metadata is retained for upcoming production/consumption change handling and teardown synchronization"
    )]
    producers: BTreeMap<String, PublishedProducer>,
    router: ChannelRouterState,
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
    #[allow(
        dead_code,
        reason = "producer ownership will be used by production state updates and cleanup paths"
    )]
    owner_session_id: SessionId,
    #[allow(
        dead_code,
        reason = "stream-type specific production updates are planned in the next phase"
    )]
    stream_type: StreamType,
    #[allow(
        dead_code,
        reason = "media-kind specific consumption behavior is planned in the next phase"
    )]
    media_kind: SignalingMediaKind,
    #[allow(
        dead_code,
        reason = "the wire payload is reused when bootstrapping new consumers after initial publish"
    )]
    rtp_parameters: RtpParameters,
    #[allow(
        dead_code,
        reason = "router producer identity is required for future production change and teardown operations"
    )]
    router_producer_id: RouterProducerId,
    #[allow(
        dead_code,
        reason = "transport media identity is required for consuming media and future modifications"
    )]
    transport_media_id: TransportMediaId,
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

    #[allow(
        clippy::significant_drop_tightening,
        clippy::cognitive_complexity,
        clippy::too_many_lines,
        reason = "publish/consumer bootstrap keeps one lock scope so peer snapshots and router updates remain coherent for this minimal path"
    )]
    pub async fn publish_track(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        rtp_parameters: RtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let mut state = self.state.write().await;
        let can_publish = state
            .sessions
            .get(session_id)
            .is_some_and(|session| session.upload_transport_connected);
        if !can_publish {
            return None;
        }
        let router_media_kind = to_router_media_kind(media_kind);
        let router_stream_type = to_router_stream_type(stream_type);
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
        let router_producer_id =
            match state
                .router
                .add_producer(session_id, router_media_kind, router_stream_type)
            {
                Ok(producer_id) => producer_id,
                Err(_error) => {
                    error!(
                        ?session_id,
                        "failed to mirror publish request into channel router producer state"
                    );
                    return None;
                }
            };
        let producer_id = allocate_wire_producer_id(&mut state.next_producer_id);
        state.producers.insert(
            producer_id.clone(),
            PublishedProducer {
                owner_session_id: session_id.clone(),
                stream_type,
                media_kind,
                rtp_parameters: rtp_parameters.clone(),
                router_producer_id,
                transport_media_id,
            },
        );

        let consumer_targets = state
            .sessions
            .iter()
            .filter_map(|(peer_session_id, peer_session)| {
                if peer_session_id == session_id {
                    return None;
                }
                if !peer_session.download_transport_connected
                    || peer_session.client_rtp_capabilities.is_none()
                {
                    return None;
                }
                Some((peer_session_id.clone(), peer_session.sender.clone()))
            })
            .collect::<Vec<_>>();
        for (peer_session_id, peer_sender) in consumer_targets {
            let consumer_id = allocate_wire_consumer_id(&mut state.next_consumer_id);
            if state
                .router
                .add_consumer(
                    &peer_session_id,
                    router_producer_id,
                    router_media_kind,
                    router_stream_type,
                    true,
                )
                .is_err()
            {
                error!(
                    ?peer_session_id,
                    producer_id = %producer_id,
                    "failed to mirror consumer bootstrap into channel router state"
                );
                continue;
            }
            if let Err(_error) = transport_adapter
                .consume_media(&peer_session_id, media_kind, session_id, transport_media_id)
                .await
            {
                warn!(
                    consumer_session_id = ?peer_session_id,
                    producer_session_id = ?session_id,
                    "transport adapter rejected consume media declaration"
                );
                continue;
            }
            let request =
                CurrentServerRequest::BootstrapRemoteTrack(CurrentRemoteTrackBootstrapPayload {
                    id: consumer_id,
                    media_kind,
                    source_id: producer_id.clone(),
                    rtp_parameters: rtp_parameters.clone(),
                    session_id: session_id.clone(),
                    active: true,
                    stream_type,
                });
            let _ = peer_sender.send(SessionOutbound::Request(Box::new(request)));
        }
        Some(producer_id)
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
            router: ChannelRouterState::new(router_id),
        }
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
