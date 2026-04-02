use std::collections::BTreeMap;
use std::fmt;

use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

use crate::signaling::{
    bundle_api::bundle_session_info_key,
    current_protocol::{
        CurrentBroadcastPayload, CurrentServerMessage, CurrentSessionDeparturePayload,
        CurrentSessionInfoSnapshotById, CurrentWebSocketCloseCode,
    },
    http::CreateChannelQuery,
    shared::{AvailableFeatures, RecordingState, SessionId, SessionInfo, SessionPermissions},
};

mod manager;
#[cfg(test)]
mod tests;

pub use manager::ChannelManager;

/// A message the server pushes to a connected session's WebSocket handler.
#[derive(Debug, Clone)]
pub enum SessionOutbound {
    /// A fire-and-forget server message wrapped in a Bus envelope by the handler.
    Message(CurrentServerMessage),
    /// Instruct the handler to close the WebSocket with the given code.
    Close(CurrentWebSocketCloseCode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelJoinError {
    ChannelFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelManagerJoinError {
    MissingChannel,
    ChannelFull,
}

/// A single discussion channel owning sessions, features, and recording state.
///
/// Identity fields (uuid, issuer, key, features) are immutable after creation.
/// Mutable state (sessions, recording) is behind an interior lock.
pub struct Channel {
    uuid: String,
    issuer: String,
    key: Option<String>,
    web_rtc_enabled: bool,
    #[allow(dead_code, reason = "stored for future recording pipeline integration")]
    recording_address: Option<String>,
    state: RwLock<ChannelState>,
}

#[derive(Debug, Default)]
struct ChannelState {
    sessions: BTreeMap<SessionId, ActiveSession>,
    next_connection_id: u64,
    recording_state: RecordingState,
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
    connection_id: u64,
    sender: mpsc::UnboundedSender<SessionOutbound>,
}

impl Channel {
    pub(super) fn new(issuer: String, key: Option<String>, query: &CreateChannelQuery) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
            web_rtc_enabled: query.web_rtc_enabled(),
            recording_address: query.recording_address.clone(),
            state: RwLock::new(ChannelState {
                recording_state: RecordingState {
                    recording: Some(false),
                    audio: Some(false),
                    transcription: Some(false),
                    video: Some(false),
                },
                ..ChannelState::default()
            }),
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
        let previous_sender = if let Some(session) = state.sessions.get_mut(&session_id) {
            let old_sender = session.sender.clone();
            session.label.clone_from(&label);
            session.permissions.clone_from(&permissions);
            session.info = SessionInfo::default();
            session.connection_id = connection_id;
            session.sender = sender;
            Some(old_sender)
        } else {
            state.sessions.insert(
                session_id.clone(),
                ActiveSession {
                    label,
                    permissions,
                    info: SessionInfo::default(),
                    connection_id,
                    sender,
                },
            );
            None
        };
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
    pub async fn leave_session(&self, session_id: &SessionId, connection_id: u64) {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get_mut(session_id) else {
            return;
        };
        if session.connection_id != connection_id {
            return;
        }
        state.sessions.remove(session_id);
        let departure = CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
            session_id: session_id.clone(),
        });
        send_to_all(&state.sessions, &departure);
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
        let Some(session) = state.sessions.get_mut(session_id) else {
            return;
        };
        session.info = info;
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

    #[cfg(test)]
    pub(super) async fn session_count(&self) -> usize {
        self.state.read().await.sessions.len()
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.sessions.is_empty()
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
            .field("uuid", &self.uuid)
            .field("issuer", &self.issuer)
            .field("web_rtc_enabled", &self.web_rtc_enabled)
            .finish_non_exhaustive()
    }
}
