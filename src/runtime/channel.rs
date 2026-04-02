use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

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
    fn new(issuer: String, key: Option<String>, query: &CreateChannelQuery) -> Self {
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
                .map(|(id, s)| (bundle_session_info_key(id), s.info.clone()))
                .collect()
        } else {
            BTreeMap::from([(
                bundle_session_info_key(session_id),
                state
                    .sessions
                    .get(session_id)
                    .map_or_else(SessionInfo::default, |s| s.info.clone()),
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
    pub async fn session_count(&self) -> usize {
        self.state.read().await.sessions.len()
    }
}

/// Send a server message to every session in the map.
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

/// Manages all active channels with idempotent creation by issuer.
#[derive(Debug, Default)]
pub struct ChannelManager {
    state: RwLock<ChannelManagerState>,
}

#[derive(Debug, Default)]
struct ChannelManagerState {
    channels_by_uuid: BTreeMap<String, Arc<Channel>>,
    uuids_by_issuer: BTreeMap<String, String>,
}

impl ChannelManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a channel for the given issuer, or return the existing one.
    /// Channel creation is idempotent: repeated calls with the same issuer
    /// return the same channel regardless of key or query differences.
    pub async fn create_or_get(
        &self,
        issuer: &str,
        key: Option<&str>,
        query: &CreateChannelQuery,
    ) -> Arc<Channel> {
        {
            let state = self.state.read().await;
            if let Some(uuid) = state.uuids_by_issuer.get(issuer)
                && let Some(channel) = state.channels_by_uuid.get(uuid)
            {
                return Arc::clone(channel);
            }
        }
        let mut state = self.state.write().await;
        if let Some(uuid) = state.uuids_by_issuer.get(issuer)
            && let Some(channel) = state.channels_by_uuid.get(uuid)
        {
            return Arc::clone(channel);
        }
        let channel = Arc::new(Channel::new(
            issuer.to_owned(),
            key.map(str::to_owned),
            query,
        ));
        state
            .uuids_by_issuer
            .insert(issuer.to_owned(), channel.uuid.clone());
        state
            .channels_by_uuid
            .insert(channel.uuid.clone(), Arc::clone(&channel));
        channel
    }

    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Channel>> {
        let state = self.state.read().await;
        state.channels_by_uuid.get(uuid).map(Arc::clone)
    }
}

#[cfg(test)]
#[allow(
    clippy::panic,
    reason = "test assertions use panic for clear failure messages"
)]
mod tests {
    use tokio::sync::mpsc;

    use super::{ChannelJoinError, ChannelManager, SessionOutbound};
    use crate::signaling::{
        current_protocol::{CurrentServerMessage, CurrentWebSocketCloseCode},
        http::CreateChannelQuery,
        shared::{SessionId, SessionInfo, SessionPermissions},
    };

    fn test_sender() -> (
        mpsc::UnboundedSender<SessionOutbound>,
        mpsc::UnboundedReceiver<SessionOutbound>,
    ) {
        mpsc::unbounded_channel()
    }

    #[tokio::test]
    async fn channel_manager_is_idempotent_by_issuer() {
        let manager = ChannelManager::new();
        let query = CreateChannelQuery::default();
        let first = manager.create_or_get("issuer-a", None, &query).await;
        let second = manager
            .create_or_get("issuer-a", Some("ignored"), &query)
            .await;
        let third = manager.create_or_get("issuer-b", None, &query).await;
        assert_eq!(first.uuid(), second.uuid());
        assert_ne!(first.uuid(), third.uuid());
    }

    #[tokio::test]
    async fn channel_manager_lookup_by_uuid() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let fetched = manager.get_by_uuid(channel.uuid()).await;
        assert!(fetched.is_some());
        assert_eq!(
            fetched.map(|c| c.uuid().to_owned()),
            Some(channel.uuid().to_owned())
        );
        assert!(manager.get_by_uuid("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn join_session_enforces_capacity() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, _rx1) = test_sender();
        let result = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                1,
            )
            .await;
        assert!(result.is_ok());

        let (tx2, _rx2) = test_sender();
        let result = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                1,
            )
            .await;
        assert_eq!(result, Err(ChannelJoinError::ChannelFull));
    }

    #[tokio::test]
    async fn reconnection_bypasses_capacity_and_replaces_existing_connection() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let first_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                1,
            )
            .await;
        assert!(first_connection.is_ok());

        // Same session ID reconnects — should succeed even at capacity
        let (tx2, mut rx2) = test_sender();
        let second_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx2,
                1,
            )
            .await;
        assert!(second_connection.is_ok());
        assert!(matches!(
            rx1.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));

        let Some(first_connection) = first_connection.ok() else {
            return;
        };
        let Some(second_connection) = second_connection.ok() else {
            return;
        };

        // A stale close from the replaced socket must not remove the new session.
        channel
            .leave_session(&SessionId::Integer(1), first_connection)
            .await;
        assert_eq!(channel.session_count().await, 1);

        channel
            .broadcast(&SessionId::Integer(99), serde_json::json!("hello"))
            .await;
        let msg = rx2.try_recv();
        assert!(msg.is_ok(), "new sender should receive broadcast");

        channel
            .leave_session(&SessionId::Integer(1), second_connection)
            .await;
        assert_eq!(channel.session_count().await, 0);
    }

    #[tokio::test]
    async fn leave_session_sends_departure_to_remaining_peers() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, _rx2) = test_sender();
        let alice_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let bob_connection = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        assert!(alice_connection.is_ok());
        assert!(bob_connection.is_ok());
        let Some(bob_connection) = bob_connection.ok() else {
            return;
        };

        channel
            .leave_session(&SessionId::Integer(2), bob_connection)
            .await;

        let msg = rx1.try_recv();
        assert!(msg.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionDeparted(payload))) = msg {
            assert_eq!(payload.session_id, SessionId::Integer(2));
        } else {
            panic!("expected SessionDeparted, got {msg:?}");
        }
        assert_eq!(channel.session_count().await, 1);
    }

    #[tokio::test]
    async fn replacing_a_session_notifies_remaining_peers() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut alice_rx) = test_sender();
        let (tx2, mut bob_old_rx) = test_sender();
        let (tx3, _bob_new_rx) = test_sender();
        let _alice_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _bob_old_connection = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;

        let _bob_new_connection = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx3,
                10,
            )
            .await;
        assert!(matches!(
            bob_old_rx.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));
        let msg = alice_rx.try_recv();
        assert!(msg.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionDeparted(payload))) = msg {
            assert_eq!(payload.session_id, SessionId::Integer(2));
        } else {
            panic!("expected SessionDeparted, got {msg:?}");
        }
        assert_eq!(channel.session_count().await, 2);
    }

    #[tokio::test]
    async fn broadcast_reaches_all_except_sender() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let (tx3, mut rx3) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(3),
                None,
                SessionPermissions::default(),
                tx3,
                10,
            )
            .await;

        channel
            .broadcast(&SessionId::Integer(2), serde_json::json!({"text": "hi"}))
            .await;

        assert!(rx1.try_recv().is_ok(), "session 1 should receive broadcast");
        assert!(
            rx2.try_recv().is_err(),
            "sender (session 2) should NOT receive own broadcast"
        );
        assert!(rx3.try_recv().is_ok(), "session 3 should receive broadcast");
    }

    #[tokio::test]
    async fn update_session_info_broadcasts_to_all() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;

        let info = SessionInfo {
            is_talking: Some(true),
            ..SessionInfo::default()
        };
        channel
            .update_session_info(&SessionId::Integer(1), info, false)
            .await;

        // Both sessions (including the one that changed) receive the update
        let msg1 = rx1.try_recv();
        let msg2 = rx2.try_recv();
        assert!(msg1.is_ok());
        assert!(msg2.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot))) =
            msg1
        {
            assert!(snapshot.contains_key("1"));
            assert_eq!(snapshot.get("1").and_then(|i| i.is_talking), Some(true));
        } else {
            panic!("expected SessionInfoChanged");
        }
    }

    #[tokio::test]
    async fn update_session_info_with_refresh_sends_full_snapshot() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, _rx2) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;

        let info = SessionInfo {
            is_camera_on: Some(true),
            ..SessionInfo::default()
        };
        channel
            .update_session_info(&SessionId::Integer(1), info, true)
            .await;

        let msg = rx1.try_recv();
        assert!(msg.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot))) =
            msg
        {
            assert_eq!(
                snapshot.len(),
                2,
                "full refresh should include all sessions"
            );
            assert!(snapshot.contains_key("1"));
            assert!(snapshot.contains_key("2"));
        } else {
            panic!("expected SessionInfoChanged with full snapshot");
        }
    }

    #[tokio::test]
    async fn disconnect_sessions_kicks_targets_and_notifies_remaining() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let (tx3, mut rx3) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(3),
                None,
                SessionPermissions::default(),
                tx3,
                10,
            )
            .await;

        channel
            .disconnect_sessions(&[SessionId::Integer(1), SessionId::Integer(2)])
            .await;

        // Kicked sessions receive Close
        let msg1 = rx1.try_recv();
        assert!(msg1.is_ok());
        assert!(matches!(
            msg1.ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));
        let msg2 = rx2.try_recv();
        assert!(msg2.is_ok());
        assert!(matches!(
            msg2.ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));

        // Remaining session 3 receives SESSION_LEAVE for both
        let departure1 = rx3.try_recv();
        let departure2 = rx3.try_recv();
        assert!(departure1.is_ok());
        assert!(departure2.is_ok());

        assert_eq!(channel.session_count().await, 1);
    }

    #[tokio::test]
    async fn disconnect_sessions_target_only_the_active_replaced_session() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let first_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let second_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        assert!(first_connection.is_ok());
        assert!(second_connection.is_ok());
        assert!(matches!(
            rx1.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));

        channel.disconnect_sessions(&[SessionId::Integer(1)]).await;

        assert!(matches!(
            rx2.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));
        assert!(rx1.try_recv().is_err());
        assert_eq!(channel.session_count().await, 0);
    }
}
