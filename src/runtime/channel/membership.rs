use std::collections::BTreeMap;

use tokio::sync::mpsc;
use tracing::error;

use crate::runtime::transport_adapter::TransportConnectDirection;
use crate::signaling::{
    bundle_api::bundle_session_info_key,
    current_protocol::{
        CurrentBroadcastPayload, CurrentServerMessage, CurrentSessionDeparturePayload,
        CurrentSessionInfoSnapshotById, CurrentWebSocketCloseCode,
    },
    shared::{SessionId, SessionInfo, SessionPermissions},
    webrtc::RtpCapabilities as SignalingRtpCapabilities,
};

use super::{
    Channel, ChannelJoinError, SessionOutbound,
    outbound::{send_to_all, send_to_all_except},
};

impl Channel {
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
        state.next_connection_id = state.next_connection_id.saturating_add(1);
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
                super::state::ActiveSession {
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

    pub async fn broadcast(&self, sender_id: &SessionId, message: serde_json::Value) {
        let state = self.state.read().await;
        let message = CurrentServerMessage::Broadcast(CurrentBroadcastPayload {
            sender_id: sender_id.clone(),
            message,
        });
        send_to_all_except(&state.sessions, &message, Some(sender_id));
    }

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
        send_to_all(
            &state.sessions,
            &CurrentServerMessage::SessionInfoChanged(snapshot),
        );
    }

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
        {
            let Some(session) = state.sessions.get_mut(session_id) else {
                return false;
            };
            session.client_rtp_capabilities = Some(capabilities);
        }
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
        direction: TransportConnectDirection,
    ) -> bool {
        {
            let mut state = self.state.write().await;
            let Some(session) = state.sessions.get_mut(session_id) else {
                return false;
            };
            match direction {
                TransportConnectDirection::Upload => {
                    session.upload_transport_connected = true;
                }
                TransportConnectDirection::Download => {
                    session.download_transport_connected = true;
                }
            }
        }
        true
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
        self.state.read().await.session_permissions(session_id)
    }

    pub(super) async fn has_session(&self, session_id: &SessionId) -> bool {
        self.state.read().await.sessions.contains_key(session_id)
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.sessions.is_empty()
    }
}
