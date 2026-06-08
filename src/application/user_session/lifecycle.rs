use std::sync::Arc;

use o_sfu_protocol::wire::{ServerEnvelope, ServerMessage, UserId, WelcomePayload};
use tracing::{debug, error};

use super::{User, UserError, UserOutput};
use crate::{
    core::prelude::{SfuCore, TransportSessionHealth},
    runtime::{ConnectionId, media_transport::TransportSessionKey, room::Room},
};

impl User {
    #[must_use]
    pub fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        transport_session_key: TransportSessionKey,
        remote_address: Arc<str>,
        room: &Arc<Room>,
        sfu_core: &SfuCore,
    ) -> Self {
        let session = sfu_core.session_with_transport_key(
            room,
            &user_id,
            connection_id,
            transport_session_key,
        );
        Self {
            id: user_id,
            connection_id,
            remote_address,
            session,
            requests: super::NegotiationRequests::default(),
            tracks: super::TrackSnapshot::default(),
            cleanup_finished: false,
        }
    }

    pub(crate) fn room_id(&self) -> &str {
        self.session.room_id()
    }

    pub(crate) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(crate) const fn id(&self) -> &UserId {
        &self.id
    }

    pub(crate) fn remote_address(&self) -> &str {
        self.remote_address.as_ref()
    }

    pub(crate) async fn is_current_connection(&self) -> bool {
        self.session.is_current_connection().await
    }

    #[must_use]
    pub fn transport_disconnected(&self) -> bool {
        matches!(
            self.session.endpoint_health(),
            Some(TransportSessionHealth::Disconnected)
        )
    }

    pub async fn start(&mut self) -> Result<UserOutput, UserError> {
        let welcome = WelcomePayload {
            features: self.session.available_features(),
            recording: self.session.recording_state().await,
            peers: self.session.peer_snapshots().await,
        };
        self.reject_stale_connection().await?;
        let mut output = vec![ServerEnvelope::Message(ServerMessage::Welcome(welcome))];
        output.extend(self.run_initial_offer().await?);
        Ok(output)
    }

    pub async fn close(&mut self) {
        if self.cleanup_finished {
            return;
        }
        self.requests.clear();
        self.session.close().await;
        self.cleanup_finished = true;
    }

    pub(super) async fn reject_stale_connection(&self) -> Result<(), UserError> {
        if self.is_current_connection().await {
            return Ok(());
        }
        debug!(
            user_id = ?&self.id,
            connection_id = ?self.connection_id,
            "rejecting intent from a stale user connection"
        );
        Err(UserError::Kicked)
    }
}

impl Drop for User {
    fn drop(&mut self) {
        if self.cleanup_finished {
            return;
        }
        error!(
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            "dropped websocket user without completing explicit cleanup"
        );
        debug_assert!(
            self.cleanup_finished,
            "websocket user dropped before explicit cleanup completed"
        );
    }
}
