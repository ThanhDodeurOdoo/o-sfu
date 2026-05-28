use std::sync::Arc;

use o_sfu_protocol::wire::{JsonPayload, ServerMessage, UserId, UserInfo, WelcomePayload};
use tracing::{debug, error};

use super::{User, UserDisconnectReason, UserError, UserOutput};
use crate::{
    core::prelude::{MediaSession, SfuCore, TransportSessionHealth, UserInfoRefresh},
    runtime::{ConnectionId, room::Room},
};

impl User {
    #[must_use]
    pub fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        remote_address: Arc<str>,
        room: Arc<Room>,
        sfu_core: SfuCore,
    ) -> Self {
        Self {
            id: user_id,
            connection_id,
            remote_address,
            sfu_core,
            room,
            state: super::UserState::default(),
            cleanup_finished: false,
        }
    }

    pub(super) fn media(&self) -> MediaSession<'_> {
        self.sfu_core
            .session(self.room.as_ref(), &self.id, self.connection_id)
    }

    /// Return the current transport-driven disconnect reason, if one is known.
    ///
    /// no terminal transport health has been reported yet
    /// disconnection. It does not prove that this is still the current room
    /// connection.
    #[must_use]
    pub fn disconnect_reason(&self) -> Option<UserDisconnectReason> {
        self.media()
            .endpoint_health()
            .and_then(|health| match health {
                TransportSessionHealth::Disconnected => {
                    Some(UserDisconnectReason::TransportDisconnected)
                }
                TransportSessionHealth::Connected => None,
            })
    }

    /// Build the startup output for a room member.
    ///
    /// The output contains the welcome snapshot followed by the initial server
    /// offer request. The caller must send it before entering the steady-state
    /// websocket loop because later client messages depend on the pending
    /// negotiation request stored here.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::InternalError`] if the media transport cannot build
    /// the initial offer.
    pub async fn start(&mut self) -> Result<UserOutput, UserError> {
        let welcome = WelcomePayload {
            features: self.room.available_features(),
            recording: self.room.recording_state().await,
            peers: self.room.user_snapshots_except(&self.id).await,
        };
        let mut output = UserOutput::new().with_signal(ServerMessage::Welcome(welcome).into());
        output.extend(self.create_initial_offer().await?);
        Ok(output)
    }

    /// do the cleanup for this connection.
    ///
    /// This is idempotent and only rolls back staged publishes from this
    /// websocket session. Room membership teardown and transport-session close
    /// remain the responsibility of the runtime room manager.
    pub async fn close(&mut self) {
        if self.cleanup_finished {
            return;
        }
        self.media()
            .publication()
            .rollback_connection_publishes()
            .await;
        self.cleanup_finished = true;
    }

    /// Apply a client-visible user-info update from this websocket.
    ///
    /// Stale connections are rejected before the room update is attempted. A
    /// successful update fans out through room state, so this method normally
    /// returns an empty direct output for the caller's socket.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] if this connection is no longer current.
    /// Returns [`UserError::ProtocolViolation`] when the payload exceeds the
    /// room broadcast byte limit.
    pub async fn update_info(&self, info: UserInfo) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.media()
            .presence()
            .update_info(info, UserInfoRefresh::NotNeeded)
            .await;
        Ok(UserOutput::new())
    }

    /// Fan a client-authored opaque broadcast through room state.
    ///
    /// The sender connection is checked against authoritative room membership.
    /// The sender does not receive an echo through this direct output.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] if this connection is no longer current.
    /// Returns [`UserError::ProtocolViolation`] when the payload exceeds the
    /// room broadcast byte limit.
    pub async fn broadcast(&self, message: JsonPayload) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.room
            .broadcast(&self.id, self.connection_id, message)
            .await
            .map_err(|_error| UserError::ProtocolViolation)?;
        Ok(UserOutput::new())
    }

    /// Reject work from a websocket that is no longer current in the room.
    ///
    /// Replacement sockets reuse the same user id with a new connection id, so
    /// every client intent must prove that this exact connection is still the
    /// valid room member before it mutates room or media state.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] when the room has replaced this connection
    /// id for the user.
    pub(super) async fn reject_stale_connection(&self) -> Result<(), UserError> {
        if self.room.has_connection(&self.id, self.connection_id).await {
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
    /// Report missed explicit cleanup paths.
    ///
    /// `Drop` cannot await staged-publish rollback. The runtime must call
    /// [`User::close`] before this value is dropped.
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
