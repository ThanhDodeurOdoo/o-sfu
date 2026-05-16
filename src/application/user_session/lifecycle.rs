use std::sync::Arc;

use o_sfu_protocol::{
    shared::{JsonPayload, UserId, UserInfo},
    signaling::{ServerMessage, WelcomePayload},
};
use tracing::{debug, error};

use super::{User, UserDisconnectReason, UserError, UserOutput};
use crate::{
    core::{MediaEndpointHealth, MediaSession, SfuCore, UserInfoRefresh},
    runtime::{ConnectionId, room::Room},
};

impl User {
    /// Create the application session for a room-admitted websocket user.
    ///
    /// The caller must pass the normalized user id, the connection id returned
    /// by room admission and shared room/core handles. Construction does not
    /// emit the welcome payload or allocate the first offer. Call
    /// [`User::start`] to perform that post-admission initialization.
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

    /// Rebuild a borrow-based media session for this room, user and runtime
    /// connection identity.
    pub(super) fn media(&self) -> MediaSession<'_> {
        self.sfu_core
            .session(self.room.as_ref(), &self.id, self.connection_id)
    }

    /// Return the current transport-driven disconnect reason, if one is known.
    ///
    /// `None` means the transport backend has not reported a terminal
    /// disconnection. It does not prove that the room still owns this
    /// connection.
    #[must_use]
    pub fn disconnect_reason(&self) -> Option<UserDisconnectReason> {
        self.media()
            .endpoint_health()
            .and_then(|health| match health {
                MediaEndpointHealth::Disconnected => {
                    Some(UserDisconnectReason::TransportDisconnected)
                }
                MediaEndpointHealth::Connected => None,
            })
    }

    /// Build the startup output for an authenticated room member.
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

    /// Run mandatory explicit cleanup for this connection.
    ///
    /// This is idempotent and only rolls back staged publishes owned by this
    /// websocket session. Room membership teardown and transport-session close
    /// remain the responsibility of the runtime room manager.
    pub async fn close(&mut self) {
        if self.cleanup_finished {
            return;
        }
        self.media().rollback_connection_publishes().await;
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
    /// Returns [`UserError::Kicked`] if the room no longer owns this connection.
    /// Returns [`UserError::ProtocolViolation`] when the payload exceeds the
    /// room broadcast byte limit.
    pub async fn update_info(&self, info: UserInfo) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.media()
            .update_user_info(info, UserInfoRefresh::NotNeeded)
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
    /// Returns [`UserError::Kicked`] if the room no longer owns this connection.
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
