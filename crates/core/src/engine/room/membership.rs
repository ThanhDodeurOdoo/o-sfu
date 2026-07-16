#[cfg(any(test, feature = "testing-transport"))]
use o_sfu_router::rtp::MediaCapabilities;
use tracing::warn;

use super::{
    BroadcastPayloadError, Room, RoomJoinError, UserOutboundSender,
    effects::batch::{RoomCommit, RoomEffectContext, RoomEffects},
    media_graph::CommittedTransportReceipt,
    placement::JoinAdmissionTurn,
    state::{ConnectionCloseCommit, RemoteSourceRefresh},
};
use crate::engine::{
    ConnectionId, UserId, UserInfo, UserPermissions, media_transport::MediaTransport,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoomUserPermissions;

impl From<UserPermissions> for RoomUserPermissions {
    fn from(_value: UserPermissions) -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCloseReason {
    Replaced,
    RemovedByRuntime,
}

pub struct JoinUserRequest {
    pub user_id: UserId,
    pub label: Option<String>,
    pub permissions: UserPermissions,
    pub sender: UserOutboundSender,
}

impl Room {
    pub(super) async fn admit_session(
        &self,
        admission: JoinAdmissionTurn<impl FnOnce() -> o_sfu_router::RouterId>,
        context: RoomEffectContext<'_>,
    ) -> Result<CommittedTransportReceipt, RoomJoinError> {
        let joined_fanout = context.user_joined_fanout();
        let commit = admission.commit(self, joined_fanout).await?;
        let receipt = commit.receipt.clone();
        RoomEffects::from_commit(self, RoomCommit::Join(commit))
            .execute(self, context)
            .await;
        Ok(receipt)
    }

    pub(crate) async fn remove_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        self.remove_user_with_teardown(
            user_id,
            connection_id,
            RoomEffectContext::runtime(media_transport),
        )
        .await
    }

    pub async fn remove_user_with_teardown(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        context: RoomEffectContext<'_>,
    ) -> bool {
        let commit = {
            let mut state = self.state.write().await;
            state.close_connection(user_id, connection_id)
        };
        let removed_current_user = matches!(&commit, Some(ConnectionCloseCommit::Current { .. }));
        if let Some(commit) = commit {
            RoomEffects::from_commit(self, RoomCommit::ConnectionClose(commit))
                .execute(self, context)
                .await;
        }
        removed_current_user
    }

    /// The sender identity is checked against authoritative room state before
    /// fan-out is emitted. Stale senders are ignored because websocket code can
    /// race with replacement or teardown.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastPayloadError`] when the payload exceeds the room
    /// broadcast byte limit or cannot be measured as serialized JSON.
    pub(crate) async fn broadcast(
        &self,
        sender_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) -> Result<(), BroadcastPayloadError> {
        let fanout = {
            let state = self.state.read().await;
            state.broadcast_fanout(sender_id, connection_id, message)
        }?;
        if let Some(fanout) = fanout {
            fanout.emit();
        }
        Ok(())
    }

    pub(crate) async fn has_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> bool {
        self.state.read().await.user_connection_id(user_id) == Some(connection_id)
    }

    pub(crate) async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
        info: UserInfo,
    ) {
        let commit = {
            let mut state = self.state.write().await;
            state.apply_presence_update(
                user_id,
                connection_id,
                &info,
                RemoteSourceRefresh::OwnerConsumers,
            )
        };
        if let Some(commit) = commit {
            RoomEffects::from_commit(self, RoomCommit::UserInfo(commit))
                .execute(self, RoomEffectContext::runtime(media_transport))
                .await;
        } else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                refresh_sources = ?RemoteSourceRefresh::OwnerConsumers,
                "user info update was rejected by room state"
            );
        }
    }

    pub(crate) async fn disconnect_users(
        &self,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        self.disconnect_users_with_teardown(user_ids, RoomEffectContext::runtime(media_transport))
            .await;
    }

    pub async fn disconnect_users_with_teardown(
        &self,
        user_ids: &[UserId],
        context: RoomEffectContext<'_>,
    ) {
        let commit = {
            let mut state = self.state.write().await;
            state.apply_disconnect_users(user_ids)
        };
        RoomEffects::from_commit(self, RoomCommit::Disconnect(commit))
            .execute(self, context)
            .await;
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &MediaTransport,
    ) -> Option<()> {
        self.user_operation(user_id, connection_id, media_port)
            .apply_session_negotiated(capabilities)
            .await
    }

    #[cfg(test)]
    pub(super) async fn user_count(&self) -> usize {
        self.state.read().await.user_count()
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
