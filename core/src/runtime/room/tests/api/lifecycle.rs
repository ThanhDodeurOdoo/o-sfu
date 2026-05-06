use tokio::sync::mpsc;

use super::super::super::{Room, RoomJoinError, UserCleanup, UserOutbound};
use crate::{
    UserInfoRefresh,
    runtime::{
        ConnectionId, RecordingOptions, UserId, UserInfo, UserPermissions,
        media_transport::MediaTransport,
    },
};

#[derive(Clone, Copy)]
pub struct RoomTestLifecycle<'a> {
    pub(super) room: &'a Room,
}

impl RoomTestLifecycle<'_> {
    /// Join one user through the room lifecycle path used by production calls.
    ///
    /// # Errors
    ///
    /// Returns [`RoomJoinError`] when admission fails or router state rejects
    /// the user.
    pub async fn join_user(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.room
            .join_session_with_cleanup(
                user_id,
                label,
                permissions,
                sender,
                UserCleanup::state_only(None),
                false,
            )
            .await
    }

    /// Join one user while keeping transport cleanup outside the lifecycle path.
    ///
    /// # Errors
    ///
    /// Returns [`RoomJoinError`] when admission fails or router state rejects
    /// the user.
    pub async fn join_session_without_transport_cleanup(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        media_transport: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.room
            .join_session_with_cleanup(
                user_id,
                label,
                permissions,
                sender,
                UserCleanup::state_only(Some(media_transport)),
                false,
            )
            .await
    }

    pub async fn leave_user(self, user_id: &UserId, connection_id: ConnectionId) -> bool {
        self.leave_session_with_cleanup(user_id, connection_id, UserCleanup::state_only(None))
            .await
    }

    pub async fn leave_session_runtime(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        self.leave_session_with_cleanup(
            user_id,
            connection_id,
            UserCleanup::runtime(media_transport),
        )
        .await
    }

    pub async fn leave_session_without_transport_cleanup(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        self.leave_session_with_cleanup(
            user_id,
            connection_id,
            UserCleanup::state_only(Some(media_transport)),
        )
        .await
    }

    async fn leave_session_with_cleanup(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) -> bool {
        self.room
            .remove_user_with_cleanup(user_id, connection_id, cleanup)
            .await
    }

    pub async fn broadcast(self, sender_id: &UserId, message: serde_json::Value) {
        let Some(connection_id) = self.room.state.read().await.user_connection_id(sender_id) else {
            return;
        };
        self.room.broadcast(sender_id, connection_id, message).await;
    }

    pub async fn update_user_info_runtime(
        self,
        user_id: &UserId,
        info: UserInfo,
        need_refresh: bool,
        media_transport: &MediaTransport,
    ) {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await
        else {
            return;
        };
        self.room
            .update_user_info(
                user_id,
                connection_id,
                info,
                UserInfoRefresh::from_needed(need_refresh),
                media_transport,
            )
            .await;
    }

    pub async fn disconnect_users(self, user_ids: &[UserId]) {
        self.room
            .disconnect_users_with_cleanup(user_ids, UserCleanup::state_only(None))
            .await;
    }

    pub async fn disconnect_users_without_transport_cleanup(
        self,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        self.room
            .disconnect_users_with_cleanup(user_ids, UserCleanup::state_only(Some(media_transport)))
            .await;
    }

    pub async fn force_cleanup_retry_cycle(self, media_transport: &MediaTransport) {
        self.room
            .force_cleanup_retry_cycle_for_test(media_transport)
            .await;
    }

    #[must_use]
    pub fn pending_cleanup_retry_count(self) -> usize {
        self.room.pending_cleanup_retry_count_for_test()
    }

    pub async fn start_recording(self, user_id: &UserId, options: RecordingOptions) -> bool {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await
        else {
            self.room.metrics.record_recording_start_rejected();
            return false;
        };
        self.room
            .start_recording_runtime(user_id, connection_id, options)
            .await
    }

    pub async fn stop_recording(self, user_id: &UserId) -> bool {
        let Some(connection_id) = self
            .room
            .test_api()
            .inspect()
            .user_connection_id(user_id)
            .await
        else {
            self.room.metrics.record_recording_stop_rejected();
            return false;
        };
        self.room
            .stop_recording_runtime(user_id, connection_id)
            .await
    }
}
