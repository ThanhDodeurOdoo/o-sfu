use o_sfu_protocol::{
    shared::{UserId, UserInfo, UserPermissions},
    signaling::RecordingOptions,
};
use tokio::sync::mpsc;

use super::super::super::{Room, RoomJoinError, UserCleanup, UserOutbound};
use crate::runtime::{ConnectionId, transport_adapter::RuntimeTransportAdapter};

#[derive(Clone, Copy)]
pub(crate) struct RoomTestLifecycle<'a> {
    pub(super) room: &'a Room,
}

impl RoomTestLifecycle<'_> {
    pub(crate) async fn join_user(
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

    pub(crate) async fn join_session_without_transport_cleanup(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.room
            .join_session_with_cleanup(
                user_id,
                label,
                permissions,
                sender,
                UserCleanup::state_only(Some(transport_adapter)),
                false,
            )
            .await
    }

    pub(crate) async fn leave_user(self, user_id: &UserId, connection_id: ConnectionId) -> bool {
        self.leave_session_with_cleanup(user_id, connection_id, UserCleanup::state_only(None))
            .await
    }

    pub(crate) async fn leave_session_runtime(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.leave_session_with_cleanup(
            user_id,
            connection_id,
            UserCleanup::runtime(transport_adapter),
        )
        .await
    }

    pub(crate) async fn leave_session_without_transport_cleanup(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.leave_session_with_cleanup(
            user_id,
            connection_id,
            UserCleanup::state_only(Some(transport_adapter)),
        )
        .await
    }

    async fn leave_session_with_cleanup(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) -> bool {
        let room = self.room;
        let outcome = {
            let mut state = room.state.write().await;
            state.apply_leave(user_id, connection_id)
        };
        let Some(outcome) = outcome else {
            return false;
        };
        room.cleanup_transport_removals(cleanup, &outcome.transport_removals)
            .await;
        if let Some(transport_adapter) = cleanup.transport_adapter() {
            room.sync_source_packet_selection_policy(Some(transport_adapter), transport_adapter)
                .await;
        }
        Room::emit_lifecycle_effects(outcome.effects);
        true
    }

    pub(crate) async fn broadcast(self, sender_id: &UserId, message: serde_json::Value) {
        let Some(connection_id) = self.room.state.read().await.user_connection_id(sender_id) else {
            return;
        };
        self.room
            .broadcast_runtime(sender_id, connection_id, message)
            .await;
    }

    pub(crate) async fn update_user_info(
        self,
        user_id: &UserId,
        info: UserInfo,
        need_refresh: bool,
    ) {
        let Some(connection_id) = self.room.state.read().await.user_connection_id(user_id) else {
            return;
        };
        let outcome = {
            let mut state = self.room.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            outcome.emit();
        }
    }

    pub(crate) async fn update_user_info_runtime(
        self,
        user_id: &UserId,
        info: UserInfo,
        need_refresh: bool,
        transport_adapter: &RuntimeTransportAdapter,
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
            .update_user_info_runtime_for_connection(
                user_id,
                connection_id,
                info,
                need_refresh,
                transport_adapter,
            )
            .await;
    }

    pub(crate) async fn disconnect_users(self, user_ids: &[UserId]) {
        self.room
            .disconnect_users_with_cleanup(user_ids, UserCleanup::state_only(None))
            .await;
    }

    pub(crate) async fn disconnect_users_without_transport_cleanup(
        self,
        user_ids: &[UserId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.room
            .disconnect_users_with_cleanup(
                user_ids,
                UserCleanup::state_only(Some(transport_adapter)),
            )
            .await;
    }

    pub(crate) async fn start_recording(self, user_id: &UserId, options: RecordingOptions) -> bool {
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

    pub(crate) async fn stop_recording(self, user_id: &UserId) -> bool {
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
