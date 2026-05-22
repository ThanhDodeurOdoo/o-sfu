use o_sfu_router::RouterId;

use super::super::super::{
    JoinPlacement, JoinSessionIntent, Room, RoomJoinError, UserCleanup, UserOutboundSender,
};
use crate::runtime::{ConnectionId, UserId, UserPermissions, media_transport::MediaTransport};

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
        sender: UserOutboundSender,
    ) -> Result<ConnectionId, RoomJoinError> {
        let home_placement = self
            .room
            .local_join_placement_from_worker_pressure(Vec::new())
            .await;
        self.room
            .join_session_with_cleanup(
                JoinSessionIntent {
                    user_id,
                    label,
                    permissions,
                    sender,
                    emit_joined_fanout: false,
                    placement: JoinPlacement::resolved(home_placement),
                },
                UserCleanup::state_only(None),
                || RouterId(0),
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
        sender: UserOutboundSender,
        media_transport: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        let home_placement = self
            .room
            .local_join_placement_from_worker_pressure(media_transport.worker_pressure_snapshots())
            .await;
        self.room
            .join_session_with_cleanup(
                JoinSessionIntent {
                    user_id,
                    label,
                    permissions,
                    sender,
                    emit_joined_fanout: false,
                    placement: JoinPlacement::resolved(home_placement),
                },
                UserCleanup::state_only(Some(media_transport)),
                || RouterId(0),
            )
            .await
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
}
