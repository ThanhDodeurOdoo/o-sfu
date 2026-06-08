use o_sfu_router::{
    MediaCapabilities, RouterId, test_support::rtp_samples::sample_client_rtp_capabilities,
};

use super::super::super::{
    JoinPlacementPlan, JoinUserRequest, Room, RoomEffectContext, RoomJoinError, UserOutboundSender,
    placement::WorkerLoadIndex,
};
use crate::engine::{
    ConnectionId, UserId, UserPermissions,
    media_transport::{MediaTransport, TransportAdapterError, TransportWorkerPressureSnapshot},
};

#[derive(Clone, Copy)]
pub struct RoomTestLifecycle<'a> {
    pub(super) room: &'a Room,
}

impl RoomTestLifecycle<'_> {
    /// # Errors
    ///
    /// returns [`RoomJoinError`] when admission or routing rejects the user
    pub async fn join_user(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
    ) -> Result<ConnectionId, RoomJoinError> {
        let placement = state_only_join_plan(self.room, Vec::new()).await;
        self.room
            .join_session_with_cleanup(
                JoinUserRequest {
                    user_id,
                    label,
                    permissions,
                    sender,
                },
                false,
                placement,
                RoomEffectContext::state_only(None),
                || RouterId(0),
            )
            .await
            .map(|receipt| receipt.connection_id)
    }

    /// # Errors
    ///
    /// returns [`RoomJoinError`] when admission or routing rejects the user
    pub async fn join_session_without_transport_cleanup(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
        media_transport: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        let placement =
            state_only_join_plan(self.room, media_transport.worker_pressure_snapshots()).await;
        self.room
            .join_session_with_cleanup(
                JoinUserRequest {
                    user_id,
                    label,
                    permissions,
                    sender,
                },
                false,
                placement,
                RoomEffectContext::state_only(Some(media_transport)),
                || RouterId(0),
            )
            .await
            .map(|receipt| receipt.connection_id)
    }

    pub async fn force_cleanup_retry_cycle(self, media_transport: &MediaTransport) {
        self.room
            .force_cleanup_retry_cycle_for_test(media_transport)
            .await;
    }

    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the session or initial offer is absent
    pub async fn make_session_ready(
        self,
        user_id: &UserId,
        media_transport: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        let connection_id = self.connection_id(user_id).await?;
        let session_key = self.room.transport_user_key(user_id, connection_id).await;
        media_transport
            .create_initial_session_offer(&session_key)
            .await?;
        if self
            .mark_session_ready(user_id, sample_client_rtp_capabilities(), media_transport)
            .await
        {
            Ok(())
        } else {
            Err(TransportAdapterError::InvalidInput)
        }
    }

    pub async fn mark_session_ready(
        self,
        user_id: &UserId,
        capabilities: MediaCapabilities,
        media_transport: &MediaTransport,
    ) -> bool {
        let Ok(connection_id) = self.connection_id(user_id).await else {
            return false;
        };
        self.room
            .apply_session_negotiated(user_id, connection_id, capabilities, media_transport)
            .await
            .is_some()
    }

    pub async fn refresh_session(self, user_id: &UserId, media_transport: &MediaTransport) -> bool {
        let Ok(connection_id) = self.connection_id(user_id).await else {
            return false;
        };
        self.room
            .user_operation(user_id, connection_id, media_transport)
            .apply_session_refreshed()
            .await
            .is_some()
    }

    async fn connection_id(self, user_id: &UserId) -> Result<ConnectionId, TransportAdapterError> {
        self.room
            .state
            .read()
            .await
            .user_connection_id(user_id)
            .ok_or(TransportAdapterError::InvalidInput)
    }

    #[must_use]
    pub fn pending_cleanup_retry_count(self) -> usize {
        self.room.pending_cleanup_retry_count_for_test()
    }
}

async fn state_only_join_plan(
    room: &Room,
    pressure: Vec<TransportWorkerPressureSnapshot>,
) -> JoinPlacementPlan {
    let mut loads = WorkerLoadIndex::new(room.room_worker_policy().max_local_routers(), pressure);
    room.record_worker_load(&mut loads).await;
    let plan = room.plan_join_placement(loads).await;
    let snapshot = room.placement_usage_snapshot().await;
    JoinPlacementPlan::Resolved(
        plan.resolve_for_commit(&snapshot, || snapshot.next_local_router_id()),
    )
}
