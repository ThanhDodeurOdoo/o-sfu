use o_sfu_router::{RouterId, test_support::rtp_samples::sample_client_rtp_capabilities};

use super::super::super::{
    JoinPlacementPlan, JoinSessionIntent, Room, RoomEffectContext, RoomJoinError,
    UserOutboundSender, placement::WorkerLoadIndex,
};
use crate::{
    SessionNegotiationOutcome,
    engine::{
        ConnectionId, UserId, UserPermissions,
        media_transport::{MediaTransport, TransportAdapterError, TransportWorkerPressureSnapshot},
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
        sender: UserOutboundSender,
    ) -> Result<ConnectionId, RoomJoinError> {
        let placement = state_only_join_plan(self.room, Vec::new()).await;
        self.room
            .join_session_with_cleanup(
                JoinSessionIntent {
                    user_id,
                    label,
                    permissions,
                    sender,
                    emit_joined_fanout: false,
                    placement,
                },
                RoomEffectContext::state_only(None),
                || RouterId(0),
            )
            .await
            .map(|receipt| receipt.connection_id)
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
        let placement =
            state_only_join_plan(self.room, media_transport.worker_pressure_snapshots()).await;
        self.room
            .join_session_with_cleanup(
                JoinSessionIntent {
                    user_id,
                    label,
                    permissions,
                    sender,
                    emit_joined_fanout: false,
                    placement,
                },
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

    /// drives one joined session to negotiated readiness through the real media transport
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the user is absent or the
    /// transport cannot create the initial offer used by the room readiness
    /// transition
    pub async fn make_session_ready(
        self,
        user_id: &UserId,
        media_transport: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        let connection_id = self
            .room
            .state
            .read()
            .await
            .user_connection_id(user_id)
            .ok_or(TransportAdapterError::InvalidInput)?;
        let session_key = self.room.transport_user_key(user_id, connection_id).await;
        media_transport
            .create_initial_session_offer(&session_key)
            .await?;
        match self
            .room
            .apply_session_negotiated(
                user_id,
                connection_id,
                sample_client_rtp_capabilities(),
                media_transport,
            )
            .await
        {
            SessionNegotiationOutcome::Applied => Ok(()),
            SessionNegotiationOutcome::StaleConnection => Err(TransportAdapterError::InvalidInput),
        }
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
    let contribution = room.worker_load_contribution().await;
    for id in contribution.session_worker_ids {
        loads.record_session(id);
    }
    for id in contribution.consumer_worker_ids {
        loads.record_consumer(id);
    }
    let plan = room.plan_join_placement(loads).await;
    let snapshot = room.placement_usage_snapshot().await;
    JoinPlacementPlan::Resolved(
        plan.resolve_for_commit(&snapshot, || snapshot.next_local_router_id()),
    )
}
