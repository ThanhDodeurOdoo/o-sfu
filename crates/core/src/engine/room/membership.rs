use std::future::Future;

#[cfg(any(test, feature = "testing-transport"))]
use o_sfu_router::MediaCapabilities;
use o_sfu_router::RouterId;
use tracing::warn;

use super::{
    BroadcastPayloadError, Room, RoomJoinError, RoomMediaCounts, UserOutboundSender,
    cleanup::TransportCleanupOperation,
    effects::{
        self,
        batch::{RoomEffectContext, RoomGaugeDelta},
    },
    placement::PendingJoinPlacement,
    routing::CommittedRoutingReceipt,
    state::RoomState,
};
use crate::engine::{
    ConnectionId, UserId, UserInfo, UserPermissions, media_transport::MediaTransport,
};

pub struct JoinUserRequest {
    pub user_id: UserId,
    pub label: Option<String>,
    pub permissions: UserPermissions,
    pub sender: UserOutboundSender,
}

#[derive(Debug, Clone, Copy)]
struct MembershipCountSnapshot {
    users: usize,
    media: RoomMediaCounts,
}

impl MembershipCountSnapshot {
    fn from_state(state: &RoomState) -> Self {
        Self {
            users: state.user_count(),
            media: state.media_counts(),
        }
    }

    fn delta_to(self, after: Self) -> RoomGaugeDelta {
        RoomGaugeDelta::membership(self.users, after.users, self.media, after.media)
    }
}

impl Room {
    pub(super) async fn admit_session<Fut>(
        &self,
        request: JoinUserRequest,
        worker_loads: super::placement::WorkerLoadIndex,
        context: RoomEffectContext<'_>,
        after_planning: Fut,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> Result<CommittedRoutingReceipt, RoomJoinError>
    where
        Fut: Future<Output = ()>,
    {
        let placement = self.plan_join_placement(worker_loads).await;
        after_planning.await;
        self.join_session_with_cleanup(request, true, placement, context, allocate_spillover_router)
            .await
    }

    pub(super) async fn join_session_with_cleanup(
        &self,
        request: JoinUserRequest,
        emit_joined_fanout: bool,
        placement: PendingJoinPlacement,
        context: RoomEffectContext<'_>,
        allocate_spillover_router: impl FnOnce() -> RouterId,
    ) -> Result<CommittedRoutingReceipt, RoomJoinError> {
        let (outcome, counts) = {
            let mut state = self.state.write().await;
            let before = MembershipCountSnapshot::from_state(&state);
            let outcome = placement.commit_join(
                &mut state,
                request,
                emit_joined_fanout,
                allocate_spillover_router,
            )?;
            let counts = before.delta_to(MembershipCountSnapshot::from_state(&state));
            drop(state);
            (outcome, counts)
        };
        let (batch, receipt) = effects::batch::build_join(self, counts, outcome);
        batch.execute(self, context).await;
        self.reconcile_spillover_routers().await;
        Ok(receipt)
    }

    pub(crate) async fn remove_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        self.remove_user_with_cleanup(
            user_id,
            connection_id,
            RoomEffectContext::runtime(media_transport),
        )
        .await
    }

    pub async fn remove_user_with_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        context: RoomEffectContext<'_>,
    ) -> bool {
        let (outcome, transport_close, counts) = {
            let mut state = self.state.write().await;
            let before = MembershipCountSnapshot::from_state(&state);
            let transport_close = state
                .committed_transport_user_key(user_id, connection_id)
                .map(|session_key| TransportCleanupOperation::CloseUser { session_key });
            let outcome = state.apply_leave(user_id, connection_id);
            if outcome.is_none() {
                state
                    .topology
                    .unregister_committed_placement(user_id, connection_id);
            }
            let counts = before.delta_to(MembershipCountSnapshot::from_state(&state));
            drop(state);
            (outcome, transport_close, counts)
        };
        let had_state = outcome.is_some();
        let staged_cleanup = self.drain_staged_publish_cleanup_operations(user_id, connection_id);
        effects::batch::build_connection_close(
            self,
            counts,
            outcome,
            user_id.clone(),
            connection_id,
            staged_cleanup,
            transport_close,
        )
        .execute(self, context)
        .await;
        self.reconcile_spillover_routers().await;
        had_state
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
        let outcome = {
            let mut state = self.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info, false)
        };
        if let Some(outcome) = outcome {
            effects::batch::build_user_info_update(outcome)
                .execute(self, RoomEffectContext::runtime(media_transport))
                .await;
        } else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                need_refresh = false,
                "user info update was rejected by room state"
            );
        }
    }

    pub(crate) async fn disconnect_users(
        &self,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        self.disconnect_users_with_cleanup(user_ids, RoomEffectContext::runtime(media_transport))
            .await;
    }

    pub async fn disconnect_users_with_cleanup(
        &self,
        user_ids: &[UserId],
        context: RoomEffectContext<'_>,
    ) {
        let (outcome, counts) = {
            let mut state = self.state.write().await;
            let before = MembershipCountSnapshot::from_state(&state);
            let outcome = state.apply_disconnect_users(user_ids);
            let counts = before.delta_to(MembershipCountSnapshot::from_state(&state));
            drop(state);
            (outcome, counts)
        };
        let staged_cleanup = outcome
            .disconnected_users
            .iter()
            .flat_map(|session| {
                self.drain_staged_publish_cleanup_operations(
                    &session.user_id,
                    session.connection_id,
                )
            })
            .collect::<Vec<_>>();
        effects::batch::build_disconnect(self, counts, outcome, staged_cleanup)
            .execute(self, context)
            .await;
        self.reconcile_spillover_routers().await;
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
