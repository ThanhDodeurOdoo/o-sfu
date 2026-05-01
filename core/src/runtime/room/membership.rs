use std::sync::{MutexGuard, PoisonError};

use o_sfu_router::MediaCapabilities;
use tokio::sync::mpsc;
use tracing::warn;

use super::{
    Room, RoomJoinError, RoomUserPermissions, UserOutbound,
    cleanup::{
        CleanupFailureAction, CleanupReconciler, CleanupRetryAction, TransportCleanupOperation,
    },
    state::{
        DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects,
        TransportMediaRemoval,
    },
    user_negotiation::UserNegotiationUpdate,
};
use crate::{
    SessionNegotiationOutcome, UserInfoRefresh,
    runtime::{
        ConnectionId, UserId, UserInfo, UserPermissions,
        diagnostics::DiagnosticsEventData,
        metrics::TransportCleanupFailureKind,
        telemetry::schema::event as telemetry_event,
        transport_adapter::{MediaPort, MediaTransport, SessionPort, TransportAdapterError},
    },
};

fn warn_transport_cleanup_failure(operation: &TransportCleanupOperation, message: &'static str) {
    warn!(
        user_id = ?operation.user_id(),
        connection_id = ?operation.connection_id(),
        transport_media_id = ?operation.transport_media_id(),
        "{message}"
    );
}
#[derive(Clone, Copy)]
pub(in crate::runtime::room) struct UserCleanup<'a> {
    transport_adapter: Option<&'a MediaTransport>,
    clean_transport_state: bool,
}

impl<'a> UserCleanup<'a> {
    pub(in crate::runtime::room) const fn runtime(transport_adapter: &'a MediaTransport) -> Self {
        Self {
            transport_adapter: Some(transport_adapter),
            clean_transport_state: true,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) const fn state_only(
        transport_adapter: Option<&'a MediaTransport>,
    ) -> Self {
        Self {
            transport_adapter,
            clean_transport_state: false,
        }
    }

    pub(in crate::runtime::room) const fn transport_adapter(self) -> Option<&'a MediaTransport> {
        self.transport_adapter
    }

    const fn cleans_transport_state(self) -> bool {
        self.clean_transport_state
    }
}

enum UserTransition<'a> {
    Join {
        user_id: &'a UserId,
        label: Option<String>,
        permissions: RoomUserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        emit_joined_fanout: bool,
    },
    Close {
        user_id: &'a UserId,
        connection_id: ConnectionId,
    },
    Disconnect {
        user_ids: &'a [UserId],
    },
}

enum UserTransitionResult {
    Joined(ConnectionId),
    Applied,
    Missing,
}

enum UserTransitionOutcome {
    Join(JoinUserOutcome),
    Close {
        outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
    },
    Disconnect(DisconnectUsersOutcome),
}

impl Room {
    pub(crate) async fn add_user(
        &self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        transport_adapter: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.join_session_with_cleanup(
            user_id,
            label,
            permissions,
            sender,
            UserCleanup::runtime(transport_adapter),
            true,
        )
        .await
    }

    /// Run the room-owned join transition and perform the deferred cleanup only
    /// after the state lock has been released.
    pub(in crate::runtime::room) async fn join_session_with_cleanup(
        &self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: mpsc::UnboundedSender<UserOutbound>,
        cleanup: UserCleanup<'_>,
        emit_joined_fanout: bool,
    ) -> Result<ConnectionId, RoomJoinError> {
        let UserTransitionResult::Joined(connection_id) = self
            .run_session_transition(
                UserTransition::Join {
                    user_id: &user_id,
                    label,
                    permissions: permissions.into(),
                    sender,
                    emit_joined_fanout,
                },
                cleanup,
            )
            .await?
        else {
            return Err(RoomJoinError::RouterState);
        };
        Ok(connection_id)
    }

    pub(crate) async fn remove_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_adapter: &MediaTransport,
    ) -> bool {
        self.run_session_transition(
            UserTransition::Close {
                user_id,
                connection_id,
            },
            UserCleanup::runtime(transport_adapter),
        )
        .await
        .is_ok_and(|result| !matches!(result, UserTransitionResult::Missing))
    }

    pub async fn broadcast(
        &self,
        sender_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) {
        let fanout = {
            let state = self.state.read().await;
            state.broadcast_fanout(sender_id, connection_id, message)
        };
        if let Some(fanout) = fanout {
            fanout.emit();
        }
    }

    pub async fn has_connection(&self, user_id: &UserId, connection_id: ConnectionId) -> bool {
        self.state.read().await.user_connection_id(user_id) == Some(connection_id)
    }

    pub(crate) async fn update_user_info_runtime_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
        transport_adapter: &MediaTransport,
    ) {
        self.update_user_info_with_transport(
            user_id,
            connection_id,
            info,
            refresh,
            Some(transport_adapter),
        )
        .await;
    }

    async fn update_user_info_with_transport(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
        transport_adapter: Option<&MediaTransport>,
    ) {
        let need_refresh = refresh.is_needed();
        let outcome = {
            let mut state = self.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            if let Some(transport_adapter) = transport_adapter {
                self.sync_source_packet_selection_policy(
                    Some(transport_adapter),
                    transport_adapter,
                )
                .await;
            }
            outcome.emit();
        } else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                need_refresh,
                "user info update was rejected by room state"
            );
        }
    }

    pub(crate) async fn disconnect_sessions_runtime(
        &self,
        user_ids: &[UserId],
        transport_adapter: &MediaTransport,
    ) {
        self.disconnect_users_with_cleanup(user_ids, UserCleanup::runtime(transport_adapter))
            .await;
    }

    pub(in crate::runtime::room) async fn disconnect_users_with_cleanup(
        &self,
        user_ids: &[UserId],
        cleanup: UserCleanup<'_>,
    ) {
        let _ = self
            .run_session_transition(UserTransition::Disconnect { user_ids }, cleanup)
            .await
            .ok();
    }

    /// Runs transport media cleanup after room state has already detached the
    /// corresponding media objects.
    ///
    /// This is an orchestration-only cold path. The caller passes the cleanup
    /// intent captured by the state transition, then this method performs the
    /// async transport effects outside the room state lock. Failed recoverable
    /// effects are recorded in the room cleanup reconciler so the state change
    /// stays committed while transport cleanup still has an owner.
    pub(in crate::runtime::room) async fn cleanup_transport_removals(
        &self,
        cleanup: UserCleanup<'_>,
        removals: &[TransportMediaRemoval],
    ) {
        let Some(transport_adapter) = cleanup.transport_adapter() else {
            return;
        };
        if !cleanup.cleans_transport_state() {
            return;
        }
        for removal in removals {
            let operation = TransportCleanupOperation::RemoveMedia {
                user_id: removal.user().clone(),
                connection_id: removal.connection(),
                transport_media_id: removal.transport_media(),
            };
            if let Err(error) = self
                .execute_transport_cleanup_operation(&operation, transport_adapter)
                .await
            {
                self.record_cleanup_failure(&operation, error, transport_adapter)
                    .await;
            }
        }
        self.reconcile_transport_cleanup_retries(transport_adapter)
            .await;
    }

    /// Closes the transport user that belonged to a room user after membership
    /// teardown has been committed.
    ///
    /// The room must not re-enter mutable state to rediscover this cleanup
    /// later, so the runtime-local user and connection identity are converted
    /// into a `TransportCleanupOperation` before the async adapter call. If the
    /// adapter is temporarily unavailable, the operation is retained by the
    /// room-local reconciler.
    async fn close_transport_user_for_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) {
        let Some(transport_adapter) = cleanup.transport_adapter() else {
            return;
        };
        if !cleanup.cleans_transport_state() {
            return;
        }
        let operation = TransportCleanupOperation::CloseUser {
            user_id: user_id.clone(),
            connection_id,
        };
        if let Err(error) = self
            .execute_transport_cleanup_operation(&operation, transport_adapter)
            .await
        {
            self.record_cleanup_failure(&operation, error, transport_adapter)
                .await;
        }
        self.reconcile_transport_cleanup_retries(transport_adapter)
            .await;
    }

    /// Executes one cleanup operation against the transport adapter.
    ///
    /// This method intentionally contains no retry or metric logic. Keeping the
    /// transport effect separate from reconciliation makes it clear that room
    /// cleanup state is updated synchronously, then async adapter work is
    /// attempted after the relevant room lock has been released.
    async fn execute_transport_cleanup_operation(
        &self,
        operation: &TransportCleanupOperation,
        transport_adapter: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        match operation {
            TransportCleanupOperation::RemoveMedia {
                user_id,
                connection_id,
                transport_media_id,
            } => {
                transport_adapter
                    .remove_media(
                        &self.transport_user_key(user_id, *connection_id),
                        *transport_media_id,
                    )
                    .await
            }
            TransportCleanupOperation::CloseUser {
                user_id,
                connection_id,
            } => {
                transport_adapter
                    .close_session(&self.transport_user_key(user_id, *connection_id))
                    .await
            }
        }
    }

    /// Records a failed first cleanup attempt and performs any immediate
    /// escalation required by the failure class.
    ///
    /// Recoverable failures stay room-owned through the retry queue. Terminal
    /// failures and queue pressure are surfaced through metrics, then media
    /// cleanup failures ask the transport adapter to drop the owning user so a
    /// lower layer can release resources that the room can no longer address
    /// precisely.
    async fn record_cleanup_failure(
        &self,
        operation: &TransportCleanupOperation,
        error: TransportAdapterError,
        transport_adapter: &MediaTransport,
    ) {
        let action = self
            .cleanup_reconciler()
            .record_failure(operation.clone(), error);
        match action {
            CleanupFailureAction::RetryQueued => {
                self.metrics.record_transport_cleanup_retry_scheduled();
                warn_transport_cleanup_failure(operation, "queued transport cleanup retry");
            }
            CleanupFailureAction::RetryAlreadyQueued => {
                warn_transport_cleanup_failure(
                    operation,
                    "transport cleanup retry was already queued",
                );
            }
            CleanupFailureAction::QueueFull => {
                self.metrics
                    .record_transport_cleanup_failure(TransportCleanupFailureKind::QueueFull);
                warn_transport_cleanup_failure(operation, "transport cleanup retry queue is full");
                self.force_transport_cleanup_owner_drop(operation, transport_adapter)
                    .await;
            }
            CleanupFailureAction::Terminal => {
                self.metrics
                    .record_transport_cleanup_failure(TransportCleanupFailureKind::Terminal);
                warn_transport_cleanup_failure(
                    operation,
                    "transport cleanup reached terminal failure",
                );
                self.force_transport_cleanup_owner_drop(operation, transport_adapter)
                    .await;
            }
        }
    }

    /// Drains all cleanup retries that are due in the current reconciliation
    /// cycle.
    ///
    /// The reconciler guard is only held while selecting work or recording the
    /// retry result. Transport adapter calls happen between those bookkeeping
    /// steps, which prevents adapter latency from blocking other room cleanup
    /// decisions.
    async fn reconcile_transport_cleanup_retries(&self, transport_adapter: &MediaTransport) {
        loop {
            let retries = self.cleanup_reconciler().due_retries();
            if retries.is_empty() {
                return;
            }
            for operation in retries {
                let result = self
                    .execute_transport_cleanup_operation(&operation, transport_adapter)
                    .await;
                let action = self
                    .cleanup_reconciler()
                    .record_retry_result(&operation, result);
                self.record_cleanup_retry_action(&operation, action, transport_adapter)
                    .await;
            }
        }
    }

    /// Converts a retry outcome into metrics and final escalation.
    ///
    /// Requeued operations remain owned by the room. Exhausted or terminal media
    /// cleanup operations are escalated by closing the owning transport user
    /// because the exact media resource may no longer be recoverable from room
    /// state.
    async fn record_cleanup_retry_action(
        &self,
        operation: &TransportCleanupOperation,
        action: CleanupRetryAction,
        transport_adapter: &MediaTransport,
    ) {
        match action {
            CleanupRetryAction::Succeeded => {
                self.metrics.record_transport_cleanup_retry_succeeded();
            }
            CleanupRetryAction::Requeued => {
                self.metrics.record_transport_cleanup_retry_scheduled();
                warn_transport_cleanup_failure(operation, "transport cleanup retry failed");
            }
            CleanupRetryAction::Exhausted => {
                self.metrics
                    .record_transport_cleanup_failure(TransportCleanupFailureKind::RetryExhausted);
                warn_transport_cleanup_failure(
                    operation,
                    "transport cleanup retry attempts were exhausted",
                );
                self.force_transport_cleanup_owner_drop(operation, transport_adapter)
                    .await;
            }
            CleanupRetryAction::Terminal => {
                self.metrics
                    .record_transport_cleanup_failure(TransportCleanupFailureKind::Terminal);
                warn_transport_cleanup_failure(
                    operation,
                    "transport cleanup retry reached terminal failure",
                );
                self.force_transport_cleanup_owner_drop(operation, transport_adapter)
                    .await;
            }
        }
    }

    /// Asks the transport boundary to release the owner of an unrecovered media
    /// cleanup operation.
    ///
    /// This is a last-resort cleanup path. It only applies to media removal
    /// failures because closing a user cleanup operation cannot be made stronger
    /// by closing the same user again. The room records the escalation even when
    /// the adapter refuses the close so operators can correlate unrecovered
    /// cleanup with worker or shard level state.
    async fn force_transport_cleanup_owner_drop(
        &self,
        operation: &TransportCleanupOperation,
        transport_adapter: &MediaTransport,
    ) {
        if !operation.is_media_removal() {
            return;
        }
        let close_result = transport_adapter
            .close_session(&self.transport_user_key(operation.user_id(), operation.connection_id()))
            .await;
        warn!(
            user_id = ?operation.user_id(),
            connection_id = ?operation.connection_id(),
            transport_media_id = ?operation.transport_media_id(),
            "transport cleanup requires owning worker or shard resource drop"
        );
        if close_result.is_err() {
            warn!(
                user_id = ?operation.user_id(),
                connection_id = ?operation.connection_id(),
                transport_media_id = ?operation.transport_media_id(),
                "transport cleanup owner drop failed"
            );
        }
    }

    /// Abandons pending cleanup retries when the room leaves the directory.
    ///
    /// Once the manager removes the room, there is no authoritative owner left
    /// to keep retrying transport effects. Each abandoned operation is reported
    /// as a shutdown cleanup failure so the telemetry surface can distinguish
    /// expected room removal from retry exhaustion.
    pub(in crate::runtime::room) fn abandon_cleanup_retries_for_shutdown(&self) {
        let abandoned = self.cleanup_reconciler().abandon_pending();
        for _ in 0..abandoned {
            self.metrics
                .record_transport_cleanup_failure(TransportCleanupFailureKind::Shutdown);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) async fn force_cleanup_retry_cycle_for_test(
        &self,
        transport_adapter: &MediaTransport,
    ) {
        self.cleanup_reconciler().force_due_for_test();
        self.reconcile_transport_cleanup_retries(transport_adapter)
            .await;
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) fn pending_cleanup_retry_count_for_test(&self) -> usize {
        self.cleanup_reconciler().pending_count()
    }

    fn cleanup_reconciler(&self) -> MutexGuard<'_, CleanupReconciler> {
        self.cleanup_reconciler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    async fn run_session_transition(
        &self,
        transition: UserTransition<'_>,
        cleanup: UserCleanup<'_>,
    ) -> Result<UserTransitionResult, RoomJoinError> {
        let Some(outcome) = self.apply_state_transition(transition).await? else {
            return Ok(UserTransitionResult::Missing);
        };
        Ok(self.finalize_session_transition(outcome, cleanup).await)
    }

    async fn apply_state_transition(
        &self,
        transition: UserTransition<'_>,
    ) -> Result<Option<UserTransitionOutcome>, RoomJoinError> {
        let outcome = match transition {
            UserTransition::Join {
                user_id,
                label,
                permissions,
                sender,
                emit_joined_fanout,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_join(user_id, label, permissions, sender, emit_joined_fanout)?
                };
                UserTransitionOutcome::Join(outcome)
            }
            UserTransition::Close {
                user_id,
                connection_id,
            } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_leave(user_id, connection_id)
                };
                UserTransitionOutcome::Close {
                    outcome,
                    user_id: user_id.clone(),
                    connection_id,
                }
            }
            UserTransition::Disconnect { user_ids } => {
                let outcome = {
                    let mut state = self.state.write().await;
                    state.apply_disconnect_users(user_ids)
                };
                UserTransitionOutcome::Disconnect(outcome)
            }
        };
        Ok(Some(outcome))
    }

    async fn finalize_session_transition(
        &self,
        outcome: UserTransitionOutcome,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        match outcome {
            UserTransitionOutcome::Join(outcome) => {
                self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                    .await;
                if let Some(transport_adapter) = cleanup.transport_adapter() {
                    self.sync_source_packet_selection_policy(
                        Some(transport_adapter),
                        transport_adapter,
                    )
                    .await;
                }
                let connection_id = outcome.connection_id;
                let user_id = outcome.user_id.clone();
                Self::emit_lifecycle_effects(outcome.effects);
                self.diagnostics.register_user(self.uuid(), &user_id);
                self.diagnostics.record(
                    DiagnosticsEventData::for_user(
                        self.uuid(),
                        &user_id,
                        telemetry_event::USER_JOINED,
                    )
                    .with_connection_id(connection_id.as_u64())
                    .with_media_worker_id(self.media_worker_id()),
                );
                UserTransitionResult::Joined(connection_id)
            }
            UserTransitionOutcome::Close {
                outcome,
                user_id,
                connection_id,
            } => {
                let had_state = outcome.is_some();
                if let Some(outcome) = outcome {
                    self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                        .await;
                    Self::emit_lifecycle_effects(outcome.effects);
                }
                self.close_transport_user_for_cleanup(&user_id, connection_id, cleanup)
                    .await;
                if had_state {
                    self.diagnostics.record(
                        DiagnosticsEventData::for_user(
                            self.uuid(),
                            &user_id,
                            telemetry_event::USER_CLOSED,
                        )
                        .with_connection_id(connection_id.as_u64())
                        .with_media_worker_id(self.media_worker_id()),
                    );
                    self.diagnostics.forget_user(self.uuid(), &user_id);
                    if let Some(transport_adapter) = cleanup.transport_adapter() {
                        self.sync_source_packet_selection_policy(
                            Some(transport_adapter),
                            transport_adapter,
                        )
                        .await;
                    }
                }
                UserTransitionResult::Applied
            }
            UserTransitionOutcome::Disconnect(outcome) => {
                self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                    .await;
                for disconnected_session in &outcome.disconnected_users {
                    self.close_transport_user_for_cleanup(
                        &disconnected_session.user_id,
                        disconnected_session.connection_id,
                        cleanup,
                    )
                    .await;
                }
                for disconnected_session in &outcome.disconnected_users {
                    self.diagnostics.record(
                        DiagnosticsEventData::for_user(
                            self.uuid(),
                            &disconnected_session.user_id,
                            telemetry_event::USER_DISCONNECTED,
                        )
                        .with_media_worker_id(self.media_worker_id()),
                    );
                    self.diagnostics
                        .forget_user(self.uuid(), &disconnected_session.user_id);
                }
                if let Some(transport_adapter) = cleanup.transport_adapter() {
                    self.sync_source_packet_selection_policy(
                        Some(transport_adapter),
                        transport_adapter,
                    )
                    .await;
                }
                Self::emit_lifecycle_effects(outcome.effects);
                UserTransitionResult::Applied
            }
        }
    }

    pub(super) fn emit_lifecycle_effects(effects: LifecycleEffects) {
        for close_request in effects.close_requests {
            let _ = close_request
                .sender
                .send(UserOutbound::Close(close_request.reason));
        }
        for fanout in effects.fanouts {
            fanout.emit();
        }
    }

    /// Commit the answer-derived negotiated capability set for one live
    /// connection and trigger any consumer bootstrap that depends on it.
    pub async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &impl MediaPort,
    ) -> SessionNegotiationOutcome {
        let update = {
            let mut state = self.state.write().await;
            state.set_user_negotiated(user_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update(user_id, connection_id, update, media_port)
            .await
    }

    async fn apply_negotiation_update(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        update: UserNegotiationUpdate,
        media_port: &impl MediaPort,
    ) -> SessionNegotiationOutcome {
        if !update.session_present {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if update.became_consumer_ready {
            if !self
                .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_port)
                .await
            {
                return SessionNegotiationOutcome::StaleConnection;
            }
            self.request_active_video_consumer_keyframes(user_id, connection_id, media_port)
                .await;
        }
        SessionNegotiationOutcome::Applied
    }

    pub(crate) async fn apply_session_refreshed(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) -> SessionNegotiationOutcome {
        if !self
            .request_active_video_consumer_keyframes(user_id, connection_id, media_port)
            .await
        {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if !self
            .bootstrap_missing_consumers_for_connection(user_id, connection_id, media_port)
            .await
        {
            return SessionNegotiationOutcome::StaleConnection;
        }
        SessionNegotiationOutcome::Applied
    }

    async fn request_active_video_consumer_keyframes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) -> bool {
        let Some(keyframe_refresh_targets) = ({
            let state = self.state.read().await;
            state.active_video_consumer_keyframe_refresh_targets(user_id, connection_id)
        }) else {
            return false;
        };
        for target in keyframe_refresh_targets {
            if media_port
                .request_consumer_keyframe(
                    &self.transport_user_key(user_id, connection_id),
                    target.consumer_media(),
                    &self.transport_user_key(
                        target.producer_user_id(),
                        target.producer_connection_id(),
                    ),
                    target.source_media(),
                )
                .await
                .is_err()
            {
                warn!(
                    ?user_id,
                    connection_id = ?connection_id,
                    producer_user_id = ?target.producer_user_id(),
                    source_transport_media_id = ?target.source_media(),
                    "transport adapter failed to request a refreshed consumer keyframe"
                );
            }
        }
        true
    }

    pub(super) async fn user_count(&self) -> usize {
        self.state.read().await.user_count()
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
