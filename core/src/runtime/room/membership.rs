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
        media_transport::{
            MediaPort, MediaTransport, ObservabilityPort, SessionPort, TransportAdapterError,
        },
        metrics::TransportCleanupFailureKind,
        telemetry::schema::event as telemetry_event,
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
    media_transport: Option<&'a MediaTransport>,
    clean_transport_state: bool,
}

impl<'a> UserCleanup<'a> {
    pub(in crate::runtime::room) const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media_transport: Some(media_transport),
            clean_transport_state: true,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::room) const fn state_only(
        media_transport: Option<&'a MediaTransport>,
    ) -> Self {
        Self {
            media_transport,
            clean_transport_state: false,
        }
    }

    pub(in crate::runtime::room) const fn media_transport(self) -> Option<&'a MediaTransport> {
        self.media_transport
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
        media_transport: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.join_session_with_cleanup(
            user_id,
            label,
            permissions,
            sender,
            UserCleanup::runtime(media_transport),
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
        media_transport: &MediaTransport,
    ) -> bool {
        self.remove_user_with_cleanup(
            user_id,
            connection_id,
            UserCleanup::runtime(media_transport),
        )
        .await
    }

    pub(in crate::runtime::room) async fn remove_user_with_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) -> bool {
        self.run_session_transition(
            UserTransition::Close {
                user_id,
                connection_id,
            },
            cleanup,
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

    pub(crate) async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        refresh: UserInfoRefresh,
        media_transport: &(impl MediaPort + ObservabilityPort),
    ) {
        let need_refresh = refresh.is_needed();
        let outcome = {
            let mut state = self.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
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

    pub(crate) async fn disconnect_users(
        &self,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        self.disconnect_users_with_cleanup(user_ids, UserCleanup::runtime(media_transport))
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
        let Some(media_transport) = cleanup.media_transport() else {
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
                .execute_transport_cleanup_operation(&operation, media_transport)
                .await
            {
                self.record_cleanup_failure(&operation, error, media_transport)
                    .await;
            }
        }
        self.reconcile_transport_cleanup_retries(media_transport)
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
        let Some(media_transport) = cleanup.media_transport() else {
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
            .execute_transport_cleanup_operation(&operation, media_transport)
            .await
        {
            self.record_cleanup_failure(&operation, error, media_transport)
                .await;
        }
        self.reconcile_transport_cleanup_retries(media_transport)
            .await;
    }

    /// Executes one cleanup operation against the media transport.
    ///
    /// This method intentionally contains no retry or metric logic. Keeping the
    /// transport effect separate from reconciliation makes it clear that room
    /// cleanup state is updated synchronously, then async adapter work is
    /// attempted after the relevant room lock has been released.
    async fn execute_transport_cleanup_operation(
        &self,
        operation: &TransportCleanupOperation,
        media_transport: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        match operation {
            TransportCleanupOperation::RemoveMedia {
                user_id,
                connection_id,
                transport_media_id,
            } => {
                media_transport
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
                media_transport
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
    /// cleanup failures ask the media transport to drop the owning user so a
    /// lower layer can release resources that the room can no longer address
    /// precisely.
    async fn record_cleanup_failure(
        &self,
        operation: &TransportCleanupOperation,
        error: TransportAdapterError,
        media_transport: &MediaTransport,
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
                self.force_transport_cleanup_owner_drop(operation, media_transport)
                    .await;
            }
            CleanupFailureAction::Terminal => {
                self.metrics
                    .record_transport_cleanup_failure(TransportCleanupFailureKind::Terminal);
                warn_transport_cleanup_failure(
                    operation,
                    "transport cleanup reached terminal failure",
                );
                self.force_transport_cleanup_owner_drop(operation, media_transport)
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
    async fn reconcile_transport_cleanup_retries(&self, media_transport: &MediaTransport) {
        loop {
            let retries = self.cleanup_reconciler().due_retries();
            if retries.is_empty() {
                return;
            }
            for operation in retries {
                let result = self
                    .execute_transport_cleanup_operation(&operation, media_transport)
                    .await;
                let action = self
                    .cleanup_reconciler()
                    .record_retry_result(&operation, result);
                self.record_cleanup_retry_action(&operation, action, media_transport)
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
        media_transport: &MediaTransport,
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
                self.force_transport_cleanup_owner_drop(operation, media_transport)
                    .await;
            }
            CleanupRetryAction::Terminal => {
                self.metrics
                    .record_transport_cleanup_failure(TransportCleanupFailureKind::Terminal);
                warn_transport_cleanup_failure(
                    operation,
                    "transport cleanup retry reached terminal failure",
                );
                self.force_transport_cleanup_owner_drop(operation, media_transport)
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
        media_transport: &MediaTransport,
    ) {
        if !operation.is_media_removal() {
            return;
        }
        let close_result = media_transport
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
        media_transport: &MediaTransport,
    ) {
        self.cleanup_reconciler().force_due_for_test();
        self.reconcile_transport_cleanup_retries(media_transport)
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
                self.finalize_join_transition(outcome, cleanup).await
            }
            UserTransitionOutcome::Close {
                outcome,
                user_id,
                connection_id,
            } => {
                self.finalize_close_transition(outcome, user_id, connection_id, cleanup)
                    .await
            }
            UserTransitionOutcome::Disconnect(outcome) => {
                self.finalize_disconnect_transition(outcome, cleanup).await
            }
        }
    }

    async fn finalize_join_transition(
        &self,
        outcome: JoinUserOutcome,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let connection_id = outcome.connection_id;
        self.definition
            .register_transport_worker(connection_id, outcome.transport_media_worker_id);
        self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
            .await;
        if let Some(media_transport) = cleanup.media_transport() {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
        }
        let user_id = outcome.user_id.clone();
        Self::emit_lifecycle_effects(outcome.effects);
        self.diagnostics.register_user(self.uuid(), &user_id);
        self.diagnostics.record(
            DiagnosticsEventData::for_user(self.uuid(), &user_id, telemetry_event::USER_JOINED)
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(outcome.transport_media_worker_id),
        );
        UserTransitionResult::Joined(connection_id)
    }

    async fn finalize_close_transition(
        &self,
        outcome: Option<LeaveUserOutcome>,
        user_id: UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
        let had_state = outcome.is_some();
        let media_worker_id = self
            .definition
            .media_worker_id_for_connection(connection_id);
        if let Some(outcome) = outcome {
            self.cleanup_transport_removals(cleanup, &outcome.transport_removals)
                .await;
            Self::emit_lifecycle_effects(outcome.effects);
        }
        self.close_transport_user_for_cleanup(&user_id, connection_id, cleanup)
            .await;
        if had_state {
            self.record_closed_user(&user_id, connection_id, media_worker_id, cleanup)
                .await;
        }
        self.definition.unregister_transport_worker(connection_id);
        UserTransitionResult::Applied
    }

    async fn record_closed_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_worker_id: usize,
        cleanup: UserCleanup<'_>,
    ) {
        self.diagnostics.record(
            DiagnosticsEventData::for_user(self.uuid(), user_id, telemetry_event::USER_CLOSED)
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(media_worker_id),
        );
        self.diagnostics.forget_user(self.uuid(), user_id);
        if let Some(media_transport) = cleanup.media_transport() {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
        }
    }

    async fn finalize_disconnect_transition(
        &self,
        outcome: DisconnectUsersOutcome,
        cleanup: UserCleanup<'_>,
    ) -> UserTransitionResult {
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
            self.record_disconnected_user(
                &disconnected_session.user_id,
                disconnected_session.connection_id,
            );
        }
        if let Some(media_transport) = cleanup.media_transport() {
            self.sync_source_packet_selection_policy(Some(media_transport), media_transport)
                .await;
        }
        Self::emit_lifecycle_effects(outcome.effects);
        UserTransitionResult::Applied
    }

    fn record_disconnected_user(&self, user_id: &UserId, connection_id: ConnectionId) {
        let media_worker_id = self
            .definition
            .media_worker_id_for_connection(connection_id);
        self.diagnostics.record(
            DiagnosticsEventData::for_user(
                self.uuid(),
                user_id,
                telemetry_event::USER_DISCONNECTED,
            )
            .with_media_worker_id(media_worker_id),
        );
        self.diagnostics.forget_user(self.uuid(), user_id);
        self.definition.unregister_transport_worker(connection_id);
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
                    "media transport failed to request a refreshed consumer keyframe"
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
