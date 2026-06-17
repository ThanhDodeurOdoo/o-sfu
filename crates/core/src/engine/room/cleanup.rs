//! Room-side reconciliation for transport cleanup effects.
//!
//! Room state is authoritative for membership and media attachments. Transport
//! cleanup is an async effect that runs after that state has already moved on.
//! This module contains the small amount of retry state needed when the effect
//! fails after the room can no longer derive it from `RoomState`.
//!
//! Cleanup operations run outside room-state locks. Recoverable failures enter
//! the bounded room retry queue. Terminal failures, retry exhaustion, queue
//! pressure and room shutdown are recorded as explicit cleanup failures.
//!
//! # Invariants
//!
//! Each pending operation is keyed by the runtime-local transport identity that
//! the adapter understands. A media cleanup retry is keyed by the resolved
//! transport session key plus transport media id. A user close retry is keyed
//! by the resolved transport session key. A relay release retry is keyed by its
//! resolved source session key plus route.
//!
//! The queue is bounded because cleanup recovery is a cold-path safety net, not
//! an unbounded task system. If the queue fills, room cleanup must
//! escalate through metrics and worker-level cleanup instead of
//! retaining more state here.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::MutexGuard,
};

use tracing::warn;

use super::{Room, media_graph::RelayRouteKey};
use crate::{
    TransportEffectOutcome,
    engine::{
        ConnectionId, UserId,
        media_transport::{
            MediaTransport, TransportAdapterError, TransportMediaId, TransportRelayRouteAction,
            TransportRelayRouteEffect, TransportSessionKey, TransportSourceKey,
        },
        metrics::TransportCleanupFailureKind::{
            self, QueueFull, RetryExhausted, Shutdown, Terminal,
        },
        sync::lock_unpoisoned,
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

pub(super) const CLEANUP_RETRY_CAPACITY: usize = 128;

pub(super) const CLEANUP_MAX_RETRIES: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransportCleanupOperation {
    RemoveMedia {
        session_key: TransportSessionKey,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    },
    CloseUser {
        session_key: TransportSessionKey,
        connection_id: ConnectionId,
    },
    ReleaseRelayRoute {
        source_session_key: TransportSessionKey,
        route: RelayRouteKey,
    },
}

impl TransportCleanupOperation {
    #[must_use]
    pub(super) fn user_id(&self) -> &UserId {
        match self {
            Self::RemoveMedia { session_key, .. } | Self::CloseUser { session_key, .. } => {
                session_key.user_id()
            }
            Self::ReleaseRelayRoute { route, .. } => &route.source_user,
        }
    }

    #[must_use]
    pub(super) const fn connection_id(&self) -> ConnectionId {
        match self {
            Self::RemoveMedia { connection_id, .. } | Self::CloseUser { connection_id, .. } => {
                *connection_id
            }
            Self::ReleaseRelayRoute { route, .. } => route.source_connection,
        }
    }

    #[must_use]
    pub(super) const fn transport_media_id(&self) -> Option<TransportMediaId> {
        match self {
            Self::RemoveMedia {
                transport_media_id, ..
            } => Some(*transport_media_id),
            Self::ReleaseRelayRoute { route, .. } => Some(route.source_media),
            Self::CloseUser { .. } => None,
        }
    }

    #[must_use]
    pub(super) const fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::RemoveMedia { session_key, .. }
            | Self::CloseUser { session_key, .. }
            | Self::ReleaseRelayRoute {
                source_session_key: session_key,
                ..
            } => session_key,
        }
    }

    #[must_use]
    pub(super) const fn needs_owner_drop(&self) -> bool {
        matches!(
            self,
            Self::RemoveMedia { .. } | Self::ReleaseRelayRoute { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupFailureAction {
    RetryQueued,
    RetryAlreadyQueued,
    QueueFull,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupRetryAction {
    Succeeded,
    Requeued,
    Exhausted,
    Terminal,
}

#[derive(Debug, Default)]
pub(super) struct CleanupReconciler {
    pending: BTreeMap<TransportCleanupOperation, PendingCleanupRetry>,
}

#[derive(Debug, Clone, Copy)]
struct PendingCleanupRetry {
    attempts: u8,
    wait_cycles: u8,
}

impl CleanupReconciler {
    pub(super) fn record_failure(
        &mut self,
        operation: TransportCleanupOperation,
        error: TransportAdapterError,
    ) -> CleanupFailureAction {
        if cleanup_error_is_terminal(error) {
            return CleanupFailureAction::Terminal;
        }
        let pending_is_full = self.pending.len() >= CLEANUP_RETRY_CAPACITY;
        match self.pending.entry(operation) {
            Entry::Occupied(_) => CleanupFailureAction::RetryAlreadyQueued,
            Entry::Vacant(_) if pending_is_full => CleanupFailureAction::QueueFull,
            Entry::Vacant(entry) => {
                entry.insert(PendingCleanupRetry {
                    attempts: 0,
                    wait_cycles: 0,
                });
                CleanupFailureAction::RetryQueued
            }
        }
    }

    pub(super) fn due_retries(&mut self) -> Vec<TransportCleanupOperation> {
        let mut due = Vec::new();
        for (operation, retry) in &mut self.pending {
            if retry.wait_cycles == 0 {
                due.push(operation.clone());
            } else {
                retry.wait_cycles = retry.wait_cycles.saturating_sub(1);
            }
        }
        due
    }

    pub(super) fn record_retry_result(
        &mut self,
        operation: &TransportCleanupOperation,
        result: Result<(), TransportAdapterError>,
    ) -> CleanupRetryAction {
        match result {
            Ok(()) => {
                self.pending.remove(operation);
                CleanupRetryAction::Succeeded
            }
            Err(error) if cleanup_error_is_terminal(error) => {
                self.pending.remove(operation);
                CleanupRetryAction::Terminal
            }
            Err(_error) => {
                let Some(retry) = self.pending.get_mut(operation) else {
                    return CleanupRetryAction::Terminal;
                };
                retry.attempts = retry.attempts.saturating_add(1);
                if retry.attempts >= CLEANUP_MAX_RETRIES {
                    self.pending.remove(operation);
                    return CleanupRetryAction::Exhausted;
                }
                retry.wait_cycles = retry.attempts;
                CleanupRetryAction::Requeued
            }
        }
    }

    pub(super) fn abandon_pending(&mut self) -> usize {
        let abandoned = self.pending.len();
        self.pending.clear();
        abandoned
    }
}

const fn cleanup_error_is_terminal(error: TransportAdapterError) -> bool {
    matches!(
        error,
        TransportAdapterError::InvalidInput | TransportAdapterError::UnsupportedFeature
    )
}

impl Room {
    pub async fn execute_transport_cleanup_operations(
        &self,
        media_transport: &MediaTransport,
        operations: &[TransportCleanupOperation],
    ) -> TransportEffectOutcome {
        let mut cleanup = TransportEffectOutcome::Applied;
        for operation in operations {
            if let Err(error) = self
                .execute_transport_cleanup_operation(operation, media_transport)
                .await
            {
                self.record_cleanup_failure(operation, error, media_transport)
                    .await;
                cleanup = TransportEffectOutcome::Failed;
            }
        }
        self.reconcile_transport_cleanup_retries(media_transport)
            .await;
        cleanup
    }

    async fn execute_transport_cleanup_operation(
        &self,
        operation: &TransportCleanupOperation,
        media_transport: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        match operation {
            TransportCleanupOperation::RemoveMedia {
                session_key,
                transport_media_id,
                ..
            } => {
                media_transport
                    .remove_media(session_key, *transport_media_id)
                    .await
            }
            TransportCleanupOperation::CloseUser { session_key, .. } => {
                media_transport.close_session(session_key).await
            }
            TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key,
                route,
            } => {
                let effect = TransportRelayRouteEffect {
                    source: TransportSourceKey::new(source_session_key.clone(), route.source_media),
                    target_media_worker_id: route.target_worker,
                    action: TransportRelayRouteAction::Release,
                };
                media_transport.apply_relay_route_effect(&effect).await
            }
        }
    }

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
                self.escalate_cleanup(operation, QueueFull, media_transport)
                    .await;
            }
            CleanupFailureAction::Terminal => {
                self.escalate_cleanup(operation, Terminal, media_transport)
                    .await;
            }
        }
    }

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

    pub async fn drain_cleanup_retries(&self, media_transport: &MediaTransport) {
        self.reconcile_transport_cleanup_retries(media_transport)
            .await;
    }

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
                self.escalate_cleanup(operation, RetryExhausted, media_transport)
                    .await;
            }
            CleanupRetryAction::Terminal => {
                self.escalate_cleanup(operation, Terminal, media_transport)
                    .await;
            }
        }
    }

    async fn escalate_cleanup(
        &self,
        operation: &TransportCleanupOperation,
        failure_kind: TransportCleanupFailureKind,
        media_transport: &MediaTransport,
    ) {
        self.metrics.record_transport_cleanup_failure(failure_kind);
        let message = match failure_kind {
            Terminal => "transport cleanup reached terminal failure",
            RetryExhausted => "transport cleanup retry attempts were exhausted",
            QueueFull => "transport cleanup retry queue is full",
            Shutdown => "transport cleanup was abandoned during shutdown",
        };
        warn_transport_cleanup_failure(operation, message);
        self.force_transport_cleanup_owner_drop(operation, media_transport)
            .await;
    }

    async fn force_transport_cleanup_owner_drop(
        &self,
        operation: &TransportCleanupOperation,
        media_transport: &MediaTransport,
    ) {
        if !operation.needs_owner_drop() {
            return;
        }
        let close_result = media_transport.close_session(operation.session_key()).await;
        warn!(
            user_id = ?operation.user_id(),
            connection_id = ?operation.connection_id(),
            transport_media_id = ?operation.transport_media_id(),
                "transport cleanup requires worker resource drop"
        );
        if close_result.is_err() {
            warn!(
                user_id = ?operation.user_id(),
                connection_id = ?operation.connection_id(),
                transport_media_id = ?operation.transport_media_id(),
                "transport cleanup transport-user fallback failed"
            );
        }
    }

    pub fn abandon_cleanup_retries_for_shutdown(&self) {
        let abandoned = self.cleanup_reconciler().abandon_pending();
        for _ in 0..abandoned {
            self.metrics.record_transport_cleanup_failure(Shutdown);
        }
    }

    pub fn has_pending_cleanup_retries(&self) -> bool {
        !self.cleanup_reconciler().pending.is_empty()
    }

    fn cleanup_reconciler(&self) -> MutexGuard<'_, CleanupReconciler> {
        lock_unpoisoned(&self.cleanup_reconciler)
    }
}
