//! Room-owned reconciliation for transport cleanup effects.
//!
//! # Boundary role
//!
//! Room state is authoritative for membership and media ownership. Transport
//! cleanup is an async effect that runs after that state has already moved on.
//! This module owns the small amount of retry state needed when the effect
//! fails after the room can no longer derive it from `RoomState`.
//!
//! The reconciler deliberately does not call the transport adapter, mutate room
//! state or decide when a room is removed. It only classifies cleanup failures,
//! deduplicates pending operations and tells room orchestration which effects
//! are due for another attempt.
//!
//! # Lifecycle graph
//!
//! ```text
//! room state transition commits
//!             |
//!             v
//! cleanup intent captured from the transition
//!             |
//!             v
//! transport cleanup effect runs outside room locks
//!             |
//!      +------+------+
//!      |             |
//!      v             v
//!   success      failure
//!      |             |
//!      v             v
//!   done       classify failure
//!                    |
//!     +--------------+--------------+--------------+
//!     |                             |              |
//!     v                             v              v
//! recoverable                 terminal       retry queue full
//!     |                             |              |
//!     v                             v              v
//! queue or reuse retry      record failure   record failure
//!     |                             |              |
//!     v                             v              v
//! due retry cycle           media owner drop when operation is media
//!     |
//!     v
//! transport cleanup retry
//!     |
//!     +----------+----------+----------+
//!     |          |          |          |
//!     v          v          v          v
//! success   recoverable terminal  retries exhausted
//!     |          |          |          |
//!     v          v          v          v
//! remove    backoff    record failure record failure
//! pending   by cycle        |          |
//!                |          v          v
//!                +----> media owner drop when operation is media
//!
//! room shutdown removes the remaining pending entries and records each one as
//! an abandoned cleanup failure.
//! ```
//!
//! # Invariants
//!
//! Each pending operation is keyed by the runtime-local transport identity that
//! the adapter understands. A media cleanup retry is keyed by user id,
//! connection id and transport media id. A user close retry is keyed by user id
//! and connection id.
//!
//! The queue is bounded because cleanup recovery is a cold-path safety net, not
//! an unbounded task system. If the queue fills, room orchestration must
//! escalate through metrics and worker or shard level cleanup instead of
//! retaining more state here.

use std::collections::{BTreeMap, btree_map::Entry};

use crate::runtime::{
    ConnectionId, UserId,
    transport_adapter::{TransportAdapterError, TransportMediaId},
};

/// Maximum number of distinct cleanup operations one room may retain.
///
/// Cleanup failures are rare cold-path work. A fixed cap prevents a broken
/// transport boundary from turning room teardown into unbounded memory growth.
pub(super) const CLEANUP_RETRY_CAPACITY: usize = 128;

/// Number of failed retry attempts before room-local recovery gives up.
///
/// The first transport failure inserts the operation. Only later attempts made
/// by the reconciler count toward this limit.
pub(super) const CLEANUP_MAX_RETRIES: u8 = 3;

/// A transport-side cleanup effect whose room state has already been removed.
///
/// The room uses this enum as the stable retry key after membership or media
/// state has been committed. Keeping the operation explicit avoids rebuilding
/// cleanup intent from current room state, which may already forget the user,
/// publication or subscription that triggered teardown.
///
/// # Ownership split
///
/// The operation stores the identity needed by `RuntimeTransportAdapter`. It
/// does not own transport resources and it does not prove that those resources
/// still exist. A retry that finds the resource gone is treated by the adapter
/// contract, then converted into a retry action by `CleanupReconciler`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum TransportCleanupOperation {
    /// Remove one media object from an already detached transport user.
    ///
    /// This is used after room media state has forgotten the publication or
    /// subscription. If it cannot be recovered, room orchestration escalates by
    /// closing the owning transport user so the worker or shard can release
    /// anything still attached to that session.
    RemoveMedia {
        user_id: UserId,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    },
    /// Close the whole transport user after room membership has moved on.
    ///
    /// This is the final cleanup operation for user teardown. It is retried
    /// when the adapter is temporarily unavailable, then abandoned explicitly
    /// if the room itself is removed before recovery completes.
    CloseUser {
        user_id: UserId,
        connection_id: ConnectionId,
    },
}

impl TransportCleanupOperation {
    #[must_use]
    pub(super) const fn user_id(&self) -> &UserId {
        match self {
            Self::RemoveMedia { user_id, .. } | Self::CloseUser { user_id, .. } => user_id,
        }
    }

    #[must_use]
    pub(super) const fn connection_id(&self) -> ConnectionId {
        match self {
            Self::RemoveMedia { connection_id, .. } | Self::CloseUser { connection_id, .. } => {
                *connection_id
            }
        }
    }

    #[must_use]
    pub(super) const fn transport_media_id(&self) -> Option<TransportMediaId> {
        match self {
            Self::RemoveMedia {
                transport_media_id, ..
            } => Some(*transport_media_id),
            Self::CloseUser { .. } => None,
        }
    }

    #[must_use]
    pub(super) const fn is_media_removal(&self) -> bool {
        matches!(self, Self::RemoveMedia { .. })
    }
}

/// Outcome returned when a fresh cleanup failure is classified.
///
/// Room orchestration records metrics and decides escalation from this value.
/// The reconciler only answers whether retry state was created, was already
/// present, could not be stored or should not be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupFailureAction {
    /// The operation was inserted and is due for a retry on the next
    /// reconciliation pass.
    RetryQueued,
    /// The same operation was already pending, so no new queue entry or retry
    /// metric should be created for this failure.
    RetryAlreadyQueued,
    /// The bounded retry queue is full and the caller must escalate without
    /// storing more room-local state.
    QueueFull,
    /// The adapter reported a deterministic contract failure that another
    /// retry cannot fix.
    Terminal,
}

/// Outcome returned after a queued cleanup operation is attempted again.
///
/// This separates retry bookkeeping from transport side effects. The caller
/// remains responsible for metrics, logging and terminal escalation because
/// those actions need room context and transport adapter access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupRetryAction {
    /// The adapter accepted the cleanup and the pending entry was removed.
    Succeeded,
    /// The cleanup still looks recoverable and remains queued for a later pass.
    Requeued,
    /// The recoverable failure repeated too many times for room-local retry to
    /// keep owning the operation.
    Exhausted,
    /// The retry encountered a deterministic contract failure and was removed.
    Terminal,
}

/// Bounded retry state for failed room cleanup effects.
///
/// # What belongs here
///
/// This type owns only retry metadata: the operation key, the number of failed
/// retry attempts and the number of cleanup cycles to wait before trying again.
/// It does not hold room locks, spawn tasks or call async code.
///
/// # Concurrency model
///
/// `Room` protects the reconciler with a standard mutex because all methods are
/// short synchronous bookkeeping steps. Callers must drop that guard before
/// awaiting transport adapter work. Holding the guard across `.await` would
/// couple room cleanup recovery to adapter latency and could block unrelated
/// cold-path teardown work.
#[derive(Debug, Default)]
pub(super) struct CleanupReconciler {
    /// Pending operations keyed by their transport cleanup identity.
    ///
    /// A map is used instead of a queue so the same cleanup effect cannot be
    /// enqueued repeatedly while retries are already in flight.
    pending: BTreeMap<TransportCleanupOperation, PendingCleanupRetry>,
}

/// Retry state for one pending cleanup operation.
///
/// The delay is expressed in cleanup reconciliation cycles, not wall-clock
/// time. That keeps recovery tied to room teardown activity and avoids adding a
/// timer task to the room domain.
#[derive(Debug, Clone, Copy)]
struct PendingCleanupRetry {
    /// Failed retry attempts after the original cleanup failure.
    attempts: u8,
    /// Reconciliation cycles to skip before the next retry is due.
    wait_cycles: u8,
}

impl CleanupReconciler {
    /// Classifies a newly failed cleanup effect and stores retry state when the
    /// failure is recoverable.
    ///
    /// `TransportUnavailable` is treated as recoverable because it can mean the
    /// adapter, worker or shard boundary is temporarily not ready. Invalid input
    /// and unsupported feature errors are terminal because they mean room
    /// orchestration asked for an operation the transport boundary cannot
    /// perform.
    pub(super) fn record_failure(
        &mut self,
        operation: TransportCleanupOperation,
        error: TransportAdapterError,
    ) -> CleanupFailureAction {
        if cleanup_error_is_terminal(error) {
            return CleanupFailureAction::Terminal;
        }
        if self.pending.len() >= CLEANUP_RETRY_CAPACITY && !self.pending.contains_key(&operation) {
            return CleanupFailureAction::QueueFull;
        }
        match self.pending.entry(operation) {
            Entry::Vacant(entry) => {
                entry.insert(PendingCleanupRetry {
                    attempts: 0,
                    wait_cycles: 0,
                });
                CleanupFailureAction::RetryQueued
            }
            Entry::Occupied(_) => CleanupFailureAction::RetryAlreadyQueued,
        }
    }

    /// Returns operations that should be retried during the current cleanup
    /// reconciliation cycle.
    ///
    /// The first retry is due immediately so transient adapter gaps are closed
    /// before teardown returns to its caller. Later failures wait a small number
    /// of cleanup cycles. This keeps recovery deterministic without introducing
    /// a timer wheel or background task inside the room domain.
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

    /// Applies the result of a retry attempt and returns the next orchestration
    /// action.
    ///
    /// The retry counter is advanced only after a queued retry fails. The
    /// original failure that inserted the operation is not counted as a retry
    /// attempt, which keeps metrics aligned with actual retry work.
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

    /// Drops all pending retry state because the owning room is leaving the
    /// directory.
    ///
    /// The returned count lets the caller publish one shutdown failure metric
    /// per abandoned operation. Once a room is removed there is no authoritative
    /// owner left that can safely retry transport cleanup later.
    pub(super) fn abandon_pending(&mut self) -> usize {
        let abandoned = self.pending.len();
        self.pending.clear();
        abandoned
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn force_due_for_test(&mut self) {
        for retry in self.pending.values_mut() {
            retry.wait_cycles = 0;
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Distinguishes deterministic adapter contract failures from failures that
/// may be fixed by retrying after the transport boundary catches up.
const fn cleanup_error_is_terminal(error: TransportAdapterError) -> bool {
    matches!(
        error,
        TransportAdapterError::InvalidInput | TransportAdapterError::UnsupportedFeature
    )
}
