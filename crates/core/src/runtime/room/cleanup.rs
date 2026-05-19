//! Room-owned reconciliation for transport cleanup effects.
//!
//! Room state is authoritative for membership and media ownership. Transport
//! cleanup is an async effect that runs after that state has already moved on.
//! This module contains the small amount of retry state needed when the effect
//! fails after the room can no longer derive it from `RoomState`.
//!
//! This module owns the room cleanup boundary after membership or media state
//! has already committed. It executes cleanup operations against the media
//! transport outside room-state locks, records retry state and escalates
//! unrecoverable operations through the coarse owner-drop primitive.
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
//! the adapter understands. A media cleanup retry is keyed by the resolved
//! transport session key plus transport media id. A user close retry is keyed
//! by the resolved transport session key. A relay release retry is keyed by its
//! resolved source session key plus route.
//!
//! The queue is bounded because cleanup recovery is a cold-path safety net, not
//! an unbounded task system. If the queue fills, room orchestration must
//! escalate through metrics and worker or worker level cleanup instead of
//! retaining more state here.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::MutexGuard,
};

use tracing::warn;

use super::{
    Room,
    effects::RoomTransportEffect,
    state::{RelayRouteEffect, RelayRouteKey, TransportMediaRemoval},
};
use crate::{
    TransportEffectOutcome,
    runtime::{
        ConnectionId, UserId,
        media_transport::{
            MediaTransport, TransportAdapterError, TransportMediaId, TransportRelayRouteAction,
            TransportRelayRouteEffect, TransportSessionKey,
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

/// Cleanup policy used by membership transitions after room state has moved on.
///
/// Production callers pass a media transport and allow the room to clean the
/// matching transport resources once the state transition has committed. Tests
/// and state-only callers can keep the same membership path while disabling
/// adapter cleanup.
///
/// A missing transport means only the effects that do not need the adapter can
/// run. That is useful for tests that want the authoritative room-state result
/// without faking transport ownership.
#[derive(Clone, Copy)]
pub(in crate::runtime::room) struct UserCleanup<'a> {
    /// Adapter boundary used for deferred cleanup, policy refresh and
    /// transport-adjacent session work.
    media_transport: Option<&'a MediaTransport>,
    /// Whether the caller allows this transition to mutate transport state.
    ///
    /// This is distinct from `media_transport` because tests may still need a
    /// transport handle for observation while keeping cleanup state unchanged.
    clean_transport_state: bool,
}

impl<'a> UserCleanup<'a> {
    /// Build the production cleanup policy for a runtime membership change.
    ///
    /// The room will close stale transport users and remove detached media
    /// after the corresponding `RoomState` transition has committed.
    pub const fn runtime(media_transport: &'a MediaTransport) -> Self {
        Self {
            media_transport: Some(media_transport),
            clean_transport_state: true,
        }
    }

    /// Build a cleanup policy that keeps transport state intact.
    ///
    /// This keeps tests focused on room-state lifecycle decisions while still
    /// allowing optional transport-backed observations after the transition.
    #[cfg(any(test, feature = "testing-transport"))]
    pub const fn state_only(media_transport: Option<&'a MediaTransport>) -> Self {
        Self {
            media_transport,
            clean_transport_state: false,
        }
    }

    pub const fn media_transport(self) -> Option<&'a MediaTransport> {
        self.media_transport
    }

    /// return the adapter only when this policy allows transport mutation
    ///
    /// callers use this for cleanup effects that would alter adapter state
    /// policy refresh may still use [`Self::media_transport`] because tests can
    /// observe transport-derived state while keeping cleanup disabled
    pub(in crate::runtime::room) fn cleaning_media_transport(self) -> Option<&'a MediaTransport> {
        self.media_transport.filter(|_| self.clean_transport_state)
    }
}

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

/// transport-side cleanup effect whose room state has already been removed
///
/// the room uses this enum as the stable retry key after membership or media
/// state has been committed
/// keeping the operation explicit avoids rebuilding
/// cleanup intent from current room state, which may already forget the user,
/// publication or subscription that triggered teardown
///
/// # Ownership split
///
/// the operation stores the identity needed by `MediaTransport` but does not
/// own transport resources or prove that those resources still exist
/// a retry that finds the resource gone is treated by the adapter contract,
/// then converted into a retry action by `CleanupReconciler`
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum TransportCleanupOperation {
    /// remove one media object from an already detached transport user
    ///
    /// this is used after room media state has forgotten the publication or
    /// subscription
    /// if it cannot be recovered, room orchestration escalates by closing the
    /// owning transport user so worker-level state can release anything still
    /// attached to that session
    RemoveMedia {
        session_key: TransportSessionKey,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    },
    /// close the whole transport user after room membership has moved on
    ///
    /// this is the final cleanup operation for user teardown
    /// it is retried
    /// when the adapter is temporarily unavailable, then abandoned explicitly
    /// if the room itself is removed before recovery completes
    CloseUser {
        session_key: TransportSessionKey,
        connection_id: ConnectionId,
    },
    /// release one relay route after route state has forgotten the consumer
    ///
    /// the source session key is resolved when the effect fails so retry does
    /// not depend on room state still containing the source user
    /// escalation can close the source transport user because that is the
    /// narrowest owner left when a relay mailbox refuses the release
    ReleaseRelayRoute {
        source_session_key: TransportSessionKey,
        route: RelayRouteKey,
    },
}

impl TransportCleanupOperation {
    /// return the room user that best owns diagnostics for this cleanup effect
    ///
    /// media and user cleanup use the resolved transport session key
    /// relay-route cleanup reports the source user because the source worker
    /// owns the mailbox being released
    #[must_use]
    pub(super) fn user_id(&self) -> &UserId {
        match self {
            Self::RemoveMedia { session_key, .. } | Self::CloseUser { session_key, .. } => {
                session_key.user_id()
            }
            Self::ReleaseRelayRoute { route, .. } => &route.source_user,
        }
    }

    /// return the connection used to correlate cleanup telemetry
    ///
    /// for relay releases this is the source connection because source-worker
    /// cleanup is the remaining precise owner of the route
    #[must_use]
    pub(super) const fn connection_id(&self) -> ConnectionId {
        match self {
            Self::RemoveMedia { connection_id, .. } | Self::CloseUser { connection_id, .. } => {
                *connection_id
            }
            Self::ReleaseRelayRoute { route, .. } => route.source_connection,
        }
    }

    /// return the media id involved in this cleanup when the operation has one
    ///
    /// user close has no media id because it addresses the whole transport
    /// session
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

    /// return the transport session key used by the adapter
    ///
    /// every cleanup operation is stored with the adapter-facing identity it
    /// needs for retry so finalization never has to reconstruct ownership from
    /// current room state
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

    /// report whether unrecovered cleanup can be escalated by closing an owner
    ///
    /// media removal and relay release can still leave worker state attached to
    /// a session
    /// a failed user close already targets that owner, so repeating the same
    /// close is not a stronger escalation
    #[must_use]
    pub(super) const fn needs_owner_drop(&self) -> bool {
        matches!(
            self,
            Self::RemoveMedia { .. } | Self::ReleaseRelayRoute { .. }
        )
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
/// those actions need room context and media transport access.
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
/// # Concurrency
///
/// `Room` protects the reconciler with a standard mutex because all methods are
/// short synchronous bookkeeping steps. Callers must drop that guard before
/// awaiting media transport work. Holding the guard across `.await` would
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
    /// adapter, worker or worker boundary is temporarily not ready. Invalid input
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

    #[must_use]
    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
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

impl Room {
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
    ) -> TransportEffectOutcome {
        let Some(media_transport) = cleanup.cleaning_media_transport() else {
            return TransportEffectOutcome::Applied;
        };
        self.cleanup_transport_removals_with_retry(media_transport, removals)
            .await
    }

    /// remove a batch of detached media ids through the retry reconciler
    ///
    /// the caller has already committed the room-state transition that produced
    /// these removals
    /// failures are therefore not rolled back into state
    /// recoverable failures are retained by the room so a later cleanup cycle
    /// can retry with the same resolved transport identities
    pub(in crate::runtime::room) async fn cleanup_transport_removals_with_retry(
        &self,
        media_transport: &MediaTransport,
        removals: &[TransportMediaRemoval],
    ) -> TransportEffectOutcome {
        let mut cleanup = TransportEffectOutcome::Applied;
        for removal in removals {
            let connection_id = removal.connection();
            let operation = TransportCleanupOperation::RemoveMedia {
                session_key: self.transport_user_key(removal.user(), connection_id),
                connection_id,
                transport_media_id: removal.transport_media(),
            };
            if let Err(error) = self
                .execute_transport_cleanup_operation(&operation, media_transport)
                .await
            {
                self.record_cleanup_failure(&operation, error, media_transport)
                    .await;
                cleanup = TransportEffectOutcome::Failed;
            }
        }
        self.reconcile_transport_cleanup_retries(media_transport)
            .await;
        cleanup
    }

    /// Removes one staged media reservation after room ownership was consumed.
    ///
    /// Staged rollback already decided that the reservation cannot commit.
    /// A recoverable adapter failure therefore needs the same room-owned retry
    /// owner as committed teardown cleanup.
    pub(in crate::runtime::room) async fn cleanup_transport_media_with_retry(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
        media_transport: &MediaTransport,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        let operation = TransportCleanupOperation::RemoveMedia {
            session_key: self.transport_user_key(user_id, connection_id),
            connection_id,
            transport_media_id,
        };
        match self
            .execute_transport_cleanup_operation(&operation, media_transport)
            .await
        {
            Ok(()) => TransportEffectOutcome::Applied,
            Err(error) => {
                warn!(
                    ?user_id,
                    connection_id = ?connection_id,
                    ?transport_media_id,
                    "{failure_message}"
                );
                self.record_cleanup_failure(&operation, error, media_transport)
                    .await;
                self.reconcile_transport_cleanup_retries(media_transport)
                    .await;
                TransportEffectOutcome::Failed
            }
        }
    }

    /// Closes the transport user that belonged to a room user after membership
    /// teardown has been committed.
    ///
    /// The room must not re-enter mutable state to rediscover this cleanup
    /// later, so the runtime-local user and connection identity are converted
    /// into a `TransportCleanupOperation` before the async adapter call. If the
    /// adapter is temporarily unavailable, the operation is retained by the
    /// room-local reconciler.
    pub(in crate::runtime::room) async fn close_transport_user_for_cleanup(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        cleanup: UserCleanup<'_>,
    ) {
        let Some(media_transport) = cleanup.cleaning_media_transport() else {
            return;
        };
        let operation = TransportCleanupOperation::CloseUser {
            session_key: self.transport_user_key(user_id, connection_id),
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
                session_key,
                transport_media_id,
                ..
            } => {
                RoomTransportEffect::MediaRemoval {
                    session_key: session_key.clone(),
                    transport_media_id: *transport_media_id,
                }
                .execute_unit(media_transport)
                .await
            }
            TransportCleanupOperation::CloseUser { session_key, .. } => {
                RoomTransportEffect::SessionClose {
                    session_key: session_key.clone(),
                }
                .execute_unit(media_transport)
                .await
            }
            TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key,
                route,
            } => {
                let effect = TransportRelayRouteEffect {
                    source_session_key: source_session_key.clone(),
                    source_transport_media_id: route.source_media,
                    target_media_worker_id: route.target_worker,
                    action: TransportRelayRouteAction::Release,
                };
                RoomTransportEffect::RelayRoute(effect)
                    .execute_unit(media_transport)
                    .await
            }
        }
    }

    /// convert a failed relay release into room-owned cleanup retry state
    ///
    /// relay effects are applied after route state has already been committed
    /// a failed release cannot be derived again from current topology, so the
    /// source transport identity and route key become the durable retry owner
    pub(in crate::runtime::room) async fn record_relay_route_release_failure(
        &self,
        effect: &RelayRouteEffect,
        error: TransportAdapterError,
        media_transport: &MediaTransport,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        let operation = TransportCleanupOperation::ReleaseRelayRoute {
            source_session_key: self
                .transport_user_key(&effect.route.source_user, effect.route.source_connection),
            route: effect.route.clone(),
        };
        warn!(?effect, "{failure_message}");
        self.record_cleanup_failure(&operation, error, media_transport)
            .await;
        self.reconcile_transport_cleanup_retries(media_transport)
            .await;
        TransportEffectOutcome::Failed
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
                self.escalate_cleanup(operation, QueueFull, media_transport)
                    .await;
            }
            CleanupFailureAction::Terminal => {
                self.escalate_cleanup(operation, Terminal, media_transport)
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

    /// lets runtime maintenance poll this room's due cleanup work
    ///
    /// the room keeps retry ordering and failure classification inside the
    /// reconciler. callers only decide when to poll and must not hold the room
    /// state lock while invoking this method
    ///
    /// this method does not remove the room from the manager. the manager owns
    /// directory lifetime after checking empty state and pending retry state
    /// under the lifecycle lock
    pub(in crate::runtime::room) async fn drain_cleanup_retries(
        &self,
        media_transport: &MediaTransport,
    ) {
        self.reconcile_transport_cleanup_retries(media_transport)
            .await;
    }

    /// Converts a retry outcome into metrics and final escalation.
    ///
    /// Requeued operations remain owned by the room. Exhausted or terminal media
    /// cleanup operations are escalated by closing the owning transport user.
    /// That user is the only remaining precise owner when room state has already
    /// forgotten the detached media or relay target.
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

    /// record an unrecovered cleanup failure and apply the owner-drop fallback
    ///
    /// every terminal path reaches this helper so metrics, warning text and
    /// last-resort owner cleanup cannot drift between first failure and retry
    /// exhaustion
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

    /// Asks the transport boundary to release the owner of an unrecovered
    /// cleanup operation.
    ///
    /// This is a last-resort cleanup path. It applies to media and relay target
    /// failures because closing a user cleanup operation cannot be made stronger
    /// by closing the same user again. The room records the escalation even when
    /// the adapter refuses the close so operators can correlate unrecovered
    /// cleanup with worker or worker level state.
    async fn force_transport_cleanup_owner_drop(
        &self,
        operation: &TransportCleanupOperation,
        media_transport: &MediaTransport,
    ) {
        if !operation.needs_owner_drop() {
            return;
        }
        let close_result = RoomTransportEffect::SessionClose {
            session_key: operation.session_key().clone(),
        }
        .execute_unit(media_transport)
        .await;
        warn!(
            user_id = ?operation.user_id(),
            connection_id = ?operation.connection_id(),
            transport_media_id = ?operation.transport_media_id(),
            "transport cleanup requires owning worker or worker resource drop"
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
            self.metrics.record_transport_cleanup_failure(Shutdown);
        }
    }

    pub(in crate::runtime::room) fn has_pending_cleanup_retries(&self) -> bool {
        self.cleanup_reconciler().has_pending()
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
        lock_unpoisoned(&self.cleanup_reconciler)
    }
}
