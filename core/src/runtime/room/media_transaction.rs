//! Room-owned transaction helpers for staged publish and transport cleanup.
//!
//! # role (between chanel and transport)
//!
//! This module owns the room-side unit of work for media changes that need
//! transport calls. `RoomState` stays authoritative for live producers and
//! consumers. The media transport stays authoritative for allocated media
//! lines. This file owns the short-lived (only latts for transactions)
//! bridge between those two layers so websocket publish and unpublish
//! flows do not have to remember rollback  details
//!
//!
//! # Staged publish lifecycle
//!
//! A publish is staged only after chanel state validates the current user
//! and the media transport reserves a media line While the browser answers
//! renegotiation, that reservation lives in `PendingPublishTransactions`.
//! Answer handling later drains the transaction and either commits it into
//! room state or consumes it through transport cleanup.
//!
//! # Concurrency
//!
//! This is cold-path orchestration. Transport calls happen after room state
//! locks are released. The pending-publish registry has its own mutex, but that
//! lock is held only for lookup, insertion and draining. Commit and cleanup run
//! after the registry lock is released.

use std::collections::BTreeMap;

use o_sfu_router::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::{
    Room, RoomMediaCounts,
    state::{
        ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, TransportMediaRemoval,
        ValidatedPublishDescriptor,
    },
};
use crate::{
    PublishStageOutcome, RollbackStagedPublishOutcome, TransportEffectOutcome,
    runtime::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{
            AppliedSessionAnswer, ConsumerActivity, MediaPort, ObservabilityPort, SessionPort,
            TransportAdapterError, TransportMediaId,
        },
        source_model::{SourcePublishIntent, UserStreamId},
        telemetry::schema::event as telemetry_event,
    },
};

#[cfg(test)]
mod test_support;

/// Registry for publish transactions that reserved transport media but are not
/// live in room state yet
///
/// # Invariant
///
/// At most one `(user, connection, stream_id)` entry may be staged at a
/// time. The key includes the runtime-local connection id so stale replaced
/// sockets cannot share ownership with the current websocket for the same user
/// facing user id.
///
/// This registry owns only in-flight reservations. Once a publish commits, the
/// producer and its transport media belong to chanel state Once a publish is
/// rolled back, the transaction must be consumed through reservation cleanup.
#[derive(Debug, Default)]
pub(super) struct PendingPublishTransactions {
    /// In-flight publish ownership keyed by the exact websocket connection that
    /// reserved the transport media.
    staged: BTreeMap<PendingPublishKey, PendingPublishTransaction>,
}

/// Stable key for one staged publish slot
///
/// This uses the protocol user identity for room ownershio, the runtime
/// connection id for stale-socket rejection and the orchestration stream id
/// for the per-user media slot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingPublishKey {
    user: UserId,
    connection: ConnectionId,
    stream: UserStreamId,
}

/// Publish transaction that owns a reserved transport media line until the
/// room either commits it or rolls it back.
///
/// The descriptor proves only that the user was publish-ready when staging
/// started. The reservation proves that the media transport allocated media
/// that must be accounted for. Keeping both values together prevents call sites
/// from committing room state while forgetting the transport owner, or from
/// cleaning transport media while leaving a descriptor that can still commit.
#[derive(Debug)]
pub(super) struct PendingPublishTransaction {
    /// Stage-time room validation. Commit must re-check it because
    /// replacement or disconnect can make the descriptor stale while transport
    /// work is in flight.
    descriptor: ValidatedPublishDescriptor,
    /// Transport media ownership while the publish is not yet a live producer.
    reservation: StagedMediaReservation,
}

/// Legal ownership states for one staged transport-media reservation.
///
/// These states are intentionally local to the transaction boundary. The
/// websocket layer sees publish intent and answer handling. `RoomState` sees
/// only committed producers. The media transport sees media add or remove
/// calls. This enum records which layer is responsible for the reserved media
/// right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedMediaReservationState {
    /// The media line exists in the media transport but is not committed in
    /// chanel state.
    Reserved,
    /// The chanel committed the producer, so normal unpublish or leave cleanup
    /// owns the transport media from this point onward.
    Committed,
    /// The transaction made an explicit cleanup decision.
    ///
    /// This does not prove the media transport removed the handle
    /// successfully. Cleanup is best-effort at this boundary and failures are
    /// reported through logs.
    Released,
}

/// Owner for transport media reserved by a staged publish.
///
/// # note
///
/// Transport cleanup is async, so this guard must not try to clean up in
/// `Drop`. Instead the guard is consumed by either `cleanup` or `commit`
/// `Drop` only asserts in tests and debug builds when a reservation is still
/// armed. That makes forgotten cleanup visible without hiding async work inside
/// a destructor.
#[derive(Debug)]
#[must_use = "staged media reservations must be committed or cleaned up explicitly"]
struct StagedMediaReservation {
    /// Protocol-facing user identity used to rebuild the transport user
    /// key for cleanup.
    owner_user_id: UserId,
    /// Runtime-local conection identity that prevents a replacement socket
    /// from inheriting stale transport media.
    owner_connection_id: ConnectionId,
    /// Transport-owned media handle that must be removed unless the publish
    /// becomes a live producer.
    transport_media_id: TransportMediaId,
    /// Current ownership state for the reserved media.
    state: StagedMediaReservationState,
}

#[derive(Debug)]
/// Post-commit work for a publish that already became live in room state.
///
/// This exists so the lock-protected state mutation stays small while the
/// follow-up effects still run in the right order after unlock
struct CommittedPublish {
    stream_id: UserStreamId,
    media_counts_before: RoomMediaCounts,
    media_counts_after: RoomMediaCounts,
    consumer_targets: Vec<PendingConsumerBootstrapTarget>,
    diagnostics: DiagnosticsEventData,
}

impl PendingPublishTransactions {
    /// Returns weather this connection already has a staged publish for the
    /// stream.
    ///
    /// This is an idempotency check for websocket publish intents. It is only a
    /// snapshot. Callers that reserve transport media must still call `stage`
    /// afterward to win the registry slot under the lock.
    pub(super) fn contains(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.staged
            .contains_key(&PendingPublishKey::new(user_id, connection_id, stream_id))
    }

    /// Attempt to register a new staged publish.
    ///
    /// If this returns `Err`, the returned transaction is still armed and the
    /// caller must consume it through cleanup. This shape lets the caller
    /// reserve transport media outside the registry lock while still keeping
    /// the post-await duplicate race safe.
    pub(super) fn stage(
        &mut self,
        transaction: PendingPublishTransaction,
    ) -> Result<(), PendingPublishTransaction> {
        let key = transaction.key();
        if self.staged.contains_key(&key) {
            return Err(transaction);
        }
        self.staged.insert(key, transaction);
        Ok(())
    }

    /// Removes one staged publish from the registry and transfers ownership to
    /// the caller.
    ///
    /// The returned transaction must be committed or cleaned up explicitly.
    /// This method is used by explicit unpublish before the answer lands.
    pub(super) fn take(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<PendingPublishTransaction> {
        self.staged
            .remove(&PendingPublishKey::new(user_id, connection_id, stream_id))
    }

    /// Drains every staged publish owned by one websocket connection.
    ///
    /// Conection cleanup and answered negotiation use this transfer so no
    /// later event can see the same staged reservation after cleanup or commit
    /// has started.
    pub(super) fn take_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<PendingPublishTransaction> {
        let matching_keys = self
            .staged
            .keys()
            .filter(|key| key.user == *user_id && key.connection == connection_id)
            .cloned()
            .collect::<Vec<_>>();
        matching_keys
            .into_iter()
            .filter_map(|key| self.staged.remove(&key))
            .collect()
    }
}

impl PendingPublishKey {
    pub(super) fn new(
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Self {
        Self {
            user: user_id.clone(),
            connection: connection_id,
            stream: stream_id.clone(),
        }
    }
}

impl PendingPublishTransaction {
    /// Creates a staged publish transaction from room validation and a
    /// transport media reservation.
    ///
    /// The descriptor and reservation must describe the same owner. The
    /// constructor derives reservation ownership from the descriptor so callers
    /// cannot accidentally pair a media handle with a different connection.
    pub(super) fn new(
        descriptor: ValidatedPublishDescriptor,
        transport_media_id: TransportMediaId,
    ) -> Self {
        let owner_user_id = descriptor.owner_user_id().clone();
        let owner_connection_id = descriptor.owner_connection_id();
        Self {
            descriptor,
            reservation: StagedMediaReservation::new(
                owner_user_id,
                owner_connection_id,
                transport_media_id,
            ),
        }
    }

    fn key(&self) -> PendingPublishKey {
        PendingPublishKey::new(
            self.descriptor.owner_user_id(),
            self.descriptor.owner_connection_id(),
            self.descriptor.stream_id(),
        )
    }

    #[cfg(test)]
    pub(super) const fn transport_media_id(&self) -> TransportMediaId {
        self.reservation.transport_media_id()
    }

    /// Finish a staged publish through the real transport-facing commit path.
    ///
    /// The websocket layer calls this only after the answer landed. If the
    /// transport layer cannot surface the final negotiated producer
    /// parameters, the transaction cleans up its reserved media here because
    /// there is nothing useful left to commit.
    ///
    /// `Some` means the producer is now live and room state owns the
    /// transport media. `None` means no producer was created and the
    /// reservation cleanup path was attempted.
    pub(super) async fn commit(
        self,
        room: &Room,
        applied_answer: &AppliedSessionAnswer,
        observability_port: &impl ObservabilityPort,
        media_port: &(impl MediaPort + SessionPort),
    ) -> Option<UserStreamId> {
        let owner_user_id = self.descriptor.owner_user_id().clone();
        let owner_connection_id = self.descriptor.owner_connection_id();
        let stream_id = self.descriptor.stream_id().clone();
        let transport_media_id = self.reservation.transport_media_id();
        let Some(negotiated_parameters) = applied_answer
            .negotiated_producer_parameters(transport_media_id)
            .cloned()
        else {
            self.cleanup_reserved_media(
                room,
                media_port,
                "media transport failed to remove staged publish media after answered negotiation omitted producer parameters",
            )
            .await;
            warn!(
                user_id = ?owner_user_id,
                connection_id = ?owner_connection_id,
                stream_id = %stream_id,
                ?transport_media_id,
                "answered negotiation did not include staged publish parameters during room commit"
            );
            return None;
        };
        self.commit_with_parameters(room, observability_port, media_port, negotiated_parameters)
            .await
    }

    /// Commits a staged publish when the caller already has router native
    /// producer parameters.
    ///
    /// This is the narrow test and helper entrypoint for flows that already
    /// resolved transport negotiation. It has the same ownership contract as
    /// `commit`: success transfers transport media to the live producer and
    /// rejection consumes the reservation through cleanup.
    pub(super) async fn commit_with_parameters(
        self,
        room: &Room,
        observability_port: &impl ObservabilityPort,
        media_port: &(impl MediaPort + SessionPort),
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<UserStreamId> {
        let Self {
            descriptor,
            reservation,
        } = self;
        let owner_user_id = descriptor.owner_user_id().clone();
        let owner_connection_id = descriptor.owner_connection_id();
        let stream_id = descriptor.stream_id().clone();
        let transport_media_id = reservation.transport_media_id();
        let committed_publish = {
            let mut state = room.state.write().await;
            let media_counts_before = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            // The descriptor is consumed only at the final state commit. If the
            // user was replaced or lost publish readiness while transport
            // work was happening, `commit_published_track` rejects it and we
            // compensate by removing the reserved transport media below
            let prepared_track = descriptor.into_prepared_track(consumable_rtp_parameters);
            let consumer_targets = state.commit_published_track(prepared_track, transport_media_id);
            let media_counts_after = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            consumer_targets.map(|(_producer_id, consumer_targets)| CommittedPublish {
                stream_id: stream_id.clone(),
                media_counts_before,
                media_counts_after,
                consumer_targets,
                diagnostics: DiagnosticsEventData::for_user(
                    room.uuid(),
                    &owner_user_id,
                    telemetry_event::PUBLISH_COMMITTED,
                )
                .with_connection_id(owner_connection_id.as_u64())
                .with_media_worker_id(room.media_worker_id())
                .with_transport_media_id(transport_media_id.as_u64()),
            })
        };
        let Some(committed_publish) = committed_publish else {
            reservation
                .cleanup(
                    room,
                    media_port,
                    "media transport failed to remove published transport media after room commit failed",
                )
                .await;
            warn!(
                user_id = ?owner_user_id,
                connection_id = ?owner_connection_id,
                stream_id = %stream_id,
                transport_media_id = ?transport_media_id,
                "room rejected staged negotiated publish during commit"
            );
            return None;
        };
        reservation.commit();
        let stream_id = committed_publish.stream_id.clone();
        committed_publish
            .finish(room, observability_port, media_port)
            .await;
        Some(stream_id)
    }

    /// Consumes a staged publish that cannot become live.
    ///
    /// All rolback paths funnel through this method so the transport-media
    /// owner and the failure log context stay consistent
    async fn cleanup_reserved_media(
        self,
        room: &Room,
        media_port: &(impl MediaPort + SessionPort),
        failure_message: &str,
    ) -> TransportEffectOutcome {
        self.reservation
            .cleanup(room, media_port, failure_message)
            .await
    }
}

impl StagedMediaReservation {
    /// Arms a reservation for transport media that is not yet live in room
    /// state.
    fn new(
        owner_user_id: UserId,
        owner_connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            owner_user_id,
            owner_connection_id,
            transport_media_id,
            state: StagedMediaReservationState::Reserved,
        }
    }

    const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    /// Attempts to remove the reserved media and marks the reservation as
    /// released
    ///
    /// Cleanup is best-effort because the transport user may already be
    /// closing. The important ownership fact is that this transaction made the
    /// cleanup decision and must not be committed afterward.
    async fn cleanup(
        mut self,
        room: &Room,
        media_port: &(impl MediaPort + SessionPort),
        failure_message: &str,
    ) -> TransportEffectOutcome {
        let outcome = room
            .cleanup_transport_media_with_retry(
                &self.owner_user_id,
                self.owner_connection_id,
                self.transport_media_id,
                media_port,
                failure_message,
            )
            .await;
        self.state = StagedMediaReservationState::Released;
        outcome
    }

    /// Transfers ownership from the staged transaction to the committed
    /// producer stored in chanel state
    fn commit(mut self) {
        self.state = StagedMediaReservationState::Committed;
    }
}

impl Drop for StagedMediaReservation {
    fn drop(&mut self) {
        #[cfg(test)]
        assert_ne!(
            self.state,
            StagedMediaReservationState::Reserved,
            "staged media reservation dropped while still reserved"
        );
        #[cfg(all(debug_assertions, not(test)))]
        debug_assert_ne!(
            self.state,
            StagedMediaReservationState::Reserved,
            "staged media reservation dropped while still reserved"
        );
    }
}

impl CommittedPublish {
    /// Run the post-commit effects after the producer is already live in
    /// room state.
    ///
    /// Ordering matters:
    /// - metrics must observe the state delta that just happened
    /// - consumers bootstrap before receiver-driven policy so the policy can
    ///   choose per-consumer simulcast layers
    /// - diagnostics happen last
    async fn finish(
        self,
        room: &Room,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        room.record_media_count_delta(self.media_counts_before, self.media_counts_after);
        room.bootstrap_consumer_targets(
            media_port,
            ConsumerBootstrapOrigin::Publish,
            self.consumer_targets,
        )
        .await;
        room.sync_source_packet_selection_policy(Some(observability_port), media_port)
            .await;
        room.diagnostics.record(self.diagnostics);
    }
}

impl Room {
    /// Records the live media gauge delta after a chanel state transition.
    ///
    /// Callers pass both snapshots because the state lock should already be
    /// released by the time metrics and transport side effects run.
    pub(super) fn record_media_count_delta(&self, before: RoomMediaCounts, after: RoomMediaCounts) {
        let before_publications = i64::try_from(before.publications).unwrap_or(i64::MAX);
        let after_publications = i64::try_from(after.publications).unwrap_or(i64::MAX);
        self.metrics
            .add_active_publications(after_publications.saturating_sub(before_publications));

        let before_subscriptions = i64::try_from(before.subscriptions).unwrap_or(i64::MAX);
        let after_subscriptions = i64::try_from(after.subscriptions).unwrap_or(i64::MAX);
        self.metrics
            .add_active_subscriptions(after_subscriptions.saturating_sub(before_subscriptions));
    }

    /// Returns weather this connection already owns a staged publish for one
    /// stream
    ///
    /// This is a websocket idempotency helper. It is not an authority for
    /// future staging because another task may reserve and stage transport
    /// media after this snapshot.
    pub async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.pending_publish_transactions
            .lock()
            .await
            .contains(user_id, connection_id, stream_id)
    }

    /// Validates room ownership and reserves transport media for a negotiated publish.
    ///
    /// `PublishStageOutcome::Staged` means the publish is staged, not live.
    /// The caller must still drive renegotiation and later call
    /// `commit_staged_publishes` after the answer lands. The method avoids
    /// holding room state or pending-registry locks across the transport call.
    ///
    /// If another task stages the same stream during the transport await, this
    /// method consumes the duplicate reservation through cleanup and reports
    /// `PublishStageOutcome::DuplicateAfterReservation`.
    pub(crate) async fn stage_negotiated_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        intent: &SourcePublishIntent,
        media_port: &(impl MediaPort + SessionPort),
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let validated_descriptor = {
            let state = self.state.read().await;
            state.validate_publish_descriptor(user_id, connection_id, intent)
        };
        let Some(validated_descriptor) = validated_descriptor else {
            return Ok(PublishStageOutcome::Rejected);
        };
        // Cheap duplicate rejection goes first so we avoid reserving transport
        // media when the same stream is already staged.
        if self.pending_publish_transactions.lock().await.contains(
            user_id,
            connection_id,
            intent.stream_id(),
        ) {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let session_key = self.transport_user_key(user_id, connection_id);
        let transport_media_id = match media_port
            .publish_media(
                &session_key,
                intent.media_kind(),
                &answer_derived_publish_parameters(),
            )
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(error) => {
                warn!(
                    ?user_id,
                    connection_id = ?connection_id,
                    stream_id = %intent.stream_id(),
                    media_kind = ?intent.media_kind(),
                    "failed to stage negotiated publish stream"
                );
                return Err(error);
            }
        };
        // The transport await above leaves a race window where another publish
        // intent can win first. We re-check under the registry lock and clean
        // up immediately so no orphan staged media survives outside the
        // transaction table
        let duplicate_stage = {
            let mut pending_publish_transactions = self.pending_publish_transactions.lock().await;
            pending_publish_transactions
                .stage(PendingPublishTransaction::new(
                    validated_descriptor,
                    transport_media_id,
                ))
                .err()
        };
        if let Some(staged_publish) = duplicate_stage {
            let cleanup = staged_publish
                .cleanup_reserved_media(
                    self,
                    media_port,
                    "media transport failed to remove duplicated staged publish media",
                )
                .await;
            return Ok(PublishStageOutcome::DuplicateAfterReservation { cleanup });
        }
        Ok(PublishStageOutcome::Staged)
    }

    /// Cancels one staged publish before it becomes a live producer.
    ///
    /// This is the explicit unpublish-before-answer path. A successful rollback
    /// consumes the reservation even when transport cleanup fails, because the
    /// publish must not remain commit-capable after the user requested removal.
    pub(crate) async fn rollback_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
        media_port: &(impl MediaPort + SessionPort),
    ) -> RollbackStagedPublishOutcome {
        // Explicit unpublish before commit only needs transport cleanup because
        // the producer never became live in room state.
        let staged_publish =
            self.pending_publish_transactions
                .lock()
                .await
                .take(user_id, connection_id, stream_id);
        let Some(staged_publish) = staged_publish else {
            return RollbackStagedPublishOutcome::NotStaged;
        };
        let cleanup = staged_publish
            .cleanup_reserved_media(
                self,
                media_port,
                "media transport failed to remove staged publish media during rollback",
            )
            .await;
        RollbackStagedPublishOutcome::RolledBack { cleanup }
    }

    /// Cleans up every staged publish owned by a websocket connection.
    ///
    /// User replacement, logical disconnect and websocket drop use this to
    /// drain all in-flight reservations before the connection can disapear.
    /// Cleanup remains best-effort because transport teardown may already be in
    /// progress
    pub(crate) async fn rollback_staged_publishes_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &(impl MediaPort + SessionPort),
    ) {
        let staged_publishes = self
            .pending_publish_transactions
            .lock()
            .await
            .take_for_connection(user_id, connection_id);
        for staged_publish in staged_publishes {
            staged_publish
                .cleanup_reserved_media(
                    self,
                    media_port,
                    "media transport failed to remove staged publish media during connection cleanup",
                )
                .await;
        }
    }

    /// Commits every staged publish for a connection after negotiation
    /// answered successfully
    ///
    /// The registry is drained before commit work starts so a later websocket
    /// message cannot commit the same reservation twice. Each transaction then
    /// re-checks current chanel state before creating a live producer. If that
    /// state is stale, the transaction consumes its transport reservation
    /// through cleanup instead.
    pub(crate) async fn commit_staged_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        applied_answer: &AppliedSessionAnswer,
        observability_port: &impl ObservabilityPort,
        media_port: &(impl MediaPort + SessionPort),
    ) -> Vec<UserStreamId> {
        let staged_publishes = self
            .pending_publish_transactions
            .lock()
            .await
            .take_for_connection(user_id, connection_id);
        let mut committed_stream_ids = Vec::new();
        for staged_publish in staged_publishes {
            if let Some(stream_id) = staged_publish
                .commit(self, applied_answer, observability_port, media_port)
                .await
            {
                committed_stream_ids.push(stream_id);
            }
        }
        committed_stream_ids
    }

    /// Releases a pending consumer-bootstrap reservation after the matching
    /// effect path no longer needs it.
    ///
    /// This mirrors staged-publish ownership on the subscriber side: room
    /// state owns the reservation, while metrics are updated after unlock>
    pub(super) async fn release_pending_consumer_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) {
        let mut state = self.state.write().await;
        let media_counts_before = RoomMediaCounts {
            publications: state.publication_count(),
            subscriptions: state.subscription_count(),
        };
        state.release_pending_consumer_bootstrap(target);
        let media_counts_after = RoomMediaCounts {
            publications: state.publication_count(),
            subscriptions: state.subscription_count(),
        };
        drop(state);
        self.record_media_count_delta(media_counts_before, media_counts_after);
    }

    /// Best-effort transport-media cleanup for a known chanel owner.
    ///
    /// The chanel has already decided that the media should no longer be
    /// owned by room state. A transport failure is logged but does not rebuild
    /// previous chanel state
    pub(super) async fn cleanup_transport_media(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
        media_port: &impl MediaPort,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        if media_port
            .remove_media(
                &self.transport_user_key(user_id, connection_id),
                transport_media_id,
            )
            .await
            .is_err()
        {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?transport_media_id,
                "{failure_message}"
            );
            return TransportEffectOutcome::Failed;
        }
        TransportEffectOutcome::Applied
    }

    /// Removes a batch of committed transport media where the caller needs to
    /// know whether every transport cleanup succeeded.
    ///
    /// Unlike staged publish rollback,this is used by transitions that already
    /// removed live room state and need a strict transport outcome to decide
    /// weather the outer cleanup can keep going.
    pub(super) async fn cleanup_transport_removals_strict(
        &self,
        media_port: &impl MediaPort,
        removals: &[TransportMediaRemoval],
    ) -> bool {
        for removal in removals {
            if media_port
                .remove_media(
                    &self.transport_user_key(removal.user(), removal.connection()),
                    removal.transport_media(),
                )
                .await
                .is_err()
            {
                warn!(
                    user_id = ?removal.user(),
                    connection_id = ?removal.connection(),
                    transport_media_id = ?removal.transport_media(),
                    "media transport failed to remove transport media during room cleanup"
                );
                return false;
            }
        }
        true
    }

    pub(super) async fn apply_initial_consumer_pause_state(
        &self,
        target: &PendingConsumerBootstrapTarget,
        consumer_transport_media_id: TransportMediaId,
        consumer_active: bool,
        media_port: &impl MediaPort,
        origin: ConsumerBootstrapOrigin,
    ) {
        if consumer_active {
            return;
        }
        if media_port
            .set_consumer_active(
                &self
                    .transport_user_key(target.consumer_user_id(), target.consumer_connection_id()),
                consumer_transport_media_id,
                &self
                    .transport_user_key(target.producer_user_id(), target.producer_connection_id()),
                target.transport_media_id(),
                ConsumerActivity::Inactive,
            )
            .await
            .is_err()
        {
            warn!(
                consumer_user_id = ?target.consumer_user_id(),
                producer_user_id = ?target.producer_user_id(),
                ?origin,
                "media transport failed to apply the initial consumer pause state"
            );
        }
    }
}

/// Marker parameters for a protocol publish whose concrete SSRC/RID bindings
/// are projected from the accepted SDP answer.
fn answer_derived_publish_parameters() -> RouterRtpParameters {
    RouterRtpParameters::new(vec![], vec![], vec![])
}

#[cfg(test)]
mod tests {
    use super::StagedMediaReservation;
    use crate::runtime::{ConnectionId, UserId, media_transport::TransportMediaId};

    #[test]
    #[should_panic(expected = "staged media reservation dropped while still reserved")]
    fn reserved_staged_media_reservation_panics_when_dropped_in_tests() {
        let _reservation = StagedMediaReservation::new(
            UserId::Integer(1),
            ConnectionId::from_raw(1),
            TransportMediaId::new(1),
        );
    }

    #[test]
    fn committed_staged_media_reservation_can_drop_in_tests() {
        StagedMediaReservation::new(
            UserId::Integer(1),
            ConnectionId::from_raw(1),
            TransportMediaId::new(1),
        )
        .commit();
    }
}
