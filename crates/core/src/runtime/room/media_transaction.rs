//! Room-owned transaction helpers for staged publish and transport cleanup.
//!
//! # Role
//!
//! This module contain the room-side unit of work for media changes that need
//! transport calls. `RoomState` stays authoritative for live producers and
//! consumers. The media transport stays authoritative for allocated media
//! lines. This file owns the short-lived (only latts for transactions)
//! bridge between those two layers so websocket publish and unpublish
//! flows do not have to remember rollback  details
//!
//!
//! # Staged publish lifecycle
//!
//! A publish is staged only after room state validates the current user and
//! the media transport reserves a media line While the browser answers
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

use std::{collections::BTreeMap, sync::MutexGuard};

use o_sfu_router::MediaStream as RouterRtpParameters;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{
    Room, RoomMediaCounts, RoomUserOperation,
    effects::{
        PublishReservationContinuation, RoomEffectBatch, RoomEffectContext, RoomTransportEffect,
    },
    state::{ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, ValidatedPublishDescriptor},
};
use crate::{
    PublishStageOutcome, RollbackStagedPublishOutcome, TransportEffectOutcome,
    runtime::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{
            AppliedSessionAnswer, MediaTransport, SessionUploadEncoding, TransportAdapterError,
            TransportMediaId,
        },
        source_model::{SourcePublishIntent, UserStreamId},
        sync::lock_unpoisoned,
    },
};

#[cfg(any(test, feature = "testing-transport"))]
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
/// producer and its transport media belong to room state. Once a publish is
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
    /// room state.
    Reserved,
    /// The room committed the producer, so normal unpublish or leave cleanup
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
    /// Runtime-local connection identity that prevents a replacement socket
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
    pub(super) fn new(
        descriptor: ValidatedPublishDescriptor,
        transport_media_id: TransportMediaId,
    ) -> Self {
        let reservation = StagedMediaReservation::for_descriptor(&descriptor, transport_media_id);
        Self {
            descriptor,
            reservation,
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
        operation: RoomUserOperation<'_>,
        applied_answer: &AppliedSessionAnswer,
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
                operation,
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
        let upload_encodings = applied_answer
            .negotiated_producer_upload_encodings(transport_media_id)
            .to_vec();
        self.commit_with_parameters_and_upload_encodings(
            operation,
            negotiated_parameters,
            upload_encodings,
        )
        .await
    }

    async fn commit_with_parameters_and_upload_encodings(
        self,
        operation: RoomUserOperation<'_>,
        consumable_rtp_parameters: RouterRtpParameters,
        upload_encodings: Vec<SessionUploadEncoding>,
    ) -> Option<UserStreamId> {
        let room = operation.room();
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
            let media_counts_before = state.media_counts();
            // The descriptor is consumed only at the final state commit. If the
            // user was replaced or lost publish readiness while transport
            // work was happening, `commit_published_track` rejects it and we
            // compensate by removing the reserved transport media below
            let prepared_track = descriptor.into_prepared_track_with_upload_encodings(
                consumable_rtp_parameters,
                upload_encodings,
            );
            let consumer_targets = state.commit_published_track(prepared_track, transport_media_id);
            let media_counts_after = state.media_counts();
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
                    operation,
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
        committed_publish.finish(operation).await;
        Some(stream_id)
    }

    /// Consumes a staged publish that cannot become live.
    ///
    /// All rolback paths funnel through this method so the transport-media
    /// owner and the failure log context stay consistent
    async fn cleanup_reserved_media(
        self,
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        self.reservation.cleanup(operation, failure_message).await
    }
}

impl StagedMediaReservation {
    fn for_descriptor(
        descriptor: &ValidatedPublishDescriptor,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self::new(
            descriptor.owner_user_id().clone(),
            descriptor.owner_connection_id(),
            transport_media_id,
        )
    }

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
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        let outcome = operation
            .room()
            .cleanup_transport_media_with_retry(
                &self.owner_user_id,
                self.owner_connection_id,
                self.transport_media_id,
                operation.media_transport(),
                failure_message,
            )
            .await;
        self.state = StagedMediaReservationState::Released;
        outcome
    }

    /// Transfers ownership from the staged transaction to the committed
    /// producer stored in room state.
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
    async fn finish(self, operation: RoomUserOperation<'_>) {
        let room = operation.room();
        RoomEffectBatch::new()
            .with_media_count_delta(self.media_counts_before, self.media_counts_after)
            .execute(
                room,
                RoomEffectContext::runtime(operation.media_transport()),
            )
            .await;
        room.bootstrap_consumer_targets(
            operation.media_transport(),
            ConsumerBootstrapOrigin::Publish,
            self.consumer_targets,
        )
        .await;
        RoomEffectBatch::new()
            .refresh_source_policy()
            .record_diagnostics(self.diagnostics)
            .execute(
                room,
                RoomEffectContext::runtime(operation.media_transport()),
            )
            .await;
    }
}

impl Room {
    fn pending_publish_transactions(&self) -> MutexGuard<'_, PendingPublishTransactions> {
        lock_unpoisoned(&self.pending_publish_transactions)
    }

    /// Records the live media gauge delta after a room state transition.
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
    #[must_use]
    pub fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.pending_publish_transactions()
            .contains(user_id, connection_id, stream_id)
    }
}

impl RoomUserOperation<'_> {
    #[must_use]
    pub(crate) fn has_staged_publish(self, stream_id: &UserStreamId) -> bool {
        self.room().pending_publish_transactions().contains(
            self.user_id(),
            self.connection_id(),
            stream_id,
        )
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
        self,
        intent: &SourcePublishIntent,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let room = self.room();
        let validated_descriptor = {
            let state = room.state.read().await;
            state.validate_publish_descriptor(self.user_id(), self.connection_id(), intent)
        };
        let Some(validated_descriptor) = validated_descriptor else {
            return Ok(PublishStageOutcome::Rejected);
        };
        if room.pending_publish_transactions().contains(
            self.user_id(),
            self.connection_id(),
            intent.stream_id(),
        ) {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let publish_effect = RoomTransportEffect::PublishReservation {
            continuation: PublishReservationContinuation {
                user: self.user_id().clone(),
                connection: self.connection_id(),
                stream: intent.stream_id().clone(),
            },
            session_key: self.transport_user_key(),
            media_kind: intent.media_kind(),
            rtp_parameters: answer_derived_publish_parameters(),
        };
        let transport_media_id = match publish_effect
            .execute_publish_reservation(self.media_transport())
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(error) => {
                warn!(
                    user_id = ?self.user_id(),
                    connection_id = ?self.connection_id(),
                    stream_id = %intent.stream_id(),
                    media_kind = ?intent.media_kind(),
                    "failed to stage negotiated publish stream"
                );
                return Err(error);
            }
        };
        let transaction = PendingPublishTransaction::new(validated_descriptor, transport_media_id);
        let duplicate_stage = {
            let mut pending_publish_transactions = room.pending_publish_transactions();
            pending_publish_transactions.stage(transaction).err()
        };
        if let Some(staged_publish) = duplicate_stage {
            let cleanup = staged_publish
                .cleanup_reserved_media(
                    self,
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
        self,
        stream_id: &UserStreamId,
    ) -> RollbackStagedPublishOutcome {
        let staged_publish = self.room().pending_publish_transactions().take(
            self.user_id(),
            self.connection_id(),
            stream_id,
        );
        let Some(staged_publish) = staged_publish else {
            return RollbackStagedPublishOutcome::NotStaged;
        };
        let cleanup = staged_publish
            .cleanup_reserved_media(
                self,
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
    pub(crate) async fn rollback_staged_publishes_for_connection(self) {
        let staged_publishes = self
            .room()
            .pending_publish_transactions()
            .take_for_connection(self.user_id(), self.connection_id());
        for staged_publish in staged_publishes {
            staged_publish
                .cleanup_reserved_media(
                    self,
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
    /// re-checks current room state before creating a live producer. If that
    /// state is stale, the transaction consumes its transport reservation
    /// through cleanup instead.
    pub(crate) async fn commit_staged_publishes(
        self,
        applied_answer: &AppliedSessionAnswer,
    ) -> Vec<UserStreamId> {
        let staged_publishes = self
            .room()
            .pending_publish_transactions()
            .take_for_connection(self.user_id(), self.connection_id());
        let mut committed_stream_ids = Vec::new();
        for staged_publish in staged_publishes {
            if let Some(stream_id) = staged_publish.commit(self, applied_answer).await {
                committed_stream_ids.push(stream_id);
            }
        }
        committed_stream_ids
    }
}

impl Room {
    /// Releases a pending consumer-bootstrap reservation after the matching
    /// effect path no longer needs it.
    ///
    /// This mirrors staged-publish ownership on the subscriber side: room
    /// state owns the reservation, while metrics are updated after unlock>
    pub(super) async fn release_pending_consumer_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
        media_port: &MediaTransport,
    ) {
        let (media_counts_before, media_counts_after, relay_effects) = {
            let mut state = self.state.write().await;
            let media_counts_before = state.media_counts();
            let relay_effects = state.release_pending_consumer_bootstrap(target);
            let media_counts_after = state.media_counts();
            drop(state);
            (media_counts_before, media_counts_after, relay_effects)
        };
        RoomEffectBatch::new()
            .with_media_count_delta(media_counts_before, media_counts_after)
            .with_relay_effects(relay_effects)
            .execute(self, RoomEffectContext::runtime(media_port))
            .await;
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
