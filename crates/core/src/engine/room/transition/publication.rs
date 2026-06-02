//! publication transitions for staged and committed room media

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::MutexGuard,
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::super::{
    Room, RoomMediaCounts, RoomUserOperation, SourcePolicyEvent,
    cleanup::TransportCleanupOperation,
    effects::{RoomCommit, RoomEffectContext},
    media_graph::{
        ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, ValidatedPublishDescriptor,
    },
};
use crate::{
    PublicationActivity, PublicationActivityOutcome, PublishStageOutcome,
    RollbackStagedPublishOutcome, TransportEffectOutcome, UnpublishOutcome,
    engine::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{
            AppliedSessionAnswer, SessionUploadEncoding, TransportAdapterError, TransportMediaId,
            TransportSourceKey,
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
/// sockets cannot share the slot with the current websocket for the same user
/// facing user id.
///
/// This registry owns only in-flight reservations. Once a publish commits, the
/// producer and its transport media belong to room state.
#[derive(Debug, Default)]
pub struct PendingPublishTransactions {
    staged: BTreeMap<PendingPublishKey, ReservedPublish>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingPublishKey {
    user: UserId,
    connection: ConnectionId,
    stream: UserStreamId,
}

/// Publish transaction that keeps a reserved transport media line until the
/// room either commits it or rolls it back.
///
/// The descriptor proves only that the user was publish-ready when staging
/// started. The reservation proves that the media transport allocated media
/// that must be accounted for. Keeping both values together prevents call sites
/// from committing room state while forgetting the transport reservation, or from
/// cleaning transport media while leaving a descriptor that can still commit.
#[derive(Debug)]
pub struct ReservedPublish {
    descriptor: ValidatedPublishDescriptor,
    reservation: StagedMediaReservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedMediaReservationState {
    Reserved,
    Committed,
    Released,
}

/// Guard for transport media reserved by a staged publish.
///
/// Transport cleanup is async, so this guard must not try to clean up in
/// `Drop`. Instead the guard is consumed by either `cleanup` or `commit`
/// `Drop` only asserts in tests and debug builds when a reservation is still
/// armed. That makes forgotten cleanup visible without hiding async work inside
/// a destructor.
#[derive(Debug)]
#[must_use = "staged media reservations must be committed or cleaned up explicitly"]
struct StagedMediaReservation {
    user: UserId,
    connection: ConnectionId,
    media: TransportMediaId,
    state: StagedMediaReservationState,
}

/// Publish ownership after the accepted answer supplied producer parameters.
#[derive(Debug)]
struct AnsweredPublish {
    descriptor: ValidatedPublishDescriptor,
    reservation: StagedMediaReservation,
    rtp: RouterRtpParameters,
    encodings: Vec<SessionUploadEncoding>,
}

#[derive(Debug)]
struct CommittedPublish {
    before: RoomMediaCounts,
    after: RoomMediaCounts,
    consumers: Vec<PendingConsumerBootstrapTarget>,
    diagnostics: DiagnosticsEventData,
}

impl PendingPublishTransactions {
    /// Returns whether this connection already has a staged publish for the
    /// stream.
    ///
    /// This is an idempotency check for websocket publish intents. It is only a
    /// snapshot. Callers that reserve transport media must still call `stage`
    /// afterward to win the registry slot under the lock.
    fn contains(
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
    fn stage(&mut self, transaction: ReservedPublish) -> Result<(), ReservedPublish> {
        let key = transaction.key();
        match self.staged.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(transaction);
                Ok(())
            }
            Entry::Occupied(_) => Err(transaction),
        }
    }

    /// Removes one staged publish from the registry and transfers ownership to
    /// the caller.
    ///
    /// The returned transaction must be committed or cleaned up explicitly.
    /// This method is used by explicit unpublish before the answer lands.
    fn take(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ReservedPublish> {
        self.staged
            .remove(&PendingPublishKey::new(user_id, connection_id, stream_id))
    }

    /// Drains every staged publish owned by one websocket connection.
    ///
    /// Connection cleanup and answered negotiation use this transfer so no
    /// later event can see the same staged reservation after cleanup or commit
    /// has started.
    fn take_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<ReservedPublish> {
        self.staged
            .extract_if(.., |key, _transaction| {
                &key.user == user_id && key.connection == connection_id
            })
            .map(|(_key, transaction)| transaction)
            .collect()
    }
}

impl PendingPublishKey {
    fn new(user_id: &UserId, connection_id: ConnectionId, stream_id: &UserStreamId) -> Self {
        Self {
            user: user_id.clone(),
            connection: connection_id,
            stream: stream_id.clone(),
        }
    }
}

impl ReservedPublish {
    /// Creates a staged publish transaction from room validation and a
    /// transport media reservation.
    ///
    pub fn new(
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
    const fn transport_media_id(&self) -> TransportMediaId {
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
    async fn commit(
        self,
        operation: RoomUserOperation<'_>,
        applied_answer: &AppliedSessionAnswer,
    ) -> Option<UserStreamId> {
        let user = self.descriptor.owner_user_id().clone();
        let connection = self.descriptor.owner_connection_id();
        let stream_id = self.descriptor.stream_id().clone();
        let media = self.reservation.transport_media_id();
        let answered_publish = match AnsweredPublish::from_reserved(self, applied_answer) {
            Ok(answered_publish) => answered_publish,
            Err(reserved_publish) => {
                reserved_publish
                    .cleanup_reserved_media(
                operation,
                "media transport failed to remove staged publish media after answered negotiation omitted producer parameters",
            )
                    .await;
                warn!(
                    user_id = ?user,
                    connection_id = ?connection,
                    stream_id = %stream_id,
                    transport_media_id = ?media,
                    "answered negotiation did not include staged publish parameters during room commit"
                );
                return None;
            }
        };
        answered_publish.commit(operation).await
    }

    /// Consumes a staged publish that cannot become live.
    ///
    /// All rollback paths funnel through this method so the transport-media
    /// owner and the failure log context stay consistent
    async fn cleanup_reserved_media(
        self,
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        self.reservation.cleanup(operation, failure_message).await
    }
}

impl AnsweredPublish {
    fn from_reserved(
        reserved_publish: ReservedPublish,
        applied_answer: &AppliedSessionAnswer,
    ) -> Result<Self, ReservedPublish> {
        let media = reserved_publish.reservation.transport_media_id();
        let Some(rtp) = applied_answer
            .negotiated_producer_parameters(media)
            .cloned()
        else {
            return Err(reserved_publish);
        };
        let encodings = applied_answer
            .negotiated_producer_upload_encodings(media)
            .to_vec();
        let ReservedPublish {
            descriptor,
            reservation,
        } = reserved_publish;
        Ok(Self {
            descriptor,
            reservation,
            rtp,
            encodings,
        })
    }

    async fn commit(self, operation: RoomUserOperation<'_>) -> Option<UserStreamId> {
        let room = operation.room();
        let Self {
            descriptor,
            reservation,
            rtp,
            encodings,
        } = self;
        let user = descriptor.owner_user_id().clone();
        let connection = descriptor.owner_connection_id();
        let stream_id = descriptor.stream_id().clone();
        let media = reservation.transport_media_id();
        let committed_publish = {
            let mut state = room.state.write().await;
            let before = state.media_counts();
            let prepared_track =
                descriptor.into_prepared_track_with_upload_encodings(rtp, encodings);
            let consumers = state.commit_publish_reservation(prepared_track, media);
            let after = state.media_counts();
            drop(state);
            consumers.map(|(_producer_id, consumers)| CommittedPublish {
                before,
                after,
                consumers,
                diagnostics: DiagnosticsEventData::for_user(
                    room.uuid(),
                    &user,
                    telemetry_event::PUBLISH_COMMITTED,
                )
                .with_connection_id(connection.as_u64())
                .with_media_worker_id(
                    room.transport_user_key(&user, connection)
                        .media_worker_id()
                        .as_usize(),
                )
                .with_transport_media_id(media.as_u64()),
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
                user_id = ?user,
                connection_id = ?connection,
                stream_id = %stream_id,
                transport_media_id = ?media,
                "room rejected staged negotiated publish during commit"
            );
            return None;
        };
        reservation.commit();
        committed_publish.finish(operation).await;
        Some(stream_id)
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
    fn new(user: UserId, connection: ConnectionId, media: TransportMediaId) -> Self {
        Self {
            user,
            connection,
            media,
            state: StagedMediaReservationState::Reserved,
        }
    }

    const fn transport_media_id(&self) -> TransportMediaId {
        self.media
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
        let cleanup = [TransportCleanupOperation::RemoveMedia {
            session_key: operation
                .room()
                .transport_user_key(&self.user, self.connection),
            connection_id: self.connection,
            transport_media_id: self.media,
        }];
        let outcome = operation
            .room()
            .execute_transport_cleanup_operations(operation.media_transport(), &cleanup)
            .await;
        if outcome == TransportEffectOutcome::Failed {
            warn!(
                user_id = ?self.user,
                connection_id = ?self.connection,
                transport_media_id = ?self.media,
                "{failure_message}"
            );
        }
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
        let commit = {
            let worker_lookup = room.placement_state.worker_lookup();
            let mut state = room.state.write().await;
            let before = state.media_counts();
            let bootstraps = state.plan_consumers(self.consumers, worker_lookup);
            let after = state.media_counts();
            drop(state);
            RoomCommit::new()
                .with_media_count_delta(self.before, self.after)
                .with_media_count_delta(before, after)
                .with_bootstraps(bootstraps, ConsumerBootstrapOrigin::Publish)
        };
        commit
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
            .record_diagnostics(self.diagnostics)
            .execute(
                room,
                RoomEffectContext::runtime(operation.media_transport()),
            )
            .await;
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
        let Some(validated_descriptor) = ({
            let state = room.state.read().await;
            state.validate_publish_descriptor(self.user_id(), self.connection_id(), intent)
        }) else {
            return Ok(PublishStageOutcome::Rejected);
        };
        if room.pending_publish_transactions().contains(
            self.user_id(),
            self.connection_id(),
            intent.stream_id(),
        ) {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let session_key = self.transport_user_key();
        let rtp_parameters = RouterRtpParameters::default();
        let media = match self
            .media_transport()
            .publish_media(&session_key, intent.media_kind(), &rtp_parameters)
            .await
        {
            Ok(media) => media,
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
        #[cfg(test)]
        room.inject_next_duplicate_for_test(&validated_descriptor, media);
        let reserved_publish = ReservedPublish::new(validated_descriptor, media);
        if let Some(staged_publish) = {
            let mut pending_publish_transactions = room.pending_publish_transactions();
            pending_publish_transactions.stage(reserved_publish).err()
        } {
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
        let Some(staged_publish) = self.room().pending_publish_transactions().take(
            self.user_id(),
            self.connection_id(),
            stream_id,
        ) else {
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
    /// drain all in-flight reservations before the connection can disappear.
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

    pub(crate) async fn set_publication_activity(
        self,
        stream_id: &UserStreamId,
        activity: PublicationActivity,
    ) -> PublicationActivityOutcome {
        let room = self.room();
        let active = activity.is_active();
        let Some(producer_target) = ({
            let state = room.state.read().await;
            state.producer_route_target(self.user_id(), self.connection_id(), stream_id)
        }) else {
            return PublicationActivityOutcome::MissingPublication;
        };
        let transport_user_key =
            room.transport_user_key(self.user_id(), producer_target.owner_connection_id());
        let media_worker_id = transport_user_key.media_worker_id();
        let Some(outcome) = ({
            let mut state = room.state.write().await;
            state.apply_producer_activity(self.user_id(), &producer_target, stream_id, active)
        }) else {
            return PublicationActivityOutcome::StalePublication;
        };
        let transport_media_id = producer_target.transport_media_id();
        let source = TransportSourceKey::new(transport_user_key, transport_media_id);
        let diagnostics = DiagnosticsEventData::for_user(
            room.uuid(),
            self.user_id(),
            telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
        )
        .with_connection_id(self.connection_id().as_u64())
        .with_media_worker_id(media_worker_id.as_usize())
        .with_transport_media_id(transport_media_id.as_u64())
        .insert_field("active", active)
        .insert_field("stream_id", stream_id.to_string());
        let (recipients, track_update) = outcome.into_track_binding_update();
        let execution = RoomCommit::new()
            .with_producer_activity(source, active, stream_id.clone(), diagnostics)
            .with_track_binding_update(recipients, track_update)
            .with_source_policy_event(SourcePolicyEvent::FanoutPressureChanged)
            .execute(room, RoomEffectContext::runtime(self.media_transport()))
            .await;
        PublicationActivityOutcome::Applied {
            transport_update: execution.producer_activity(),
        }
    }

    pub(crate) async fn unpublish(self, stream_id: &UserStreamId) -> UnpublishOutcome {
        let room = self.room();
        let user_id = self.user_id();
        let connection_id = self.connection_id();
        let media_port = self.media_transport();
        let (before, outcome, after) = {
            let mut state = room.state.write().await;
            let before = state.media_counts();
            let outcome = state.unpublish_track(user_id, connection_id, stream_id);
            let after = state.media_counts();
            drop(state);
            (before, outcome, after)
        };
        let Some(outcome) = outcome else {
            return UnpublishOutcome::MissingPublication;
        };
        let (recipients, track_update, relays, removals) = outcome.into_parts();
        let execution = RoomCommit::new()
            .with_media_count_delta(before, after)
            .with_relay_effects(relays)
            .with_transport_removals(room, removals)
            .with_track_binding_update(recipients, track_update)
            .with_source_policy_event(SourcePolicyEvent::RouteGraphChanged)
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        room.reconcile_spillover_routers().await;
        UnpublishOutcome::Unpublished {
            cleanup: execution.cleanup(),
        }
    }
}

impl Room {
    fn pending_publish_transactions(&self) -> MutexGuard<'_, PendingPublishTransactions> {
        lock_unpoisoned(&self.pending_publish_transactions)
    }

    /// Returns whether this connection already owns a staged publish for one
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::panic,
        reason = "transition tests fail loudly when fixed room setup is invalid"
    )]

    use std::sync::Arc;

    use o_sfu_router::test_support::rtp_samples::sample_simulcast_video_rtp_parameters;

    use super::StagedMediaReservation;
    use crate::{
        PublishStageOutcome, RollbackStagedPublishOutcome, SessionNegotiationOutcome,
        TransportEffectOutcome,
        engine::{
            ConnectionId, TestSourceKind, UserId, UserPermissions,
            media_transport::{
                AppliedSessionAnswer, MediaTransport, TransportMediaId,
                test_support::{test_media_transport_builder, test_rtc_port_range},
            },
            metrics::RuntimeMetrics,
            room::{Room, RoomConfig, RoomManager, UserOutboundSender},
            source_model::test_support::{source_publish_intent_for_source, stream_id_for_source},
        },
    };

    fn media_transport() -> MediaTransport {
        let rtc_port_range = test_rtc_port_range(4).expect("test ports should be available");
        test_media_transport_builder(rtc_port_range)
            .worker_count(4)
            .build()
            .expect("test media transport config should be valid")
    }

    fn test_sender() -> UserOutboundSender {
        UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default())).0
    }

    async fn join_user(room: &Arc<Room>, user_id: &UserId) -> ConnectionId {
        room.test_api()
            .lifecycle()
            .join_user(
                user_id.clone(),
                None,
                UserPermissions::default(),
                test_sender(),
            )
            .await
            .expect("test user should join")
    }

    async fn prepare_publish_session(
        room: &Arc<Room>,
        media_transport: &MediaTransport,
        user_id: &UserId,
    ) -> ConnectionId {
        let connection_id = join_user(room, user_id).await;
        let session_key = room.transport_user_key(user_id, connection_id);
        media_transport
            .create_initial_session_offer(&session_key)
            .await
            .expect("test session should create an initial offer");
        assert_eq!(
            room.apply_session_negotiated(
                user_id,
                connection_id,
                o_sfu_router::MediaCapabilities::default(),
                media_transport,
            )
            .await,
            SessionNegotiationOutcome::Applied
        );
        connection_id
    }

    async fn staged_room() -> (Arc<Room>, MediaTransport, UserId, ConnectionId) {
        let manager = RoomManager::for_test();
        let room = manager
            .serve_room(
                "issuer-transition-publication",
                "room",
                &RoomConfig::default(),
                None,
            )
            .await;
        let media_transport = media_transport();
        let user_id = UserId::Integer(1);
        let connection_id = prepare_publish_session(&room, &media_transport, &user_id).await;
        assert_eq!(
            room.user_operation(&user_id, connection_id, &media_transport)
                .stage_negotiated_publish(&source_publish_intent_for_source(
                    TestSourceKind::ScalableVideo,
                ))
                .await
                .expect("stage publish should not fail"),
            PublishStageOutcome::Staged
        );
        (room, media_transport, user_id, connection_id)
    }

    async fn staged_media_id(
        room: &Room,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportMediaId {
        room.staged_media_id(user_id, connection_id, TestSourceKind::ScalableVideo)
            .await
            .expect("test publish should be staged")
    }

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

    #[tokio::test]
    async fn staged_publish_is_not_visible_in_room_graph_before_answer() {
        let (room, media_transport, user_id, connection_id) = staged_room().await;
        let transport_media_id = staged_media_id(&room, &user_id, connection_id).await;
        let session_key = room.transport_user_key(&user_id, connection_id);

        assert_eq!(room.test_api().inspect().producer_count().await, 0);
        assert!(
            room.user_operation(&user_id, connection_id, &media_transport)
                .has_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
        );
        assert!(
            media_transport
                .transport_media_mid(&session_key, transport_media_id)
                .await
                .is_some()
        );
        assert_eq!(
            room.user_operation(&user_id, connection_id, &media_transport)
                .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
                .await,
            RollbackStagedPublishOutcome::RolledBack {
                cleanup: TransportEffectOutcome::Applied
            }
        );
    }

    #[tokio::test]
    async fn missing_answered_producer_parameters_release_reserved_publish() {
        let (room, media_transport, user_id, connection_id) = staged_room().await;
        let transport_media_id = staged_media_id(&room, &user_id, connection_id).await;

        let committed = room
            .user_operation(&user_id, connection_id, &media_transport)
            .commit_staged_publishes(&AppliedSessionAnswer::default())
            .await;

        assert!(committed.is_empty());
        assert_eq!(room.test_api().inspect().producer_count().await, 0);
        assert_eq!(room.staged_count(&user_id, connection_id).await, 0);
        assert!(
            media_transport
                .test_api()
                .route_entry_by_media_id(transport_media_id)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn stale_connection_commit_rejects_and_releases_reserved_publish() {
        let (room, media_transport, user_id, stale_connection_id) = staged_room().await;
        let transport_media_id = staged_media_id(&room, &user_id, stale_connection_id).await;
        let _new_connection_id = prepare_publish_session(&room, &media_transport, &user_id).await;
        let applied_answer = AppliedSessionAnswer::from_negotiated_producers([(
            transport_media_id,
            sample_simulcast_video_rtp_parameters(None),
        )]);

        let committed = room
            .user_operation(&user_id, stale_connection_id, &media_transport)
            .commit_staged_publishes(&applied_answer)
            .await;

        assert!(committed.is_empty());
        assert_eq!(room.test_api().inspect().producer_count().await, 0);
        assert!(
            media_transport
                .test_api()
                .route_entry_by_media_id(transport_media_id)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn rollback_before_answer_consumes_reserved_publish_once() {
        let (room, media_transport, user_id, connection_id) = staged_room().await;
        let transport_media_id = staged_media_id(&room, &user_id, connection_id).await;

        assert_eq!(
            room.user_operation(&user_id, connection_id, &media_transport)
                .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
                .await,
            RollbackStagedPublishOutcome::RolledBack {
                cleanup: TransportEffectOutcome::Applied
            }
        );

        assert_eq!(
            room.user_operation(&user_id, connection_id, &media_transport)
                .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
                .await,
            RollbackStagedPublishOutcome::NotStaged
        );
        assert!(
            media_transport
                .test_api()
                .route_entry_by_media_id(transport_media_id)
                .await
                .is_none()
        );
    }
}
