//! Channel-owned transaction helpers for staged publish and transport cleanup.
//!
//! # role (between chanel and transport)
//!
//! This module owns the room-side unit of work for media changes that need
//! transport calls. `ChannelState` stays authoritative for live producers and
//! consumers. The transport adapter stays authoritative for allocated media
//! lines. This file owns the short-lived (only latts for transactions)
//! bridge between those two layers so websocket publish and unpublish
//! flows do not have to remember rollback  details
//!
//!
//! # Staged publish lifecycle
//!
//! A publish is staged only after chanel state validates the current session
//! and the transport adapter reserves a media line While the browser answers
//! renegotiation, that reservation lives in `PendingPublishTransactions`.
//! Answer handling later drains the transaction and either commits it into
//! channel state or consumes it through transport cleanup.
//!
//! # Concurrency
//!
//! This is cold-path orchestration. Transport calls happen after channel state
//! locks are released. The pending-publish registry has its own mutex, but that
//! lock is held only for lookup, insertion and draining. Commit and cleanup run
//! after the registry lock is released.

use std::collections::BTreeMap;

use o_sfu_protocol::shared::{SessionId, StreamType};
use o_sfu_router::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::{
    Channel, ChannelMediaCounts,
    state::{
        ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, TransportMediaRemoval,
        ValidatedPublishDescriptor,
    },
};
use crate::runtime::{
    ConnectionId,
    diagnostics::DiagnosticsEventData,
    telemetry::schema::event as telemetry_event,
    transport_adapter::{MediaPort, ObservabilityPort, TransportMediaId},
};

#[cfg(test)]
mod test_support;

/// Registry for publish transactions that reserved transport media but are not
/// live in room state yet
///
/// # Invariant
///
/// At most one `(session, connection, stream_type)` entry may be staged at a
/// time. The key includes the runtime-local connection id so stale replaced
/// sockets cannot share ownership with the current websocket for the same user
/// facing session id.
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
/// This uses the protocol session identity for room ownershio, the runtime
/// connection id for stale-socket rejection and the stream type for the
/// per-session media slot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingPublishKey {
    session_id: SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
}

/// Publish transaction that owns a reserved transport media line until the
/// channel either commits it or rolls it back.
///
/// The descriptor proves only that the session was publish-ready when staging
/// started. The reservation proves that the transport adapter allocated media
/// that must be accounted for. Keeping both values together prevents call sites
/// from committing channel state while forgetting the transport owner, or from
/// cleaning transport media while leaving a descriptor that can still commit.
#[derive(Debug)]
pub(super) struct PendingPublishTransaction {
    /// Stage-time channel validation. Commit must re-check it because
    /// replacement or disconnect can make the descriptor stale while transport
    /// work is in flight.
    descriptor: ValidatedPublishDescriptor,
    /// Transport media ownership while the publish is not yet a live producer.
    reservation: StagedMediaReservation,
}

/// Legal ownership states for one staged transport-media reservation.
///
/// These states are intentionally local to the transaction boundary. The
/// websocket layer sees publish intent and answer handling. `ChannelState` sees
/// only committed producers. The transport adapter sees media add or remove
/// calls. This enum records which layer is responsible for the reserved media
/// right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedMediaReservationState {
    /// The media line exists in the transport adapter but is not committed in
    /// chanel state.
    Reserved,
    /// The chanel committed the producer, so normal unpublish or leave cleanup
    /// owns the transport media from this point onward.
    Committed,
    /// The transaction made an explicit cleanup decision.
    ///
    /// This does not prove the transport adapter removed the handle
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
    /// Protocol-facing session identity used to rebuild the transport session
    /// key for cleanup.
    owner_session_id: SessionId,
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
/// Post-commit work for a publish that already became live in channel state.
///
/// This exists so the lock-protected state mutation stays small while the
/// follow-up effects still run in the right order after unlock
struct CommittedPublish {
    producer_id: String,
    media_counts_before: ChannelMediaCounts,
    media_counts_after: ChannelMediaCounts,
    consumer_targets: Vec<PendingConsumerBootstrapTarget>,
    diagnostics: DiagnosticsEventData,
}

impl PendingPublishTransactions {
    /// Returns weather this connection already has a staged publish for the
    /// stream.
    ///
    /// This is an idempotency check for websocket publish intents. It is only a
    /// snapshot; callers that reserve transport media must still call `stage`
    /// afterward to win the registry slot under the lock.
    pub(super) fn contains(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.staged.contains_key(&PendingPublishKey::new(
            session_id,
            connection_id,
            stream_type,
        ))
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
    /// The retirned transaction must be commited or cleaned up explicitly.
    /// This method is used by explicit unpublish before the answer lands.
    pub(super) fn take(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<PendingPublishTransaction> {
        self.staged.remove(&PendingPublishKey::new(
            session_id,
            connection_id,
            stream_type,
        ))
    }

    /// Drains every staged publish owned by one websocket connection.
    ///
    /// Conection cleanup and answered negotiation use this transfer so no
    /// later event can see the same staged reservation after cleanup or commit
    /// has started.
    pub(super) fn take_for_connection(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> Vec<PendingPublishTransaction> {
        let matching_keys = self
            .staged
            .keys()
            .filter(|key| key.session_id == *session_id && key.connection_id == connection_id)
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
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Self {
        Self {
            session_id: session_id.clone(),
            connection_id,
            stream_type,
        }
    }
}

impl PendingPublishTransaction {
    /// Creates a staged publish transaction from channel validation and a
    /// transport media reservation.
    ///
    /// The descriptor and reservation must describe the same owner. The
    /// constructor derives reservation ownership from the descriptor so callers
    /// cannot accidentally pair a media handle with a different connection.
    pub(super) fn new(
        descriptor: ValidatedPublishDescriptor,
        transport_media_id: TransportMediaId,
    ) -> Self {
        let owner_session_id = descriptor.owner_session_id().clone();
        let owner_connection_id = descriptor.owner_connection_id();
        Self {
            descriptor,
            reservation: StagedMediaReservation::new(
                owner_session_id,
                owner_connection_id,
                transport_media_id,
            ),
        }
    }

    fn key(&self) -> PendingPublishKey {
        PendingPublishKey::new(
            self.descriptor.owner_session_id(),
            self.descriptor.owner_connection_id(),
            self.descriptor.stream_type(),
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
    /// `Some` means the producer is now live and channel state owns the
    /// transport media. `None` means no producer was created and the
    /// reservation cleanup path was attempted.
    pub(super) async fn commit(
        self,
        channel: &Channel,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) -> Option<String> {
        let owner_session_id = self.descriptor.owner_session_id().clone();
        let owner_connection_id = self.descriptor.owner_connection_id();
        let stream_type = self.descriptor.stream_type();
        let transport_media_id = self.reservation.transport_media_id();
        let session_key = channel.transport_session_key(&owner_session_id, owner_connection_id);
        let negotiated_parameters = match media_port
            .negotiated_producer_parameters(&session_key, transport_media_id)
            .await
        {
            Ok(rtp_parameters) => rtp_parameters,
            Err(error) => {
                self.cleanup_reserved_media(
                    channel,
                    media_port,
                    "transport adapter failed to remove staged publish media after negotiated parameter lookup failed",
                )
                .await;
                warn!(
                    session_id = ?owner_session_id,
                    connection_id = ?owner_connection_id,
                    ?stream_type,
                    ?transport_media_id,
                    ?error,
                    "failed to load negotiated publish parameters during channel commit"
                );
                return None;
            }
        };
        self.commit_with_parameters(
            channel,
            observability_port,
            media_port,
            negotiated_parameters,
        )
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
        channel: &Channel,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<String> {
        let Self {
            descriptor,
            reservation,
        } = self;
        let owner_session_id = descriptor.owner_session_id().clone();
        let owner_connection_id = descriptor.owner_connection_id();
        let stream_type = descriptor.stream_type();
        let transport_media_id = reservation.transport_media_id();
        let committed_publish = {
            let mut state = channel.state.write().await;
            let media_counts_before = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            // The descriptor is consumed only at the final state commit. If the
            // session was replaced or lost publish readiness while transport
            // work was happening, `commit_published_track` rejects it and we
            // compensate by removing the reserved transport media below
            let prepared_track = descriptor.into_prepared_track(consumable_rtp_parameters);
            let consumer_targets = state.commit_published_track(prepared_track, transport_media_id);
            let media_counts_after = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            consumer_targets.map(|(producer_id, consumer_targets)| CommittedPublish {
                producer_id: producer_id.into_wire_id(),
                media_counts_before,
                media_counts_after,
                consumer_targets,
                diagnostics: DiagnosticsEventData::for_session(
                    channel.uuid(),
                    &owner_session_id,
                    telemetry_event::PUBLISH_COMMITTED,
                )
                .with_connection_id(owner_connection_id.as_u64())
                .with_media_worker_id(channel.media_worker_id())
                .with_transport_media_id(transport_media_id.as_u64()),
            })
        };
        let Some(committed_publish) = committed_publish else {
            reservation
                .cleanup(
                    channel,
                    media_port,
                    "transport adapter failed to remove published transport media after channel commit failed",
                )
                .await;
            warn!(
                session_id = ?owner_session_id,
                connection_id = ?owner_connection_id,
                stream_type = ?stream_type,
                transport_media_id = ?transport_media_id,
                "channel rejected staged negotiated publish during commit"
            );
            return None;
        };
        reservation.commit();
        let producer_id = committed_publish.producer_id.clone();
        committed_publish
            .finish(channel, observability_port, media_port)
            .await;
        Some(producer_id)
    }

    /// Consumes a staged publish that cannot become live.
    ///
    /// All rolback paths funnel through this method so the transport-media
    /// owner and the failure log context stay consistent
    async fn cleanup_reserved_media(
        self,
        channel: &Channel,
        media_port: &impl MediaPort,
        failure_message: &str,
    ) {
        self.reservation
            .cleanup(channel, media_port, failure_message)
            .await;
    }
}

impl StagedMediaReservation {
    /// Arms a reservation for transport media that is not yet live in channel
    /// state.
    fn new(
        owner_session_id: SessionId,
        owner_connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            owner_session_id,
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
    /// Cleanup is best-effort because the transport session may already be
    /// closing. The important ownership fact is that this transaction made the
    /// cleanup decision and must not be committed afterward.
    async fn cleanup(
        mut self,
        channel: &Channel,
        media_port: &impl MediaPort,
        failure_message: &str,
    ) {
        channel
            .cleanup_transport_media(
                &self.owner_session_id,
                self.owner_connection_id,
                self.transport_media_id,
                media_port,
                failure_message,
            )
            .await;
        self.state = StagedMediaReservationState::Released;
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
    /// channel state.
    ///
    /// Ordering matters:
    /// - metrics must observe the state delta that just happende
    /// - room-owned source policy must see the new producer before consumers
    ///   bootstrap against it
    /// - bootstrap and diagnostics happen last
    async fn finish(
        self,
        channel: &Channel,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        channel.record_media_count_delta(self.media_counts_before, self.media_counts_after);
        channel
            .sync_source_packet_selection_policy(Some(observability_port), media_port)
            .await;
        channel
            .bootstrap_consumer_targets(
                media_port,
                ConsumerBootstrapOrigin::Publish,
                self.consumer_targets,
            )
            .await;
        channel.diagnostics.record(self.diagnostics);
    }
}

impl Channel {
    /// Records the live media gauge delta after a chanel state transition.
    ///
    /// Callers pass both snapshots because the state lock should already be
    /// released by the time metrics and transport side effects run.
    pub(super) fn record_media_count_delta(
        &self,
        before: ChannelMediaCounts,
        after: ChannelMediaCounts,
    ) {
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
    pub(crate) async fn has_staged_publish(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.pending_publish_transactions.lock().await.contains(
            session_id,
            connection_id,
            stream_type,
        )
    }

    /// Validates the current room state and reserves transport media for a
    /// negotiated publish.
    ///
    /// The returned `true` means the publish is staged, not live. The caller
    /// must still drive renegotiation and later call `commit_staged_publishes`
    /// after the answer lands. The method avoids holding chanel state or
    /// pending-registry locks across the transport call.
    ///
    /// If another task stages the same stream during the transport await, this
    /// method consumes the duplicate reservation through cleanup and returns
    /// `false`
    pub(crate) async fn stage_negotiated_publish(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &impl MediaPort,
    ) -> bool {
        let media_kind = media_kind_for_stream_type(stream_type);
        let validated_descriptor = {
            let state = self.state.read().await;
            state.validate_publish_descriptor(session_id, connection_id, stream_type, media_kind)
        };
        let Some(validated_descriptor) = validated_descriptor else {
            return false;
        };
        // Cheap duplicate rejection goes first so we avoid reserving transport
        // media when the same stream is already staged.
        if self.pending_publish_transactions.lock().await.contains(
            session_id,
            connection_id,
            stream_type,
        ) {
            return false;
        }
        let session_key = self.transport_session_key(session_id, connection_id);
        let transport_media_id = match media_port
            .publish_media(&session_key, media_kind, &pending_publish_parameters())
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(_error) => {
                warn!(
                    ?session_id,
                    connection_id = ?connection_id,
                    ?stream_type,
                    "failed to stage negotiated publish stream"
                );
                return false;
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
            staged_publish
                .cleanup_reserved_media(
                    self,
                    media_port,
                    "transport adapter failed to remove duplicated staged publish media",
                )
                .await;
            return false;
        }
        true
    }

    /// Cancels one staged publish before it becomes a live producer.
    ///
    /// This is the explicit unpublish-before-answer path. `true` means a
    /// reservation existed and cleanup was attempted. `false` means the stream
    /// was not staged for this connection.
    pub(crate) async fn rollback_staged_publish(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &impl MediaPort,
    ) -> bool {
        // Explicit unpublish before commit only needs transport cleanup because
        // the producer never became live in channel state.
        let staged_publish = self.pending_publish_transactions.lock().await.take(
            session_id,
            connection_id,
            stream_type,
        );
        let Some(staged_publish) = staged_publish else {
            return false;
        };
        staged_publish
            .cleanup_reserved_media(
                self,
                media_port,
                "transport adapter failed to remove staged publish media during rollback",
            )
            .await;
        true
    }

    /// Cleans up every staged publish owned by a websocket connection.
    ///
    /// Session replacement, logical disconnect and websocket drop use this to
    /// drain all in-flight reservations before the connection can disapear.
    /// Cleanup remains best-effort because transport teardown may already be in
    /// progress
    pub(crate) async fn rollback_staged_publishes_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) {
        let staged_publishes = self
            .pending_publish_transactions
            .lock()
            .await
            .take_for_connection(session_id, connection_id);
        for staged_publish in staged_publishes {
            staged_publish
                .cleanup_reserved_media(
                    self,
                    media_port,
                    "transport adapter failed to remove staged publish media during connection cleanup",
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
        session_id: &SessionId,
        connection_id: ConnectionId,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        let staged_publishes = self
            .pending_publish_transactions
            .lock()
            .await
            .take_for_connection(session_id, connection_id);
        for staged_publish in staged_publishes {
            let _producer_id = staged_publish
                .commit(self, observability_port, media_port)
                .await;
        }
    }

    /// Releases a pending consumer-bootstrap reservation after the matching
    /// effect path no longer needs it.
    ///
    /// This mirrors staged-publish ownership on the subscriber side: channel
    /// state owns the reservation, while metrics are updated after unlock>
    pub(super) async fn release_pending_consumer_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) {
        let mut state = self.state.write().await;
        let media_counts_before = ChannelMediaCounts {
            publications: state.publication_count(),
            subscriptions: state.subscription_count(),
        };
        state.release_pending_consumer_bootstrap(target);
        let media_counts_after = ChannelMediaCounts {
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
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
        media_port: &impl MediaPort,
        failure_message: &str,
    ) {
        if media_port
            .remove_media(
                &self.transport_session_key(session_id, connection_id),
                transport_media_id,
            )
            .await
            .is_err()
        {
            warn!(
                ?session_id,
                connection_id = ?connection_id,
                ?transport_media_id,
                "{failure_message}"
            );
        }
    }

    /// Removes a batch of committed transport media where the caller needs to
    /// know whether every transport cleanup succeeded.
    ///
    /// Unlike staged publish rollback,this is used by transitions that already
    /// removed live channel state and need a strict transport outcome to decide
    /// weather the outer cleanup can keep going.
    pub(super) async fn cleanup_transport_removals_strict(
        &self,
        media_port: &impl MediaPort,
        removals: &[TransportMediaRemoval],
    ) -> bool {
        for removal in removals {
            if media_port
                .remove_media(
                    &self.transport_session_key(removal.session(), removal.connection()),
                    removal.transport_media(),
                )
                .await
                .is_err()
            {
                warn!(
                    session_id = ?removal.session(),
                    connection_id = ?removal.connection(),
                    transport_media_id = ?removal.transport_media(),
                    "transport adapter failed to remove transport media during channel cleanup"
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
                &self.transport_session_key(
                    target.consumer_session_id(),
                    target.consumer_connection_id(),
                ),
                consumer_transport_media_id,
                &self.transport_session_key(
                    target.producer_session_id(),
                    target.producer_connection_id(),
                ),
                target.transport_media_id(),
                false,
            )
            .await
            .is_err()
        {
            warn!(
                consumer_session_id = ?target.consumer_session_id(),
                producer_session_id = ?target.producer_session_id(),
                ?origin,
                "transport adapter failed to apply the initial consumer pause state"
            );
        }
    }
}

fn media_kind_for_stream_type(stream_type: StreamType) -> o_sfu_router::MediaKind {
    match stream_type {
        StreamType::Audio => o_sfu_router::MediaKind::Audio,
        StreamType::Camera | StreamType::Screen => o_sfu_router::MediaKind::Video,
    }
}

fn pending_publish_parameters() -> RouterRtpParameters {
    RouterRtpParameters::new(vec![], vec![], vec![])
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::shared::SessionId;

    use super::StagedMediaReservation;
    use crate::runtime::{ConnectionId, transport_adapter::TransportMediaId};

    #[test]
    #[should_panic(expected = "staged media reservation dropped while still reserved")]
    fn reserved_staged_media_reservation_panics_when_dropped_in_tests() {
        let _reservation = StagedMediaReservation::new(
            SessionId::Integer(1),
            ConnectionId::from_raw(1),
            TransportMediaId::new(1),
        );
    }

    #[test]
    fn committed_staged_media_reservation_can_drop_in_tests() {
        StagedMediaReservation::new(
            SessionId::Integer(1),
            ConnectionId::from_raw(1),
            TransportMediaId::new(1),
        )
        .commit();
    }
}
