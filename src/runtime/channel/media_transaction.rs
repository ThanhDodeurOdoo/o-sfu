//! Channel-owned transaction helpers for staged publish and transport cleanup
//!
//! The staged publish path has three phases:
//! - validate the current session and reserve transport media
//! - keep that reserved media in `PendingPublishTransactions` while the
//!   offer/answer round-trip is still in flight
//! - commit or clean it up once the answer made the final producer parameters
//!   available
//!
//! The important boundary is that transport work happens outside the channel
//! lock, but the full staged publish lifecycle still has one owner.

use std::collections::BTreeMap;

use tracing::warn;

use crate::runtime::ConnectionId;
use crate::runtime::diagnostics::DiagnosticsEventData;
use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::transport_adapter::{MediaPort, ObservabilityPort, TransportMediaId};
use o_sfu_protocol::shared::{SessionId, StreamType};
use o_sfu_router::MediaStream as RouterRtpParameters;

use super::{
    Channel, ChannelMediaCounts,
    state::{
        ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, TransportMediaRemoval,
        ValidatedPublishDescriptor,
    },
};

#[cfg(test)]
mod test_support;

/// Connection-local registry for staged publishes that are waiting on the
/// negotiation answer
///
/// The key invariant is that one `(session, connection, stream_type)` can own
/// at most one staged publish. That prevent racing publish intents from leaking
/// multiple reserved transport-media handles for the same stream.
#[derive(Debug, Default)]
pub(super) struct PendingPublishTransactions {
    staged: BTreeMap<PendingPublishKey, PendingPublishTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingPublishKey {
    session_id: SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
}

#[derive(Debug, Clone)]
/// Staged publish that already owns reserved transport media but is not live in
/// channel state yet
///
/// This type is the authoritative staged publish lifecycle owner. Callers stage
/// it before renegotiation, then either commit it after the answer lands or
/// clean it up when the publish is cancelled or the connection dies.
pub(super) struct PendingPublishTransaction {
    descriptor: ValidatedPublishDescriptor,
    transport_media_id: TransportMediaId,
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
    /// Duplicate rejection is what keeps racing publish intents from leaving
    /// multiple staged transport-media handles behind for one stream.
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
    pub(super) fn new(
        descriptor: ValidatedPublishDescriptor,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            descriptor,
            transport_media_id,
        }
    }

    fn key(&self) -> PendingPublishKey {
        PendingPublishKey::new(
            self.descriptor.owner_session_id(),
            self.descriptor.owner_connection_id(),
            self.descriptor.stream_type(),
        )
    }

    pub(super) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    /// Finish a staged publish through the real transport-facing commit path.
    ///
    /// The websocket layer calls this only after the answer landed. If the
    /// transport layer cannot surface the final negotiated producer
    /// parameters, the transaction cleans up its reserved media here because
    /// there is nothing useful left to commit.
    pub(super) async fn commit(
        self,
        channel: &Channel,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) -> Option<String> {
        let session_key = channel.transport_session_key(
            self.descriptor.owner_session_id(),
            self.descriptor.owner_connection_id(),
        );
        let negotiated_parameters = match media_port
            .negotiated_producer_parameters(&session_key, self.transport_media_id)
            .await
        {
            Ok(rtp_parameters) => rtp_parameters,
            Err(error) => {
                self.cleanup(
                    channel,
                    media_port,
                    "transport adapter failed to remove staged publish media after negotiated parameter lookup failed",
                )
                .await;
                warn!(
                    session_id = ?self.descriptor.owner_session_id(),
                    connection_id = ?self.descriptor.owner_connection_id(),
                    stream_type = ?self.descriptor.stream_type(),
                    transport_media_id = ?self.transport_media_id,
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

    pub(super) async fn commit_with_parameters(
        self,
        channel: &Channel,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<String> {
        let owner_session_id = self.descriptor.owner_session_id().clone();
        let owner_connection_id = self.descriptor.owner_connection_id();
        let stream_type = self.descriptor.stream_type();
        let transport_media_id = self.transport_media_id;
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
            let prepared_track = self
                .descriptor
                .into_prepared_track(consumable_rtp_parameters);
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
            channel
                .cleanup_transport_media(
                    &owner_session_id,
                    owner_connection_id,
                    transport_media_id,
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
        let producer_id = committed_publish.producer_id.clone();
        committed_publish
            .finish(channel, observability_port, media_port)
            .await;
        Some(producer_id)
    }

    /// Remove the reserved transport media when the staged publish cannot
    /// complete.
    ///
    /// Kepping cleanup on the transaction makes all failure paths use the same
    /// owner session and transport-media handle.
    async fn cleanup(&self, channel: &Channel, media_port: &impl MediaPort, failure_message: &str) {
        channel
            .cleanup_transport_media(
                self.descriptor.owner_session_id(),
                self.descriptor.owner_connection_id(),
                self.transport_media_id,
                media_port,
                failure_message,
            )
            .await;
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
        // The transport await above laeves a race window where another publish
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
                .is_err()
        };
        if duplicate_stage {
            self.cleanup_transport_media(
                session_id,
                connection_id,
                transport_media_id,
                media_port,
                "transport adapter failed to remove duplicated staged publish media",
            )
            .await;
            return false;
        }
        true
    }

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
        self.cleanup_transport_media(
            session_id,
            connection_id,
            staged_publish.transport_media_id(),
            media_port,
            "transport adapter failed to remove staged publish media during rollback",
        )
        .await;
        true
    }

    pub(crate) async fn rollback_staged_publishes_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) {
        // Connection teardown drains every staged publish for that connection in
        // one place so websocket drop, replacement and bulk cleanup cannot
        // leave reserved producer media behind.
        let staged_publishes = self
            .pending_publish_transactions
            .lock()
            .await
            .take_for_connection(session_id, connection_id);
        for staged_publish in staged_publishes {
            self.cleanup_transport_media(
                session_id,
                connection_id,
                staged_publish.transport_media_id(),
                media_port,
                "transport adapter failed to remove staged publish media during connection cleanup",
            )
            .await;
        }
    }

    pub(crate) async fn commit_staged_publishes(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        // Drain first so a later renegotiation cannot re-commit the same staged
        // publish if more messages arrive while we are finishing this batch.
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
