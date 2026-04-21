use std::collections::BTreeMap;

use tracing::warn;

use crate::runtime::ConnectionId;
use crate::runtime::diagnostics::DiagnosticsEventData;
use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::transport_adapter::{
    RuntimeTransportAdapter, TransportAdapterError, TransportMediaId,
};
use o_sfu_protocol::shared::{SessionId, StreamType};
use o_sfu_router::MediaStream as RouterRtpParameters;

use super::{
    Channel, ChannelMediaCounts, SessionOutbound,
    effects::StagedPublishCommitEffectPlan,
    state::{
        ConsumerBootstrapOrigin, PendingConsumerBootstrap, PendingConsumerBootstrapTarget,
        PreparedConsumerBootstrap, PreparedPublishedTrack, TransportMediaRemoval,
        ValidatedPublishDescriptor,
    },
};

#[cfg(test)]
mod test_support;

#[derive(Debug, Default)]
pub(super) struct PendingPublishTransactions {
    staged: BTreeMap<PendingPublishKey, StagedPublishTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingPublishKey {
    session_id: SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
}

#[derive(Debug, Clone)]
pub(super) struct StagedPublishTransaction {
    descriptor: ValidatedPublishDescriptor,
    transport_media_id: TransportMediaId,
}

#[derive(Debug)]
pub(super) struct PublishCommitSnapshot {
    session_id: SessionId,
    connection_id: ConnectionId,
    prepared_track: PreparedPublishedTrack,
    transport_media_id: TransportMediaId,
}

#[derive(Debug)]
pub(super) enum StagedPublishCommitOutcome {
    Committed(String),
    LoadParametersFailed(TransportAdapterError),
    PublishRejected,
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

    pub(super) fn insert(&mut self, staged_publish: StagedPublishTransaction) {
        self.staged.insert(staged_publish.key(), staged_publish);
    }

    pub(super) fn take(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<StagedPublishTransaction> {
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
    ) -> Vec<StagedPublishTransaction> {
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

impl StagedPublishTransaction {
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

    pub(super) fn into_commit_snapshot(
        self,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> PublishCommitSnapshot {
        PublishCommitSnapshot {
            session_id: self.descriptor.owner_session_id().clone(),
            connection_id: self.descriptor.owner_connection_id(),
            prepared_track: self
                .descriptor
                .into_prepared_track(consumable_rtp_parameters),
            transport_media_id: self.transport_media_id,
        }
    }
}

impl PublishCommitSnapshot {
    pub(super) async fn commit(
        self,
        channel: &Channel,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let effect_plan = {
            let mut state = channel.state.write().await;
            let media_counts_before = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let consumer_targets =
                state.commit_published_track(self.prepared_track, self.transport_media_id);
            let media_counts_after = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            match consumer_targets {
                Some((producer_id, consumer_targets)) => StagedPublishCommitEffectPlan::committed(
                    producer_id.into_wire_id(),
                    (media_counts_before, media_counts_after),
                    consumer_targets,
                    DiagnosticsEventData::for_session(
                        channel.uuid(),
                        &self.session_id,
                        telemetry_event::PUBLISH_COMMITTED,
                    )
                    .with_connection_id(self.connection_id.as_u64())
                    .with_media_worker_id(channel.media_worker_id())
                    .with_transport_media_id(self.transport_media_id.as_u64()),
                ),
                None => StagedPublishCommitEffectPlan::rejected(
                    self.session_id.clone(),
                    self.connection_id,
                    self.transport_media_id,
                ),
            }
        };
        effect_plan.execute(channel, transport_adapter).await
    }
}

#[derive(Debug)]
pub(super) struct ConsumerBootstrapTransaction {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
}

impl ConsumerBootstrapTransaction {
    async fn declare_consumer_transport_media(
        &self,
        channel: &Channel,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let consumer_session_key = channel.transport_session_key(
            self.target.consumer_session_id(),
            self.target.consumer_connection_id(),
        );
        let consumer_transport_media_id = match transport_adapter
            .consume_media(
                &consumer_session_key,
                self.target.media_kind(),
                &channel.transport_session_key(
                    self.target.producer_session_id(),
                    self.target.producer_connection_id(),
                ),
                self.target.transport_media_id(),
                self.prepared.consumer_rtp_parameters(),
            )
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(error) => {
                channel
                    .release_pending_consumer_bootstrap(&self.target)
                    .await;
                warn!(
                    consumer_session_id = ?self.target.consumer_session_id(),
                    consumer_connection_id = ?self.target.consumer_connection_id(),
                    producer_session_id = ?self.target.producer_session_id(),
                    producer_connection_id = ?self.target.producer_connection_id(),
                    source_transport_media_id = ?self.target.transport_media_id(),
                    error = ?error,
                    consumer_mid = self.prepared.consumer_rtp_parameters().mid(),
                    ?origin,
                    "transport adapter rejected consume media declaration"
                );
                return None;
            }
        };
        let consumer_mid = transport_adapter
            .transport_media_mid(&consumer_session_key, consumer_transport_media_id)
            .await;
        Some((consumer_transport_media_id, consumer_mid))
    }

    async fn execute(
        self,
        channel: &Channel,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        let Some((consumer_transport_media_id, consumer_mid)) = self
            .declare_consumer_transport_media(channel, transport_adapter, origin)
            .await
        else {
            return;
        };
        let (media_counts_before, outbound, media_counts_after) = {
            let mut state = channel.state.write().await;
            let media_counts_before = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let outbound = state.commit_consumer_bootstrap(
                &self.target,
                self.pending_bootstrap,
                consumer_transport_media_id,
                consumer_mid,
            );
            let media_counts_after = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            (media_counts_before, outbound, media_counts_after)
        };
        let Some((sender, bootstrap, consumer_active)) = outbound else {
            channel.record_media_count_delta(media_counts_before, media_counts_after);
            channel
                .cleanup_transport_media(
                    self.target.consumer_session_id(),
                    self.target.consumer_connection_id(),
                    consumer_transport_media_id,
                    transport_adapter,
                    "transport adapter failed to remove consumer transport media after bootstrap state commit failed",
                )
                .await;
            return;
        };
        channel.record_media_count_delta(media_counts_before, media_counts_after);
        channel
            .apply_initial_consumer_pause_state(
                &self.target,
                consumer_transport_media_id,
                consumer_active,
                transport_adapter,
                origin,
            )
            .await;
        channel.diagnostics.record(
            DiagnosticsEventData::for_session(
                channel.uuid(),
                self.target.consumer_session_id(),
                telemetry_event::SUBSCRIBE_SUCCEEDED,
            )
            .with_connection_id(self.target.consumer_connection_id().as_u64())
            .with_media_worker_id(channel.media_worker_id())
            .with_transport_media_id(consumer_transport_media_id.as_u64())
            .insert_field(
                "producer_session_id",
                serde_json::to_value(self.target.producer_session_id())
                    .unwrap_or(serde_json::Value::Null),
            )
            .insert_field(
                "source_transport_media_id",
                self.target.transport_media_id().as_u64(),
            )
            .insert_field(
                "stream_type",
                format!("{:?}", self.target.stream_type()).to_lowercase(),
            ),
        );
        let _ = sender.send(SessionOutbound::Request(Box::new(
            bootstrap.into_channel_event_request(),
        )));
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
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let media_kind = media_kind_for_stream_type(stream_type);
        let validated_descriptor = {
            let state = self.state.read().await;
            state.validate_publish_descriptor(session_id, connection_id, stream_type, media_kind)
        };
        let Some(validated_descriptor) = validated_descriptor else {
            return false;
        };
        if self.pending_publish_transactions.lock().await.contains(
            session_id,
            connection_id,
            stream_type,
        ) {
            return false;
        }
        let session_key = self.transport_session_key(session_id, connection_id);
        let transport_media_id = match transport_adapter
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
        let duplicate_stage = {
            let mut pending_publish_transactions = self.pending_publish_transactions.lock().await;
            if pending_publish_transactions.contains(session_id, connection_id, stream_type) {
                true
            } else {
                pending_publish_transactions.insert(StagedPublishTransaction::new(
                    validated_descriptor,
                    transport_media_id,
                ));
                false
            }
        };
        if duplicate_stage {
            self.cleanup_transport_media(
                session_id,
                connection_id,
                transport_media_id,
                transport_adapter,
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
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
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
            transport_adapter,
            "transport adapter failed to remove staged publish media during rollback",
        )
        .await;
        true
    }

    pub(crate) async fn rollback_staged_publishes_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
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
                transport_adapter,
                "transport adapter failed to remove staged publish media during connection cleanup",
            )
            .await;
        }
    }

    pub(crate) async fn commit_staged_publishes(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let staged_publishes = self
            .pending_publish_transactions
            .lock()
            .await
            .take_for_connection(session_id, connection_id);
        let session_key = self.transport_session_key(session_id, connection_id);
        for staged_publish in staged_publishes {
            let stream_type = staged_publish.descriptor.stream_type();
            let transport_media_id = staged_publish.transport_media_id();
            let commit_outcome = match transport_adapter
                .negotiated_producer_parameters(&session_key, transport_media_id)
                .await
            {
                Ok(rtp_parameters) => staged_publish
                    .into_commit_snapshot(rtp_parameters)
                    .commit(self, transport_adapter)
                    .await
                    .map_or(
                        StagedPublishCommitOutcome::PublishRejected,
                        StagedPublishCommitOutcome::Committed,
                    ),
                Err(error) => {
                    self.cleanup_transport_media(
                        session_id,
                        connection_id,
                        transport_media_id,
                        transport_adapter,
                        "transport adapter failed to remove staged publish media after negotiated parameter lookup failed",
                    )
                    .await;
                    StagedPublishCommitOutcome::LoadParametersFailed(error)
                }
            };
            match commit_outcome {
                StagedPublishCommitOutcome::Committed(_producer_id) => {}
                StagedPublishCommitOutcome::LoadParametersFailed(error) => {
                    warn!(
                        ?session_id,
                        connection_id = ?connection_id,
                        ?stream_type,
                        ?transport_media_id,
                        ?error,
                        "failed to load negotiated publish parameters during channel commit"
                    );
                }
                StagedPublishCommitOutcome::PublishRejected => {
                    warn!(
                        ?session_id,
                        connection_id = ?connection_id,
                        ?stream_type,
                        ?transport_media_id,
                        "channel rejected staged negotiated publish during commit"
                    );
                }
            }
        }
    }

    pub(super) async fn prepare_consumer_bootstrap_transaction(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<ConsumerBootstrapTransaction> {
        let prepared = {
            let state = self.state.read().await;
            state.prepare_consumer_bootstrap(target)?
        };
        let pending_bootstrap = {
            let mut state = self.state.write().await;
            let media_counts_before = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let pending_bootstrap =
                state.prepare_consumer_bootstrap_transaction(target, &prepared)?;
            let media_counts_after = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            self.record_media_count_delta(media_counts_before, media_counts_after);
            pending_bootstrap
        };
        Some(ConsumerBootstrapTransaction {
            target: target.clone(),
            prepared,
            pending_bootstrap,
        })
    }

    pub(super) async fn execute_consumer_bootstrap(
        &self,
        target: PendingConsumerBootstrapTarget,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        let Some(transaction) = self.prepare_consumer_bootstrap_transaction(&target).await else {
            return;
        };
        transaction.execute(self, transport_adapter, origin).await;
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
        transport_adapter: &RuntimeTransportAdapter,
        failure_message: &str,
    ) {
        if transport_adapter
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
        transport_adapter: &RuntimeTransportAdapter,
        removals: &[TransportMediaRemoval],
    ) -> bool {
        for removal in removals {
            if transport_adapter
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

    async fn apply_initial_consumer_pause_state(
        &self,
        target: &PendingConsumerBootstrapTarget,
        consumer_transport_media_id: TransportMediaId,
        consumer_active: bool,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        if consumer_active {
            return;
        }
        if transport_adapter
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
