use std::collections::BTreeMap;

use tracing::warn;

use crate::runtime::transport_adapter::{
    RuntimeTransportAdapter, TransportAdapterError, TransportMediaId,
};
use o_sfu_protocol::shared::{SessionId, StreamType};
use o_sfu_router::RtpParameters as RouterRtpParameters;

use super::{
    Channel, SessionOutbound,
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
    connection_id: u64,
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
    connection_id: u64,
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
        connection_id: u64,
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
        connection_id: u64,
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
        connection_id: u64,
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
    pub(super) fn new(session_id: &SessionId, connection_id: u64, stream_type: StreamType) -> Self {
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
        let consumer_targets = {
            let mut state = channel.state.write().await;
            state.commit_published_track(self.prepared_track, self.transport_media_id)
        };
        let Some((producer_id, consumer_targets)) = consumer_targets else {
            channel
                .cleanup_transport_media(
                    &self.session_id,
                    self.connection_id,
                    self.transport_media_id,
                    transport_adapter,
                    "transport adapter failed to remove published transport media after channel commit failed",
                )
                .await;
            return None;
        };
        channel
            .sync_source_packet_selection_policy(Some(transport_adapter))
            .await;
        for target in consumer_targets {
            channel
                .execute_consumer_bootstrap(
                    target,
                    transport_adapter,
                    ConsumerBootstrapOrigin::Publish,
                )
                .await;
        }
        Some(producer_id.into_wire_id())
    }
}

#[derive(Debug)]
pub(super) struct ConsumerBootstrapTransaction {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
}

impl ConsumerBootstrapTransaction {
    async fn execute(
        self,
        channel: &Channel,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
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
                    consumer_connection_id = self.target.consumer_connection_id(),
                    producer_session_id = ?self.target.producer_session_id(),
                    producer_connection_id = self.target.producer_connection_id(),
                    source_transport_media_id = ?self.target.transport_media_id(),
                    error = ?error,
                    consumer_mid = self.prepared.consumer_rtp_parameters().mid(),
                    ?origin,
                    "transport adapter rejected consume media declaration"
                );
                return;
            }
        };
        let consumer_mid = transport_adapter
            .transport_media_mid(&consumer_session_key, consumer_transport_media_id)
            .await;
        let outbound = {
            let mut state = channel.state.write().await;
            state.commit_consumer_bootstrap(
                &self.target,
                self.pending_bootstrap,
                consumer_transport_media_id,
                consumer_mid,
            )
        };
        let Some((sender, bootstrap, consumer_active)) = outbound else {
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
        channel
            .apply_initial_consumer_pause_state(
                &self.target,
                consumer_transport_media_id,
                consumer_active,
                transport_adapter,
                origin,
            )
            .await;
        let _ = sender.send(SessionOutbound::Request(Box::new(
            bootstrap.into_channel_event_request(),
        )));
    }
}

#[derive(Debug)]
pub(super) struct UnpublishTransaction {
    session_id: SessionId,
    connection_id: u64,
    stream_type: StreamType,
    transport_removals: Vec<TransportMediaRemoval>,
}

impl UnpublishTransaction {
    pub(super) fn new(
        session_id: SessionId,
        connection_id: u64,
        stream_type: StreamType,
        transport_removals: Vec<TransportMediaRemoval>,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            stream_type,
            transport_removals,
        }
    }

    pub(super) async fn commit(
        self,
        channel: &Channel,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        if !channel
            .cleanup_transport_removals_strict(transport_adapter, &self.transport_removals)
            .await
        {
            return false;
        }
        let Some(outcome) = ({
            let mut state = channel.state.write().await;
            state.unpublish_track(&self.session_id, self.connection_id, self.stream_type)
        }) else {
            warn!(
                session_id = ?self.session_id,
                connection_id = self.connection_id,
                stream_type = ?self.stream_type,
                "transport cleanup succeeded but channel state commit failed"
            );
            return false;
        };
        outcome.emit(&self.session_id, self.stream_type);
        true
    }
}

impl Channel {
    pub(crate) async fn has_staged_publish(
        &self,
        session_id: &SessionId,
        connection_id: u64,
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
        connection_id: u64,
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
                    connection_id,
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
        connection_id: u64,
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
        connection_id: u64,
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
        connection_id: u64,
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
                        connection_id,
                        ?stream_type,
                        ?transport_media_id,
                        ?error,
                        "failed to load negotiated publish parameters during channel commit"
                    );
                }
                StagedPublishCommitOutcome::PublishRejected => {
                    warn!(
                        ?session_id,
                        connection_id,
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
            state.prepare_consumer_bootstrap_transaction(target, &prepared)?
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
        state.release_pending_consumer_bootstrap(target);
    }

    pub(super) async fn cleanup_transport_media(
        &self,
        session_id: &SessionId,
        connection_id: u64,
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
                connection_id,
                ?transport_media_id,
                "{failure_message}"
            );
        }
    }

    async fn cleanup_transport_removals_strict(
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
                    connection_id = removal.connection(),
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
