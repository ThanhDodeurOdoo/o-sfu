use tracing::warn;

use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportMediaId};
use crate::signaling::shared::{SessionId, StreamType};

use super::{
    Channel, SessionOutbound,
    state::{
        ConsumerBootstrapOrigin, PendingConsumerBootstrap, PendingConsumerBootstrapTarget,
        PreparedConsumerBootstrap, PreparedPublishedTrack, TransportMediaRemoval,
    },
};

#[derive(Debug)]
pub(super) struct PublishTransaction {
    session_id: SessionId,
    connection_id: u64,
    pending_publish: PreparedPublishedTrack,
}

impl PublishTransaction {
    pub(super) fn new(
        session_id: SessionId,
        connection_id: u64,
        pending_publish: PreparedPublishedTrack,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            pending_publish,
        }
    }

    pub(super) async fn commit(
        self,
        channel: &Channel,
        transport_media_id: TransportMediaId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let consumer_targets = {
            let mut state = channel.state.write().await;
            state.commit_published_track(self.pending_publish, transport_media_id)
        };
        let Some((producer_id, consumer_targets)) = consumer_targets else {
            channel
                .cleanup_transport_media(
                    &self.session_id,
                    self.connection_id,
                    transport_media_id,
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
            state.unpublish_track(
                &self.session_id,
                self.connection_id,
                self.stream_type,
                self.transport_removals,
            )
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
