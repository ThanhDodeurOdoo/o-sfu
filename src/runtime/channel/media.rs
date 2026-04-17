use o_sfu_router::{
    MediaKind, RtpParameters as RouterRtpParameters, derive_consumable_rtp_parameters,
};
use tracing::warn;

use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportMediaId};
use crate::signaling::shared::{DownloadStates, SessionId, StreamType};

use super::{
    Channel,
    media_transaction::{PublishTransaction, UnpublishTransaction},
    state::{ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget},
};

#[derive(Debug, Clone)]
pub(crate) struct NegotiatedPublish {
    pub(crate) connection_id: u64,
    pub(crate) stream_type: StreamType,
    pub(crate) media_kind: MediaKind,
    pub(crate) transport_media_id: TransportMediaId,
    pub(crate) consumable_rtp_parameters: o_sfu_router::RtpParameters,
}

impl Channel {
    pub async fn publish_negotiated_track(
        &self,
        session_id: &SessionId,
        publish: NegotiatedPublish,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let pending_publish = {
            let state = self.state.read().await;
            state.prepare_published_track(
                session_id,
                publish.connection_id,
                publish.stream_type,
                publish.media_kind,
                publish.consumable_rtp_parameters,
            )?
        };
        PublishTransaction::new(session_id.clone(), publish.connection_id, pending_publish)
            .commit(self, publish.transport_media_id, transport_adapter)
            .await
    }

    #[cfg(test)]
    pub async fn bootstrap_missing_consumers(
        &self,
        session_id: &SessionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let targets = {
            let state = self.state.read().await;
            state.missing_consumer_targets(session_id)
        };
        self.bootstrap_consumer_targets(
            targets,
            transport_adapter,
            ConsumerBootstrapOrigin::LateJoin,
        )
        .await;
    }

    pub(crate) async fn bootstrap_missing_consumers_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some(targets) = ({
            let state = self.state.read().await;
            state.missing_consumer_targets_for_connection(session_id, connection_id)
        }) else {
            return false;
        };
        self.bootstrap_consumer_targets(
            targets,
            transport_adapter,
            ConsumerBootstrapOrigin::LateJoin,
        )
        .await;
        true
    }

    async fn bootstrap_consumer_targets(
        &self,
        targets: Vec<PendingConsumerBootstrapTarget>,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        for target in targets {
            self.execute_consumer_bootstrap(target, transport_adapter, origin)
                .await;
        }
    }
    pub async fn publish_track(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        media_kind: MediaKind,
        producer_rtp_parameters: RouterRtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let publish_prerequisites = {
            let state = self.state.read().await;
            state.publish_prerequisites(session_id)?
        };
        let publisher_connection_id = publish_prerequisites.connection_id();
        let router_capabilities = publish_prerequisites.router_capabilities();

        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&producer_rtp_parameters, &router_capabilities)
                .map_err(|error| {
                    warn!(
                        ?session_id,
                        ?error,
                        "failed to derive consumable RTP parameters for producer"
                    );
                })
                .ok()?;

        let pending_publish = {
            let state = self.state.read().await;
            state.prepare_published_track(
                session_id,
                publisher_connection_id,
                stream_type,
                media_kind,
                consumable_rtp_parameters,
            )?
        };
        let transport_media_id = match transport_adapter
            .publish_media(
                &self.transport_session_key(session_id, publisher_connection_id),
                media_kind,
                &producer_rtp_parameters,
            )
            .await
        {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    ?session_id,
                    connection_id = publisher_connection_id,
                    ?stream_type,
                    "transport adapter rejected publish media declaration"
                );
                return None;
            }
        };

        PublishTransaction::new(session_id.clone(), publisher_connection_id, pending_publish)
            .commit(self, transport_media_id, transport_adapter)
            .await
    }

    #[allow(
        clippy::cognitive_complexity,
        reason = "the production-change transition intentionally keeps router updates, session-info sync, broadcast, and transport activity in one explicit sequence"
    )]
    pub(crate) async fn set_publication_active_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
        active: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(producer_target) = ({
            let state = self.state.read().await;
            state.producer_route_target(session_id, connection_id, stream_type)
        }) else {
            return;
        };
        let transport_session_key =
            self.transport_session_key(session_id, producer_target.owner_connection_id());
        let Some(outcome) = ({
            let mut state = self.state.write().await;
            state.apply_producer_activity(session_id, &producer_target, stream_type, active)
        }) else {
            return;
        };
        if transport_adapter
            .set_producer_active(
                &transport_session_key,
                outcome.transport_media_id,
                outcome.active,
            )
            .await
            .is_err()
        {
            warn!(
                ?session_id,
                ?stream_type,
                active = outcome.active,
                "transport adapter failed to update producer route activity"
            );
        }
        outcome.fanout.emit();
    }

    #[cfg(test)]
    pub async fn set_publication_active(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        active: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            return;
        };
        self.set_publication_active_runtime(
            session_id,
            connection_id,
            stream_type,
            active,
            transport_adapter,
        )
        .await;
    }

    pub(crate) async fn update_subscription_runtime(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        target_session_id: &SessionId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let committed_updates = {
            let mut state = self.state.write().await;
            state.apply_download_state_update(session_id, connection_id, target_session_id, states)
        };
        for route_update in committed_updates {
            if transport_adapter
                .set_consumer_active(
                    &self.transport_session_key(session_id, route_update.consumer_connection_id()),
                    route_update.consumer_media(),
                    &self.transport_session_key(
                        target_session_id,
                        route_update.source_connection_id(),
                    ),
                    route_update.source_media(),
                    route_update.active(),
                )
                .await
                .is_err()
            {
                warn!(
                    ?session_id,
                    ?target_session_id,
                    stream_type = ?route_update.stream_type(),
                    active = route_update.active(),
                    "transport adapter failed to update consumer route activity"
                );
            }
        }
    }

    #[cfg(test)]
    pub async fn update_subscription(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            return;
        };
        self.update_subscription_runtime(
            session_id,
            connection_id,
            target_session_id,
            states,
            transport_adapter,
        )
        .await;
    }

    pub(crate) async fn is_stream_published(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
    ) -> bool {
        self.state
            .read()
            .await
            .producer_route_target_for_session(session_id, stream_type)
            .is_some()
    }

    pub(crate) async fn unpublish_track(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some(transport_removals) = ({
            let state = self.state.read().await;
            state.unpublish_transport_removals(session_id, connection_id, stream_type)
        }) else {
            return false;
        };
        UnpublishTransaction::new(
            session_id.clone(),
            connection_id,
            stream_type,
            transport_removals,
        )
        .commit(self, transport_adapter)
        .await
    }
}
