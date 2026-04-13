use o_sfu_router::derive_consumable_rtp_parameters;
use tracing::warn;

use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportMediaId};
use crate::signaling::{
    ortc_mapper,
    shared::{DownloadStates, SessionId, StreamType},
    webrtc::{MediaKind as SignalingMediaKind, RtpParameters},
};

use super::{
    Channel, TransportCleanupMode,
    state::{ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget, PendingPublishedTrack},
};

#[derive(Debug, Clone)]
pub(crate) struct NegotiatedPublish {
    pub(crate) connection_id: u64,
    pub(crate) stream_type: StreamType,
    pub(crate) media_kind: SignalingMediaKind,
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
            let mut state = self.state.write().await;
            state.prepare_published_track(
                session_id,
                publish.connection_id,
                publish.stream_type,
                publish.media_kind,
                publish.consumable_rtp_parameters,
            )?
        };
        self.commit_published_track(
            session_id,
            publish.connection_id,
            pending_publish,
            publish.transport_media_id,
            transport_adapter,
        )
        .await
    }

    pub async fn bootstrap_missing_consumers(
        &self,
        session_id: &SessionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let targets = {
            let state = self.state.read().await;
            state.missing_consumer_targets(session_id)
        };

        for target in targets {
            self.bootstrap_consumer_target(
                &target,
                transport_adapter,
                ConsumerBootstrapOrigin::LateJoin,
            )
            .await;
        }
    }

    pub async fn publish_track(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        rtp_parameters: RtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let publish_prerequisites = {
            let state = self.state.read().await;
            state.publish_prerequisites(session_id)?
        };
        let publisher_connection_id = publish_prerequisites.connection_id();
        let router_capabilities = publish_prerequisites.router_capabilities();

        let parsed_rtp_parameters =
            ortc_mapper::parse_rtp_parameters(&rtp_parameters.0).or_else(|| {
                warn!(
                    ?session_id,
                    "failed to parse producer RTP parameters from wire format"
                );
                None
            })?;
        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&parsed_rtp_parameters, &router_capabilities)
                .map_err(|error| {
                    warn!(
                        ?session_id,
                        ?error,
                        "failed to derive consumable RTP parameters for producer"
                    );
                })
                .ok()?;

        let pending_publish = {
            let mut state = self.state.write().await;
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
                &parsed_rtp_parameters,
            )
            .await
        {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    ?session_id,
                    "transport adapter rejected publish media declaration"
                );
                return None;
            }
        };

        self.commit_published_track(
            session_id,
            publisher_connection_id,
            pending_publish,
            transport_media_id,
            transport_adapter,
        )
        .await
    }

    async fn bootstrap_consumer_target(
        &self,
        target: &PendingConsumerBootstrapTarget,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        let Some(prepared) = ({
            let state = self.state.read().await;
            state.prepare_consumer_bootstrap(target)
        }) else {
            return;
        };
        let Some(pending_bootstrap) = ({
            let mut state = self.state.write().await;
            state.prepare_consumer_bootstrap_transaction(target, &prepared)
        }) else {
            return;
        };
        let consumer_transport_media_id = match transport_adapter
            .consume_media(
                &self.transport_session_key(
                    target.consumer_session_id(),
                    target.consumer_connection_id(),
                ),
                target.media_kind(),
                &self.transport_session_key(
                    target.producer_session_id(),
                    target.producer_connection_id(),
                ),
                target.transport_media_id(),
                prepared.consumer_rtp_parameters(),
            )
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id(),
                    producer_session_id = ?target.producer_session_id(),
                    ?origin,
                    "transport adapter rejected consume media declaration"
                );
                return;
            }
        };
        let outbound = {
            let mut state = self.state.write().await;
            state.commit_consumer_bootstrap(target, pending_bootstrap, consumer_transport_media_id)
        };
        let Some((sender, bootstrap)) = outbound else {
            let _result = transport_adapter
                .remove_media(
                    &self.transport_session_key(
                        target.consumer_session_id(),
                        target.consumer_connection_id(),
                    ),
                    consumer_transport_media_id,
                )
                .await;
            return;
        };
        let _ = sender.send(super::SessionOutbound::Request(Box::new(
            bootstrap.into_current_server_request(),
        )));
    }

    #[allow(
        clippy::cognitive_complexity,
        reason = "the production-change transition intentionally keeps router updates, session-info sync, broadcast, and transport activity in one explicit sequence"
    )]
    pub async fn update_upload_state(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        active: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(producer_target) = ({
            let state = self.state.read().await;
            state.producer_route_target_for_session(session_id, stream_type)
        }) else {
            return;
        };
        let transport_session_key =
            self.transport_session_key(session_id, producer_target.owner_connection_id());
        if transport_adapter
            .set_producer_active(
                &transport_session_key,
                producer_target.transport_media_id(),
                active,
            )
            .await
            .is_err()
        {
            warn!(
                ?session_id,
                ?stream_type,
                active,
                "transport adapter failed to update producer route activity"
            );
            return;
        }
        let fanout = {
            let mut state = self.state.write().await;
            state.apply_producer_activity(session_id, &producer_target, stream_type, active)
        };
        if let Some(fanout) = fanout {
            fanout.emit();
        }
    }

    pub async fn update_download_state(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let route_updates = {
            let state = self.state.read().await;
            state.download_route_updates(session_id, target_session_id, states)
        };
        let mut committed_updates = Vec::new();
        for route_update in route_updates {
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
                continue;
            }
            committed_updates.push(route_update);
        }
        let mut state = self.state.write().await;
        state.commit_download_route_updates(session_id, target_session_id, committed_updates);
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
        let Some(outcome) = ({
            let mut state = self.state.write().await;
            state.unpublish_track(session_id, connection_id, stream_type)
        }) else {
            return false;
        };
        self.cleanup_transport_removals(
            Some(transport_adapter),
            &outcome.transport_removals,
            TransportCleanupMode::NativeSessionProtocol,
        )
        .await;
        outcome.emit(session_id, stream_type);
        true
    }

    async fn commit_published_track(
        &self,
        session_id: &SessionId,
        connection_id: u64,
        pending_publish: PendingPublishedTrack,
        transport_media_id: TransportMediaId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let consumer_targets = {
            let mut state = self.state.write().await;
            state.commit_published_track(pending_publish, transport_media_id)
        };
        let Some((producer_id, consumer_targets)) = consumer_targets else {
            let _result = transport_adapter
                .remove_media(
                    &self.transport_session_key(session_id, connection_id),
                    transport_media_id,
                )
                .await;
            return None;
        };

        for target in consumer_targets {
            self.bootstrap_consumer_target(
                &target,
                transport_adapter,
                ConsumerBootstrapOrigin::Publish,
            )
            .await;
        }
        Some(producer_id.into_wire_id())
    }
}
