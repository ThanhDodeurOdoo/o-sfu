use o_sfu_protocol::shared::{
    DownloadStates, SessionId, SessionInfo, SessionPermissions, StreamType,
};
use o_sfu_protocol::signaling::RecordingOptions;
use o_sfu_router::{
    MediaCapabilities, MediaKind, RtpParameters as RouterRtpParameters,
    derive_consumable_rtp_parameters,
};
use tokio::sync::mpsc;
use tracing::warn;

use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportMediaId};

use super::super::session_negotiation::SessionTransportReady;
use super::super::{
    Channel, ChannelJoinError, ChannelManager, SessionCleanup, SessionOutbound,
    media_transaction::StagedPublishTransaction, session_negotiation::SessionNegotiationUpdate,
    state::ConsumerBootstrapOrigin,
};

#[derive(Debug, Clone)]
pub(crate) struct NegotiatedPublish {
    pub(crate) connection_id: u64,
    pub(crate) stream_type: StreamType,
    pub(crate) media_kind: MediaKind,
    pub(crate) transport_media_id: TransportMediaId,
    pub(crate) consumable_rtp_parameters: o_sfu_router::RtpParameters,
}

#[derive(Clone, Copy)]
pub(crate) struct ChannelTestApi<'a> {
    channel: &'a Channel,
}

#[derive(Clone, Copy)]
pub(crate) struct ChannelManagerTestApi<'a> {
    manager: &'a ChannelManager,
}

impl Channel {
    #[must_use]
    pub(crate) const fn test_api(&self) -> ChannelTestApi<'_> {
        ChannelTestApi { channel: self }
    }
}

impl ChannelManager {
    #[must_use]
    pub(crate) const fn test_api(&self) -> ChannelManagerTestApi<'_> {
        ChannelManagerTestApi { manager: self }
    }
}

impl ChannelTestApi<'_> {
    pub(crate) async fn join_session(
        self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
    ) -> Result<u64, ChannelJoinError> {
        self.channel
            .join_session_with_cleanup(
                session_id,
                label,
                permissions,
                sender,
                SessionCleanup::state_only(None),
                false,
            )
            .await
    }

    pub(crate) async fn leave_session(self, session_id: &SessionId, connection_id: u64) -> bool {
        self.leave_session_with_cleanup(session_id, connection_id, SessionCleanup::state_only(None))
            .await
    }

    pub(crate) async fn join_session_without_transport_cleanup(
        self,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<u64, ChannelJoinError> {
        self.channel
            .join_session_with_cleanup(
                session_id,
                label,
                permissions,
                sender,
                SessionCleanup::state_only(Some(transport_adapter)),
                false,
            )
            .await
    }

    pub(crate) async fn leave_session_runtime(
        self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.leave_session_with_cleanup(
            session_id,
            connection_id,
            SessionCleanup::runtime(transport_adapter),
        )
        .await
    }

    pub(crate) async fn leave_session_without_transport_cleanup(
        self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.leave_session_with_cleanup(
            session_id,
            connection_id,
            SessionCleanup::state_only(Some(transport_adapter)),
        )
        .await
    }

    async fn leave_session_with_cleanup(
        self,
        session_id: &SessionId,
        connection_id: u64,
        cleanup: SessionCleanup<'_>,
    ) -> bool {
        let channel = self.channel;
        let outcome = {
            let mut state = channel.state.write().await;
            state.apply_leave(session_id, connection_id)
        };
        let Some(outcome) = outcome else {
            return false;
        };
        channel
            .cleanup_transport_removals(cleanup, &outcome.transport_removals)
            .await;
        channel
            .sync_source_packet_selection_policy(cleanup.transport_adapter())
            .await;
        Channel::emit_lifecycle_effects(outcome.effects);
        true
    }

    pub(crate) async fn broadcast(self, sender_id: &SessionId, message: serde_json::Value) {
        let channel = self.channel;
        let Some(connection_id) = channel.state.read().await.session_connection_id(sender_id)
        else {
            return;
        };
        channel
            .broadcast_runtime(sender_id, connection_id, message)
            .await;
    }

    pub(crate) async fn update_session_info(
        self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
    ) {
        let channel = self.channel;
        let Some(connection_id) = channel.state.read().await.session_connection_id(session_id)
        else {
            return;
        };
        let outcome = {
            let mut state = channel.state.write().await;
            state.apply_presence_update(session_id, connection_id, &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            outcome.emit();
        }
    }

    pub(crate) async fn start_recording(
        self,
        session_id: &SessionId,
        options: RecordingOptions,
    ) -> bool {
        let channel = self.channel;
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            channel.metrics.record_recording_start_rejected();
            return false;
        };
        channel
            .start_recording_runtime(session_id, connection_id, options)
            .await
    }

    pub(crate) async fn stop_recording(self, session_id: &SessionId) -> bool {
        let channel = self.channel;
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            channel.metrics.record_recording_stop_rejected();
            return false;
        };
        channel
            .stop_recording_runtime(session_id, connection_id)
            .await
    }

    pub(crate) async fn update_session_info_runtime(
        self,
        session_id: &SessionId,
        info: SessionInfo,
        need_refresh: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self.session_connection_id(session_id).await else {
            return;
        };
        self.channel
            .update_session_info_runtime_for_connection(
                session_id,
                connection_id,
                info,
                need_refresh,
                transport_adapter,
            )
            .await;
    }

    pub(crate) async fn disconnect_sessions(self, session_ids: &[SessionId]) {
        self.channel
            .disconnect_sessions_with_cleanup(session_ids, SessionCleanup::state_only(None))
            .await;
    }

    pub(crate) async fn disconnect_sessions_without_transport_cleanup(
        self,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.channel
            .disconnect_sessions_with_cleanup(
                session_ids,
                SessionCleanup::state_only(Some(transport_adapter)),
            )
            .await;
    }

    pub(crate) async fn apply_client_rtp_capabilities(
        self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: MediaCapabilities,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.channel.state.write().await;
            state.set_client_rtp_capabilities_for_test(session_id, connection_id, &capabilities)
        };
        self.apply_negotiation_update_for_test(session_id, connection_id, update, transport_adapter)
            .await
    }

    pub(crate) async fn apply_publish_transport_ready(
        self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Publish,
            transport_adapter,
        )
        .await
    }

    pub(crate) async fn apply_consume_transport_ready(
        self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.apply_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Consume,
            transport_adapter,
        )
        .await
    }

    async fn apply_transport_ready_for_test(
        self,
        session_id: &SessionId,
        connection_id: u64,
        readiness: SessionTransportReady,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let update = {
            let mut state = self.channel.state.write().await;
            state.set_transport_ready_for_test(session_id, connection_id, readiness)
        };
        self.apply_negotiation_update_for_test(session_id, connection_id, update, transport_adapter)
            .await
    }

    async fn apply_negotiation_update_for_test(
        self,
        session_id: &SessionId,
        connection_id: u64,
        update: SessionNegotiationUpdate,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        if !update.session_present {
            return false;
        }
        if update.became_consumer_ready {
            return self
                .channel
                .bootstrap_missing_consumers_for_connection(
                    session_id,
                    connection_id,
                    transport_adapter,
                )
                .await;
        }
        true
    }

    pub(super) async fn set_client_rtp_capabilities(
        self,
        session_id: &SessionId,
        capabilities: MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let mut state = self.channel.state.write().await;
        let connection_id = state.session_connection_id(session_id).unwrap_or(u64::MAX);
        state.set_client_rtp_capabilities_for_test(session_id, connection_id, &capabilities)
    }

    pub(super) async fn set_publish_transport_ready(
        self,
        session_id: &SessionId,
    ) -> SessionNegotiationUpdate {
        let mut state = self.channel.state.write().await;
        let connection_id = state.session_connection_id(session_id).unwrap_or(u64::MAX);
        state.set_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Publish,
        )
    }

    pub(super) async fn set_consume_transport_ready(
        self,
        session_id: &SessionId,
    ) -> SessionNegotiationUpdate {
        let mut state = self.channel.state.write().await;
        let connection_id = state.session_connection_id(session_id).unwrap_or(u64::MAX);
        state.set_transport_ready_for_test(
            session_id,
            connection_id,
            SessionTransportReady::Consume,
        )
    }

    pub(super) async fn router_session_count(self) -> usize {
        let (count, _camera_count, _screen_count) =
            self.channel.state.read().await.session_stats_counts();
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    pub(super) async fn router_session_permissions(
        self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.channel
            .state
            .read()
            .await
            .session_permissions(session_id)
    }

    pub(super) async fn session_has_parsed_client_rtp_capabilities(
        self,
        session_id: &SessionId,
    ) -> bool {
        self.channel
            .state
            .read()
            .await
            .session_has_parsed_client_rtp_capabilities(session_id)
    }

    pub(crate) async fn parsed_client_rtp_capabilities(
        self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::MediaCapabilities> {
        self.channel
            .state
            .read()
            .await
            .parsed_client_rtp_capabilities(session_id)
    }

    pub(crate) async fn session_connection_id(self, session_id: &SessionId) -> Option<u64> {
        self.channel
            .state
            .read()
            .await
            .session_connection_id(session_id)
    }

    pub(super) async fn producer_count(self) -> usize {
        self.channel.state.read().await.producer_count()
    }

    pub(super) async fn consumer_count(self) -> usize {
        self.channel.state.read().await.consumer_count()
    }

    pub(super) async fn first_published_transport_media_id(self) -> Option<TransportMediaId> {
        self.channel
            .state
            .read()
            .await
            .first_published_transport_media_id()
    }

    pub(super) async fn producer_transport_media_id(
        self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.channel.state.read().await.producer_transport_media_id(
            session_id,
            connection_id,
            stream_type,
        )
    }

    pub(super) async fn has_producer_route_target(
        self,
        owner_session_id: &SessionId,
        owner_connection_id: u64,
        stream_type: StreamType,
    ) -> bool {
        self.channel
            .state
            .read()
            .await
            .producer_route_target(owner_session_id, owner_connection_id, stream_type)
            .is_some()
    }

    pub(super) async fn producer_stream_type_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.channel
            .state
            .read()
            .await
            .producer_stream_type_for_transport_media_id(transport_media_id)
    }

    pub(super) async fn session_info_snapshot(
        self,
        session_id: &SessionId,
    ) -> Option<(SessionId, SessionInfo)> {
        self.channel
            .state
            .read()
            .await
            .session_info_snapshot(session_id)
    }

    pub(super) async fn has_session(self, session_id: &SessionId) -> bool {
        self.channel.state.read().await.has_session(session_id)
    }

    #[must_use]
    pub(crate) const fn media_worker_id(self) -> usize {
        self.channel.definition.media_worker_id()
    }

    pub(crate) async fn publish_negotiated_track(
        self,
        session_id: &SessionId,
        publish: NegotiatedPublish,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let validated_descriptor = {
            let state = self.channel.state.read().await;
            state.validate_publish_descriptor(
                session_id,
                publish.connection_id,
                publish.stream_type,
                publish.media_kind,
            )?
        };
        StagedPublishTransaction::new(validated_descriptor, publish.transport_media_id)
            .into_commit_snapshot(publish.consumable_rtp_parameters)
            .commit(self.channel, transport_adapter)
            .await
    }

    pub(crate) async fn bootstrap_missing_consumers(
        self,
        session_id: &SessionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let targets = {
            let state = self.channel.state.read().await;
            state.missing_consumer_targets(session_id)
        };
        for target in targets {
            self.channel
                .execute_consumer_bootstrap(
                    target,
                    transport_adapter,
                    ConsumerBootstrapOrigin::LateJoin,
                )
                .await;
        }
    }

    pub(crate) async fn publish_track(
        self,
        session_id: &SessionId,
        stream_type: StreamType,
        media_kind: MediaKind,
        producer_rtp_parameters: RouterRtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let publish_prerequisites = {
            let state = self.channel.state.read().await;
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
        let validated_descriptor = {
            let state = self.channel.state.read().await;
            state.validate_publish_descriptor(
                session_id,
                publisher_connection_id,
                stream_type,
                media_kind,
            )?
        };
        let transport_media_id = match transport_adapter
            .publish_media(
                &self
                    .channel
                    .transport_session_key(session_id, publisher_connection_id),
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
        StagedPublishTransaction::new(validated_descriptor, transport_media_id)
            .into_commit_snapshot(consumable_rtp_parameters)
            .commit(self.channel, transport_adapter)
            .await
    }

    pub(crate) async fn set_publication_active(
        self,
        session_id: &SessionId,
        stream_type: StreamType,
        active: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self
            .channel
            .state
            .read()
            .await
            .session_connection_id(session_id)
        else {
            return;
        };
        self.channel
            .set_publication_active_runtime(
                session_id,
                connection_id,
                stream_type,
                active,
                transport_adapter,
            )
            .await;
    }

    pub(crate) async fn update_subscription(
        self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(connection_id) = self
            .channel
            .state
            .read()
            .await
            .session_connection_id(session_id)
        else {
            return;
        };
        self.channel
            .update_subscription_runtime(
                session_id,
                connection_id,
                target_session_id,
                states,
                transport_adapter,
            )
            .await;
    }

    pub(crate) async fn stage_negotiated_publish_for_test(
        self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.channel
            .stage_negotiated_publish(session_id, connection_id, stream_type, transport_adapter)
            .await
    }

    pub(crate) async fn rollback_staged_publish_for_test(
        self,
        session_id: &SessionId,
        connection_id: u64,
        stream_type: StreamType,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        self.channel
            .rollback_staged_publish(session_id, connection_id, stream_type, transport_adapter)
            .await
    }

    pub(crate) async fn commit_staged_publishes_for_test(
        self,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        self.channel
            .commit_staged_publishes(session_id, connection_id, transport_adapter)
            .await;
    }

    pub(crate) async fn staged_publish_count(
        self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> usize {
        self.channel
            .staged_publish_count_for_connection(session_id, connection_id)
            .await
    }
}

impl ChannelManagerTestApi<'_> {
    pub(crate) async fn has_session(self, channel_uuid: &str, session_id: &SessionId) -> bool {
        let Some(channel) = self.manager.get_by_uuid(channel_uuid).await else {
            return false;
        };
        channel.test_api().has_session(session_id).await
    }
}
