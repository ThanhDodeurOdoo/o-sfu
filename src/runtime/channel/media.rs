use tracing::warn;

use crate::runtime::ConnectionId;
use crate::runtime::diagnostics::DiagnosticsEventData;
use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use o_sfu_protocol::shared::{DownloadStates, SessionId, StreamType};

use super::{
    Channel,
    effects::{SubscriptionEffectPlan, UnpublishEffectPlan},
    state::{ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget},
};

impl Channel {
    pub(crate) async fn bootstrap_missing_consumers_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
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

    #[allow(
        clippy::cognitive_complexity,
        reason = "the production-change transition intentionally keeps router updates, session-info sync, broadcast, and transport activity in one explicit sequence"
    )]
    pub(crate) async fn set_publication_active_runtime(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
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
        self.diagnostics.record(
            DiagnosticsEventData::for_session(
                self.uuid(),
                session_id,
                telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
            )
            .with_connection_id(connection_id.as_u64())
            .with_media_worker_id(self.media_worker_id())
            .with_transport_media_id(outcome.transport_media_id.as_u64())
            .insert_field("active", outcome.active)
            .insert_field("stream_type", format!("{stream_type:?}").to_lowercase()),
        );
        outcome.fanout.emit();
    }

    /// Persist the subscriber's download intent and project the resulting route
    /// activity changes onto the transport boundary.
    pub(crate) async fn update_subscription_runtime(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let effect_plan = {
            let mut state = self.state.write().await;
            SubscriptionEffectPlan::from_route_updates(
                self,
                session_id,
                connection_id,
                target_session_id,
                state.apply_download_state_update(
                    session_id,
                    connection_id,
                    target_session_id,
                    states,
                ),
            )
        };
        effect_plan.execute(self, transport_adapter).await;
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
        connection_id: ConnectionId,
        stream_type: StreamType,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some(effect_plan) = ({
            let state = self.state.read().await;
            state
                .unpublish_transport_removals(session_id, connection_id, stream_type)
                .map(|transport_removals| {
                    UnpublishEffectPlan::new(
                        session_id.clone(),
                        connection_id,
                        stream_type,
                        transport_removals,
                    )
                })
        }) else {
            return false;
        };
        effect_plan.execute(self, transport_adapter).await
    }
}
