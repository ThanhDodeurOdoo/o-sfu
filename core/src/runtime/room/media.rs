use tracing::warn;

use super::{
    Room, RoomMediaCounts,
    effects::{SubscriptionEffectContext, SubscriptionEffectPlan, UnpublishEffectPlan},
    state::ConsumerBootstrapOrigin,
};
use crate::runtime::{
    ConnectionId, DownloadStates, StreamType, UserId,
    diagnostics::DiagnosticsEventData,
    telemetry::schema::event as telemetry_event,
    transport_adapter::{MediaPort, ObservabilityPort},
};

impl Room {
    pub(crate) async fn bootstrap_missing_consumers_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &impl MediaPort,
    ) -> bool {
        let mut state = self.state.write().await;
        let media_counts_before = RoomMediaCounts {
            publications: state.publication_count(),
            subscriptions: state.subscription_count(),
        };
        let Some(planned_bootstraps) =
            state.plan_missing_consumer_bootstraps_for_connection(user_id, connection_id)
        else {
            return false;
        };
        let media_counts_after = RoomMediaCounts {
            publications: state.publication_count(),
            subscriptions: state.subscription_count(),
        };
        drop(state);
        let effect_plan = SubscriptionEffectPlan::from_planned_bootstraps(
            media_counts_before,
            media_counts_after,
            planned_bootstraps,
            ConsumerBootstrapOrigin::LateJoin,
        );
        effect_plan.execute(self, media_port).await;
        true
    }

    pub(super) async fn bootstrap_consumer_targets(
        &self,
        media_port: &impl MediaPort,
        origin: ConsumerBootstrapOrigin,
        targets: Vec<super::state::PendingConsumerBootstrapTarget>,
    ) {
        let effect_plan = {
            let mut state = self.state.write().await;
            let media_counts_before = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let planned_bootstraps = state.plan_consumer_bootstraps_for_targets(targets);
            let media_counts_after = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            SubscriptionEffectPlan::from_planned_bootstraps(
                media_counts_before,
                media_counts_after,
                planned_bootstraps,
                origin,
            )
        };
        effect_plan.execute(self, media_port).await;
    }

    #[allow(
        clippy::cognitive_complexity,
        reason = "the production-change transition intentionally keeps router updates, user-info sync, broadcast, and transport activity in one explicit sequence"
    )]
    pub(crate) async fn set_publication_active_runtime(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        active: bool,
        media_port: &impl MediaPort,
    ) {
        let Some(producer_target) = ({
            let state = self.state.read().await;
            state.producer_route_target(user_id, connection_id, stream_type)
        }) else {
            return;
        };
        let transport_user_key =
            self.transport_user_key(user_id, producer_target.owner_connection_id());
        let Some(outcome) = ({
            let mut state = self.state.write().await;
            state.apply_producer_activity(user_id, &producer_target, stream_type, active)
        }) else {
            return;
        };
        if media_port
            .set_producer_active(
                &transport_user_key,
                outcome.transport_media_id,
                outcome.active,
            )
            .await
            .is_err()
        {
            warn!(
                ?user_id,
                ?stream_type,
                active = outcome.active,
                "transport adapter failed to update producer route activity"
            );
        }
        self.diagnostics.record(
            DiagnosticsEventData::for_user(
                self.uuid(),
                user_id,
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
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
        media_port: &(impl MediaPort + ObservabilityPort),
    ) {
        let effect_plan = {
            let mut state = self.state.write().await;
            let media_counts_before = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let planned_change =
                state.plan_subscription_change(user_id, connection_id, target_user_id, states);
            let media_counts_after = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            SubscriptionEffectPlan::from_planned_change(
                self,
                SubscriptionEffectContext {
                    user_id,
                    connection_id,
                    target_user_id,
                    media_counts_before,
                    media_counts_after,
                    origin: ConsumerBootstrapOrigin::Subscribe,
                },
                planned_change,
            )
        };
        effect_plan.execute(self, media_port).await;
        self.sync_source_packet_selection_policy(Some(media_port), media_port)
            .await;
    }

    pub async fn is_stream_published(&self, user_id: &UserId, stream_type: StreamType) -> bool {
        self.state
            .read()
            .await
            .producer_route_target_for_user(user_id, stream_type)
            .is_some()
    }

    pub(crate) async fn unpublish_track(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        media_port: &impl MediaPort,
    ) -> bool {
        let Some(effect_plan) = ({
            let state = self.state.read().await;
            state
                .unpublish_transport_removals(user_id, connection_id, stream_type)
                .map(|transport_removals| {
                    UnpublishEffectPlan::new(
                        user_id.clone(),
                        connection_id,
                        stream_type,
                        transport_removals,
                    )
                })
        }) else {
            return false;
        };
        effect_plan.execute(self, media_port).await
    }
}
