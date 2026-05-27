use std::collections::BTreeMap;

use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{
    super::{
        SourcePolicyEvent,
        effects::{SubscriptionEffectContext, SubscriptionEffectPlan, UnpublishEffectPlan},
        state::ConsumerBootstrapOrigin,
    },
    RoomUserOperation,
};
use crate::{
    PublicationActivity, PublicationActivityOutcome, SubscriptionUpdateOutcome,
    TransportEffectOutcome, UnpublishOutcome,
    runtime::{
        UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{ProducerActivity, TransportSourceKey},
        source_model::{SourceSubscriptionIntent, UserStreamId},
    },
};

impl RoomUserOperation<'_> {
    pub(crate) async fn is_stream_published(self, stream_id: &UserStreamId) -> bool {
        self.room()
            .is_stream_published(self.user_id(), stream_id)
            .await
    }

    pub(crate) async fn bootstrap_missing_consumers(self) -> bool {
        let room = self.room();
        let worker_lookup = room.placement_state.worker_lookup();
        let mut state = room.state.write().await;
        let media_counts_before = state.media_counts();
        let Some(planned_bootstraps) = state.plan_missing_consumer_bootstraps_for_connection(
            self.user_id(),
            self.connection_id(),
            worker_lookup,
        ) else {
            return false;
        };
        let media_counts_after = state.media_counts();
        drop(state);
        let effect_plan = SubscriptionEffectPlan::from_planned_bootstraps(
            media_counts_before,
            media_counts_after,
            planned_bootstraps,
            ConsumerBootstrapOrigin::LateJoin,
        );
        effect_plan.execute(room, self.media_transport()).await;
        room.handle_source_policy_event(
            SourcePolicyEvent::RouteGraphChanged,
            Some(self.media_transport()),
        )
        .await;
        true
    }

    #[allow(
        clippy::cognitive_complexity,
        reason = "the production-change transition intentionally keeps router updates, user-info sync, broadcast, and transport activity in one explicit sequence"
    )]
    pub(crate) async fn set_publication_activity(
        self,
        stream_id: &UserStreamId,
        activity: PublicationActivity,
    ) -> PublicationActivityOutcome {
        let room = self.room();
        let active = activity.is_active();
        let Some(producer_target) = ({
            let state = room.state.read().await;
            state.producer_route_target(self.user_id(), self.connection_id(), stream_id)
        }) else {
            return PublicationActivityOutcome::MissingPublication;
        };
        let transport_user_key =
            room.transport_user_key(self.user_id(), producer_target.owner_connection_id());
        let media_worker_id = transport_user_key.media_worker_id();
        let Some(outcome) = ({
            let mut state = room.state.write().await;
            state.apply_producer_activity(self.user_id(), &producer_target, stream_id, active)
        }) else {
            return PublicationActivityOutcome::StalePublication;
        };
        let source = TransportSourceKey::new(transport_user_key, outcome.transport_media_id);
        let transport_update = if self
            .media_transport()
            .set_producer_active(&source, ProducerActivity::from_active(outcome.active))
            .await
            .is_err()
        {
            warn!(
                user_id = ?self.user_id(),
                stream_id = %stream_id,
                active = outcome.active,
                "media transport failed to update producer route activity"
            );
            TransportEffectOutcome::Failed
        } else {
            TransportEffectOutcome::Applied
        };
        room.diagnostics.record(
            DiagnosticsEventData::for_user(
                room.uuid(),
                self.user_id(),
                telemetry_event::PUBLICATION_ACTIVITY_CHANGED,
            )
            .with_connection_id(self.connection_id().as_u64())
            .with_media_worker_id(media_worker_id)
            .with_transport_media_id(outcome.transport_media_id.as_u64())
            .insert_field("active", outcome.active)
            .insert_field("stream_id", stream_id.to_string()),
        );
        outcome.emit();
        room.handle_source_policy_event(
            SourcePolicyEvent::FanoutPressureChanged,
            Some(self.media_transport()),
        )
        .await;
        PublicationActivityOutcome::Applied { transport_update }
    }

    /// Persist the subscriber's download intent and project the resulting route
    /// activity changes onto the transport boundary.
    pub(crate) async fn update_subscription(
        self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> SubscriptionUpdateOutcome {
        let room = self.room();
        let (effect_plan, source_policy_event) = {
            let worker_lookup = room.placement_state.worker_lookup();
            let mut state = room.state.write().await;
            if state
                .user_for_connection(self.user_id(), self.connection_id())
                .is_none()
            {
                return SubscriptionUpdateOutcome::StaleConnection;
            }
            let media_counts_before = state.media_counts();
            let planned_change = state.plan_subscription_change(
                self.user_id(),
                self.connection_id(),
                target_user_id,
                intents,
                worker_lookup,
            );
            let source_policy_event = if planned_change.touches_route_graph() {
                SourcePolicyEvent::RouteGraphChanged
            } else {
                SourcePolicyEvent::ReceiverIntentChanged
            };
            let media_counts_after = state.media_counts();
            drop(state);
            (
                SubscriptionEffectPlan::from_planned_change(
                    room,
                    SubscriptionEffectContext {
                        user_id: self.user_id(),
                        connection_id: self.connection_id(),
                        media_counts_before,
                        media_counts_after,
                        origin: ConsumerBootstrapOrigin::Subscribe,
                    },
                    planned_change,
                ),
                source_policy_event,
            )
        };
        effect_plan.execute(room, self.media_transport()).await;
        room.handle_source_policy_event(source_policy_event, Some(self.media_transport()))
            .await;
        SubscriptionUpdateOutcome::Applied
    }

    pub(crate) async fn unpublish(self, stream_id: &UserStreamId) -> UnpublishOutcome {
        UnpublishEffectPlan::new(
            self.user_id().clone(),
            self.connection_id(),
            stream_id.clone(),
        )
        .execute(self.room(), self.media_transport())
        .await
    }
}
