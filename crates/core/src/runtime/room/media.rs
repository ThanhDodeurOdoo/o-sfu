//! Room media orchestration above pure state and transport effects.
//!
//! Public callers should normally enter through [`crate::MediaSession`]. This
//! module is the room-internal bridge that takes generic source ids and
//! subscription intents, performs short authoritative state transitions and
//! then delegates transport work to effect plans after locks are released.
//!
//! The room does not translate product stream labels here. A caller must already
//! have a [`UserStreamId`] and, for subscriptions, a
//! [`SourceSubscriptionIntent`]. That keeps business policy at the application
//! edge while this layer focuses on ownership, staleness and cleanup ordering.

use std::collections::BTreeMap;

use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{
    Room, RoomMediaCounts,
    effects::{SubscriptionEffectContext, SubscriptionEffectPlan, UnpublishEffectPlan},
    state::ConsumerBootstrapOrigin,
};
use crate::{
    PublicationActivity, PublicationActivityOutcome, SubscriptionUpdateOutcome,
    TransportEffectOutcome, UnpublishOutcome,
    runtime::{
        ConnectionId, UserId,
        diagnostics::DiagnosticsEventData,
        media_transport::{MediaTransport, ProducerActivity},
        source_model::{SourceSubscriptionIntent, UserStreamId},
    },
};

impl Room {
    pub(crate) async fn bootstrap_missing_consumers_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_port: &MediaTransport,
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
        media_port: &MediaTransport,
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
        stream_id: &UserStreamId,
        activity: PublicationActivity,
        media_port: &MediaTransport,
    ) -> PublicationActivityOutcome {
        let active = activity.is_active();
        let Some(producer_target) = ({
            let state = self.state.read().await;
            state.producer_route_target(user_id, connection_id, stream_id)
        }) else {
            return PublicationActivityOutcome::MissingPublication;
        };
        let transport_user_key =
            self.transport_user_key(user_id, producer_target.owner_connection_id());
        let Some(outcome) = ({
            let mut state = self.state.write().await;
            state.apply_producer_activity(user_id, &producer_target, stream_id, active)
        }) else {
            return PublicationActivityOutcome::StalePublication;
        };
        let transport_update = if media_port
            .set_producer_active(
                &transport_user_key,
                outcome.transport_media_id,
                ProducerActivity::from_active(outcome.active),
            )
            .await
            .is_err()
        {
            warn!(
                ?user_id,
                stream_id = %stream_id,
                active = outcome.active,
                "media transport failed to update producer route activity"
            );
            TransportEffectOutcome::Failed
        } else {
            TransportEffectOutcome::Applied
        };
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
            .insert_field("stream_id", stream_id.to_string()),
        );
        outcome.emit();
        PublicationActivityOutcome::Applied { transport_update }
    }

    /// Persist the subscriber's download intent and project the resulting route
    /// activity changes onto the transport boundary.
    pub(crate) async fn update_subscription_runtime(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        media_port: &MediaTransport,
    ) -> SubscriptionUpdateOutcome {
        let effect_plan = {
            let mut state = self.state.write().await;
            if state.user_for_connection(user_id, connection_id).is_none() {
                return SubscriptionUpdateOutcome::StaleConnection;
            }
            let media_counts_before = RoomMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let planned_change =
                state.plan_subscription_change(user_id, connection_id, target_user_id, intents);
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
        SubscriptionUpdateOutcome::Applied
    }

    pub async fn is_stream_published(&self, user_id: &UserId, stream_id: &UserStreamId) -> bool {
        self.state
            .read()
            .await
            .producer_route_target_for_user(user_id, stream_id)
            .is_some()
    }

    pub(crate) async fn unpublish_track(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
        media_port: &MediaTransport,
    ) -> UnpublishOutcome {
        let effect_plan =
            UnpublishEffectPlan::new(user_id.clone(), connection_id, stream_id.clone());
        effect_plan.execute(self, media_port).await
    }
}
