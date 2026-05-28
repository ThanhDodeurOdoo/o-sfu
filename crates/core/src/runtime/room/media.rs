//! Room media orchestration above pure state and transport effects.
//!
//! Public callers should normally enter through [`crate::prelude::MediaSession`]. This
//! module is the room-internal bridge that takes generic source ids and
//! subscription intents, performs short authoritative state transitions and
//! then delegates transport work to effect plans after locks are released.
//!
//! The room does not translate product stream labels here. A caller must already
//! have a [`UserStreamId`] and, for subscriptions, a
//! [`SourceSubscriptionIntent`]. That keeps business policy at the application
//! edge while this layer focuses on ownership, staleness and cleanup ordering.

use super::{Room, effects::SubscriptionEffectPlan, media_graph::ConsumerBootstrapOrigin};
use crate::runtime::{UserId, media_transport::MediaTransport, source_model::UserStreamId};

impl Room {
    pub(super) async fn bootstrap_consumer_targets(
        &self,
        media_port: &MediaTransport,
        origin: ConsumerBootstrapOrigin,
        targets: Vec<super::media_graph::PendingConsumerBootstrapTarget>,
    ) {
        let effect_plan = {
            let worker_lookup = self.placement_state.worker_lookup();
            let mut state = self.state.write().await;
            let media_counts_before = state.media_counts();
            let planned_bootstraps =
                state.plan_consumer_bootstraps_for_targets(targets, worker_lookup);
            let media_counts_after = state.media_counts();
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

    pub async fn is_stream_published(&self, user_id: &UserId, stream_id: &UserStreamId) -> bool {
        self.state
            .read()
            .await
            .producer_route_target_for_user(user_id, stream_id)
            .is_some()
    }
}
