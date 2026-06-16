use super::SourcePolicyEffectPlan;
use crate::engine::{
    UserId, media_transport::TransportConsumerRoute, source_model::PublishedSourceId,
};

impl SourcePolicyEffectPlan {
    pub fn retain_updates_for_consumer_source_for_test(
        &mut self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        self.packet_updates.retain(|update| {
            &update.route.consumer_user_id == consumer_user_id && update.source_id == source_id
        });
        let kept = !self.packet_updates.is_empty();
        self.featured_users.clear();
        kept
    }

    pub fn uses_transport_route_for_consumer_source_for_test(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
        transport_route: &TransportConsumerRoute,
    ) -> bool {
        self.packet_updates.iter().any(|update| {
            &update.route.consumer_user_id == consumer_user_id
                && update.source_id == source_id
                && &update.transport_route == transport_route
        })
    }
}
