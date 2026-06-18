use super::SourcePolicyPlan;
use crate::engine::{
    UserId, media_transport::TransportConsumerRoute, source_model::PublishedSourceId,
};

impl SourcePolicyPlan {
    pub fn retain_updates_for_consumer_source_for_test(
        &mut self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        self.state_only_packet_updates.retain(|update| {
            &update.route.consumer_user_id == consumer_user_id && update.source_id == source_id
        });
        self.transport_effect_packet_updates
            .retain(|(update, _route)| {
                &update.route.consumer_user_id == consumer_user_id && update.source_id == source_id
            });
        self.featured_users.clear();
        !self.state_only_packet_updates.is_empty()
            || !self.transport_effect_packet_updates.is_empty()
    }

    pub fn has_captured_transport_route_for_consumer_source_for_test(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
        transport_route: &TransportConsumerRoute,
    ) -> bool {
        self.transport_effect_packet_updates
            .iter()
            .any(|(update, route)| {
                &update.route.consumer_user_id == consumer_user_id
                    && update.source_id == source_id
                    && route == transport_route
            })
    }

    pub fn has_only_state_update_for_consumer_source_for_test(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        let has_state_update = self.state_only_packet_updates.iter().any(|update| {
            &update.route.consumer_user_id == consumer_user_id && update.source_id == source_id
        });
        let has_transport_update =
            self.transport_effect_packet_updates
                .iter()
                .any(|(update, _route)| {
                    &update.route.consumer_user_id == consumer_user_id
                        && update.source_id == source_id
                });
        has_state_update && !has_transport_update
    }
}
