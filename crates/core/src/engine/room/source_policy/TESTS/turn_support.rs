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
        self.state_packet_updates.retain(|update| {
            &update.transport_ref.consumer_user_id == consumer_user_id
                && update.source_id == source_id
        });
        self.transport_packet_updates.retain(|packet| {
            let update = &packet.update;
            &update.transport_ref.consumer_user_id == consumer_user_id
                && update.source_id == source_id
        });
        self.receiver_bwe_targets.clear();
        self.featured_users.clear();
        !self.state_packet_updates.is_empty() || !self.transport_packet_updates.is_empty()
    }

    pub fn has_captured_transport_route_for_consumer_source_for_test(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
        transport_route: &TransportConsumerRoute,
    ) -> bool {
        self.transport_packet_updates.iter().any(|packet| {
            let update = &packet.update;
            let target = &packet.target;
            &update.transport_ref.consumer_user_id == consumer_user_id
                && update.source_id == source_id
                && target.transport_route() == transport_route
        })
    }

    pub fn has_only_state_update_for_consumer_source_for_test(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        let mut state_update = false;
        for packet in &self.transport_packet_updates {
            let update = &packet.update;
            if &update.transport_ref.consumer_user_id == consumer_user_id
                && update.source_id == source_id
            {
                return false;
            }
        }
        for update in &self.state_packet_updates {
            if &update.transport_ref.consumer_user_id != consumer_user_id
                || update.source_id != source_id
            {
                continue;
            }
            if update.requires_media_transport_effect() {
                return false;
            }
            state_update = true;
        }
        state_update
    }
}
