use super::SourcePolicyEffectPlan;
use crate::runtime::{UserId, source_model::PublishedSourceId};

impl SourcePolicyEffectPlan {
    pub(in crate::runtime::room) fn retain_updates_for_consumer_source_for_test(
        &mut self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        self.consumer_packet_updates.retain(|update| {
            update.route().consumer_user_id() == consumer_user_id && update.source_id() == source_id
        });
        self.featured_users.clear();
        !self.consumer_packet_updates.is_empty()
    }
}
