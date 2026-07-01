use super::{ConsumerPacketSelectionUpdate, SourcePolicyTransaction};
use crate::engine::{UserId, source_model::PublishedSourceId};

impl SourcePolicyTransaction {
    pub(in crate::engine::room::source_policy) fn route_updates_for_test(
        &self,
    ) -> impl Iterator<Item = &ConsumerPacketSelectionUpdate> + '_ {
        self.route_control
            .consumer_finishes_for_test()
            .map(|finish| &finish.selection)
    }

    pub fn has_only_state_update_for_consumer_source_for_test(
        &self,
        consumer_user_id: &UserId,
        source_id: PublishedSourceId,
    ) -> bool {
        let mut state_update = false;
        for update in self.route_updates_for_test() {
            if &update.transport_ref.consumer_user_id == consumer_user_id
                && update.source_id == source_id
            {
                return false;
            }
        }
        for update in &self.state_updates {
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
