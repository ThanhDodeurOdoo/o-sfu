//! Post-effect video policy commit helpers.
//!
//! Async transport calls may race with websocket replacement, unpublish, or
//! user cleanup. The commit helpers revalidate every connection and media
//! handle observed during planning before selector state or featured projection
//! becomes authoritative.

use super::{
    super::{RoomEventMessage, outbound::MessageFanout, state::RoomState},
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
};

impl RoomState {
    /// Commits selector updates that still match the routed consumer media.
    ///
    /// Accepted updates store both the selected source-domain intent and the
    /// hysteresis counters that make later refreshes deterministic. Stale
    /// updates from a replaced socket or removed route become no-ops.
    pub fn commit_consumer_packet_selection_updates(
        &mut self,
        updates: &[ConsumerPacketSelectionUpdate],
    ) {
        for update in updates {
            self.update_source_policy_consumer_selection(
                &update.route,
                update.source_id,
                |selection| {
                    selection.set_selector(update.selector);
                    selection.set_policy_pause_reason(update.policy_pause_reason);
                    selection.set_budget(update.budget);
                    selection.set_adaptation_observations(
                        update.pressure_observations,
                        update.upgrade_observations,
                    );
                },
            );
        }
    }

    /// Commits featured-state updates and builds the compatibility fanout.
    ///
    /// The returned fanout is emitted after the caller releases the room
    /// write lock. That keeps layout projection consistent with room state
    /// while preserving the no-I/O-under-lock rule used by the effect layer.
    pub fn commit_featured_user_updates(
        &mut self,
        updates: &[FeaturedUserUpdate],
    ) -> Option<MessageFanout> {
        let mut changed_user_ids = Vec::new();
        for update in updates {
            if self.update_source_policy_featured_user(update.user_id(), update.featured()) {
                changed_user_ids.push(update.user_id().clone());
            }
        }
        if changed_user_ids.is_empty() {
            return None;
        }
        let snapshot = changed_user_ids
            .into_iter()
            .filter_map(|user_id| self.user_info_snapshot(&user_id))
            .collect();
        Some(self.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)))
    }
}
