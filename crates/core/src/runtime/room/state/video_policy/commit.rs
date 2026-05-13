//! Post-effect video policy commit helpers.
//!
//! Async transport calls may race with websocket replacement, unpublish, or
//! user cleanup. The commit helpers revalidate every connection and media
//! handle observed during planning before selector state or featured projection
//! becomes authoritative.

use super::{
    super::{
        super::{RoomEventMessage, outbound::MessageFanout},
        shared::{ConsumerKey, RoomState},
    },
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
};
use crate::runtime::source_model::ConsumerSourceSelection;

impl RoomState {
    /// Commits selector updates that still match the routed consumer media.
    ///
    /// Accepted updates store both the selected source-domain intent and the
    /// hysteresis counters that make later refreshes deterministic. Stale
    /// updates from a replaced socket or removed route become no-ops.
    pub(in crate::runtime::room) fn commit_consumer_packet_selection_updates(
        &mut self,
        updates: &[ConsumerPacketSelectionUpdate],
    ) {
        for update in updates {
            let key = ConsumerKey::new(update.consumer_user_id(), update.source_id());
            let Some(consumer_state) = self.consumer_index.get(&key) else {
                continue;
            };
            if consumer_state.consumer_connection_id != update.consumer_connection_id()
                || consumer_state.source_connection_id != update.source_connection_id()
                || consumer_state.source_media != update.source_transport_media_id()
                || consumer_state.consumer_media != update.consumer_transport_media_id()
            {
                continue;
            }
            let selection = self
                .consumer_source_selections
                .entry(key)
                .or_insert_with(|| ConsumerSourceSelection::open(true));
            selection.set_selector(update.selector());
            selection.set_policy_pause_reason(update.policy_pause_reason());
            selection.set_budget(update.budget());
            selection.set_adaptation_observations(
                update.pressure_observations(),
                update.upgrade_observations(),
            );
        }
    }

    /// Commits featured-state updates and builds the compatibility fanout.
    ///
    /// The returned fanout is emitted after the caller releases the room
    /// write lock. That keeps layout projection consistent with room state
    /// while preserving the no-I/O-under-lock rule used by the effect layer.
    pub(in crate::runtime::room) fn commit_featured_user_updates(
        &mut self,
        updates: &[FeaturedUserUpdate],
    ) -> Option<MessageFanout> {
        let changed_user_ids = updates
            .iter()
            .filter_map(|update| {
                let user = self.users.get_mut(update.user_id())?;
                if user.layout.featured() == update.featured() {
                    return None;
                }
                user.layout.set_featured(update.featured());
                Some(update.user_id().clone())
            })
            .collect::<Vec<_>>();
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
