//! Post-effect video policy commit helpers.
//!
//! Async transport calls may race with websocket replacement, unpublish, or
//! session cleanup. The commit helpers revalidate every connection and media
//! handle observed during planning before selector state or featured projection
//! becomes authoritative.

use super::{
    super::{
        super::{ChannelEventMessage, outbound::MessageFanout},
        shared::{ChannelState, ConsumerKey},
    },
    action::{ConsumerPacketSelectionUpdate, FeaturedSessionUpdate},
};
use crate::runtime::source_model::ConsumerSourceSelection;

impl ChannelState {
    /// Commits selector updates that still match the routed consumer media.
    ///
    /// Accepted updates store both the selected source-domain intent and the
    /// hysteresis counters that make later refreshes deterministic. Stale
    /// updates from a replaced socket or removed route become no-ops.
    pub(in crate::runtime::channel) fn commit_consumer_packet_selection_updates(
        &mut self,
        updates: &[ConsumerPacketSelectionUpdate],
    ) {
        for update in updates {
            let key = ConsumerKey::new(update.consumer_session_id(), update.source_id());
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
            selection.set_adaptation_observations(
                update.pressure_observations(),
                update.upgrade_observations(),
            );
        }
    }

    /// Commits featured-state updates and builds the compatibility fanout.
    ///
    /// The returned fanout is emitted after the caller releases the channel
    /// write lock. That keeps layout projection consistent with room state
    /// while preserving the no-I/O-under-lock rule used by the effect layer.
    pub(in crate::runtime::channel) fn commit_featured_session_updates(
        &mut self,
        updates: &[FeaturedSessionUpdate],
    ) -> Option<MessageFanout> {
        let changed_session_ids = updates
            .iter()
            .filter_map(|update| {
                let session = self.sessions.get_mut(update.session_id())?;
                if session.layout.featured() == update.featured() {
                    return None;
                }
                session.layout.set_featured(update.featured());
                Some(update.session_id().clone())
            })
            .collect::<Vec<_>>();
        if changed_session_ids.is_empty() {
            return None;
        }
        let snapshot = changed_session_ids
            .into_iter()
            .filter_map(|session_id| self.session_info_snapshot(&session_id))
            .collect();
        Some(self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)))
    }
}
