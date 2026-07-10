use super::{super::ConsumerKey, RoomTopology};
use crate::engine::source_model::{ConsumerSourceSelection, PublishedSourceId};

impl RoomTopology {
    pub(in crate::engine::room::media_graph) fn ensure_selection_for_test(
        &mut self,
        key: &ConsumerKey,
        selection: ConsumerSourceSelection,
    ) {
        self.route_graph.ensure_selection(key, selection);
    }

    pub(in crate::engine::room::media_graph) fn remove_source_for_test(
        &mut self,
        source_id: PublishedSourceId,
    ) -> bool {
        self.remove_source(source_id).is_some()
    }

    pub(in crate::engine::room::media_graph) fn remove_route_graph_entry_for_test(
        &mut self,
        key: &ConsumerKey,
    ) {
        self.route_graph.remove_key_state(key);
    }
}
