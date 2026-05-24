use super::{ConsumerKey, RoomMediaGraph};
use crate::runtime::{UserId, source_model::PublishedSourceId};

impl RoomMediaGraph {
    pub fn contains_source(&self, source_id: PublishedSourceId) -> bool {
        self.sources.contains_key(&source_id)
    }

    pub fn contains_pending_consumer_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.pending_consumer_bootstraps.contains(key)
    }

    pub fn contains_consumer_source_selection(&self, key: &ConsumerKey) -> bool {
        self.consumer_source_selections.contains_key(key)
    }

    pub fn source_indexes_are_empty(&self) -> bool {
        self.sources.is_empty()
            && self.source_ids_by_owner_stream.is_empty()
            && self.source_ids_by_owner.is_empty()
            && self.producer_id_by_source_id.is_empty()
            && self.producer_ids_by_owner.is_empty()
            && self.producers.is_empty()
    }

    pub fn owner_source_index_is_empty(&self, user_id: &UserId) -> bool {
        !self.source_ids_by_owner.contains_key(user_id)
    }

    pub fn owner_producer_index_is_empty(&self, user_id: &UserId) -> bool {
        !self.producer_ids_by_owner.contains_key(user_id)
    }
}
