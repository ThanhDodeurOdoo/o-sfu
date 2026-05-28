#[cfg(test)]
use super::{ConsumerKey, RoomMediaGraph};
#[cfg(test)]
use crate::runtime::UserId;

#[cfg(test)]
impl RoomMediaGraph {
    pub fn contains_pending_consumer_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.consumers.pending_consumer_bootstraps.contains(key)
    }

    pub fn publication_state_is_empty(&self) -> bool {
        self.sources.descriptors.is_empty()
            && self.sources.source_ids_by_owner_stream.is_empty()
            && self.sources.source_ids_by_owner.is_empty()
            && self.sources.producer_id_by_source_id.is_empty()
            && self.sources.producer_ids_by_owner.is_empty()
            && self.sources.producers.is_empty()
            && self.sources.source_transport_media_index.is_empty()
    }

    pub fn owner_publication_state_is_empty(&self, user_id: &UserId) -> bool {
        !self.sources.source_ids_by_owner.contains_key(user_id)
            && !self.sources.producer_ids_by_owner.contains_key(user_id)
    }
}
