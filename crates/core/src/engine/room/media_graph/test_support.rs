#[cfg(test)]
use super::{ConsumerKey, RoomMediaGraph};
#[cfg(test)]
use crate::engine::UserId;

#[cfg(test)]
impl RoomMediaGraph {
    pub fn contains_pending_consumer_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.consumers.pending_bootstraps.contains(key)
    }

    pub fn publication_state_is_empty(&self) -> bool {
        self.sources.descriptors.is_empty()
            && self.sources.id_by_key.is_empty()
            && self.sources.ids_by_owner.is_empty()
            && self.sources.producer_by_source.is_empty()
            && self.sources.producers_by_owner.is_empty()
            && self.sources.producers.is_empty()
            && self.sources.by_transport_media.is_empty()
    }

    pub fn owner_publication_state_is_empty(&self, user_id: &UserId) -> bool {
        !self.sources.ids_by_owner.contains_key(user_id)
            && !self.sources.producers_by_owner.contains_key(user_id)
    }
}
