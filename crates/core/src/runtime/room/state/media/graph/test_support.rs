use super::{ConsumerKey, RoomMediaGraph};
use crate::runtime::UserId;

impl RoomMediaGraph {
    pub fn contains_pending_consumer_bootstrap(&self, key: &ConsumerKey) -> bool {
        self.consumers.contains_pending_bootstrap(key)
    }

    pub fn publication_state_is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn owner_publication_state_is_empty(&self, user_id: &UserId) -> bool {
        self.sources.owner_state_is_empty(user_id)
    }
}
