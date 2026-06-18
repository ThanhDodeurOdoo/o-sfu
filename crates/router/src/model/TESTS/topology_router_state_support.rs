use o_sfu_model::UserId;

use super::RouterAdapterState;
#[cfg(test)]
use crate::model::{MediaCapabilities, RouterId};

impl RouterAdapterState {
    #[cfg(test)]
    #[must_use]
    pub fn new_for_test(router_id: RouterId) -> Self {
        Self::new(router_id, MediaCapabilities::new(Vec::new(), Vec::new()))
    }

    pub fn remove_user_mappings_for_test(&mut self, user_id: &UserId) {
        self.sessions_by_user.remove(user_id);
        self.transports_by_user.remove(user_id);
    }

    #[must_use]
    pub fn mapped_session_count_for_test(&self) -> usize {
        self.sessions_by_user.len()
    }
}
