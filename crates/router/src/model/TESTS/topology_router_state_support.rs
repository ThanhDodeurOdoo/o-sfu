use o_sfu_model::UserId;

use super::RouterAdapterState;

impl RouterAdapterState {
    pub fn remove_user_mappings_for_test(&mut self, user_id: &UserId) {
        self.sessions_by_user.remove(user_id);
        self.transports_by_user.remove(user_id);
    }

    #[must_use]
    pub fn mapped_session_count_for_test(&self) -> usize {
        self.sessions_by_user.len()
    }
}
