use super::RouterAdapterState;

impl RouterAdapterState {
    #[must_use]
    pub fn mapped_session_count_for_test(&self) -> usize {
        self.sessions_by_user.len()
    }
}
