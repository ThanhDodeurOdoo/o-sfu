use crate::runtime::UserInfo;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct UserPresence {
    info: UserInfo,
}

impl UserPresence {
    pub fn apply_update(&mut self, info: &UserInfo) {
        self.info.apply_partial_update(info);
    }

    #[must_use]
    pub fn info(&self) -> &UserInfo {
        &self.info
    }
}
