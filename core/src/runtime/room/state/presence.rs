use crate::runtime::UserInfo;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct UserPresence {
    talking: Option<bool>,
    self_muted: Option<bool>,
    deaf: Option<bool>,
    raising_hand: Option<bool>,
}

impl UserPresence {
    pub(in crate::runtime::room) fn apply_update(&mut self, info: &UserInfo) {
        if let Some(is_talking) = info.is_talking {
            self.talking = Some(is_talking);
        }
        if let Some(is_self_muted) = info.is_self_muted {
            self.self_muted = Some(is_self_muted);
        }
        if let Some(is_deaf) = info.is_deaf {
            self.deaf = Some(is_deaf);
        }
        if let Some(is_raising_hand) = info.is_raising_hand {
            self.raising_hand = Some(is_raising_hand);
        }
    }

    #[must_use]
    pub(in crate::runtime::room) const fn talking(&self) -> Option<bool> {
        self.talking
    }

    #[must_use]
    pub(in crate::runtime::room) const fn self_muted(&self) -> Option<bool> {
        self.self_muted
    }

    #[must_use]
    pub(in crate::runtime::room) const fn deaf(&self) -> Option<bool> {
        self.deaf
    }

    #[must_use]
    pub(in crate::runtime::room) const fn raising_hand(&self) -> Option<bool> {
        self.raising_hand
    }
}
