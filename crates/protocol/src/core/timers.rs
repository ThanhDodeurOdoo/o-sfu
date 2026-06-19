//! request timeout timer identity for the protocol core

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RequestTimeoutId {
    raw: u32,
}

impl RequestTimeoutId {
    #[must_use]
    const fn new(raw: u32) -> Self {
        Self { raw }
    }

    #[must_use]
    pub(super) const fn raw(self) -> u32 {
        self.raw
    }

    #[must_use]
    pub(super) const fn try_from_raw(raw: u32) -> Option<Self> {
        if raw >= REQUEST_TIMEOUT_TIMER_BASE.raw {
            Some(Self::new(raw))
        } else {
            None
        }
    }

    #[must_use]
    pub(super) const fn next(self) -> Self {
        Self::new(self.raw.saturating_add(1))
    }
}

pub(super) const REQUEST_TIMEOUT_TIMER_BASE: RequestTimeoutId = RequestTimeoutId::new(10_000);
