#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerCapability {
    Compatible,
    Incompatible,
}

impl ConsumerCapability {
    #[must_use]
    pub const fn from_negotiation_result(can_consume: bool) -> Self {
        if can_consume {
            Self::Compatible
        } else {
            Self::Incompatible
        }
    }

    #[must_use]
    pub const fn is_compatible(self) -> bool {
        matches!(self, Self::Compatible)
    }
}
