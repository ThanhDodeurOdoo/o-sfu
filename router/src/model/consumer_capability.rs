//! Result of capability negotiation as seen by the pure router.
/// Structural compatibility gate used when attaching a consumer.
///
/// The full negotiation algorithm happen outside the router. The router only
/// needs the final yes or no result so it can reject impossible attachments
/// without importing RTP-matching mechanics into the core state machine.
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
