//! route-control vocabulary owned by the pure router
//!
//! producer-side pauses and consumer-local pauses are different state axes
//! producer route state is a source shadow copied to every dependent consumer
//! consumer route state is the receiver's own routing choice

/// source-side routing state for a producer
///
/// this state belongs to the producer and is shadowed onto every dependent
/// consumer so callers cannot confuse it with local subscription pause
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerRouteState {
    /// media from the producer may be routed when each consumer also allows it
    Active,
    /// every dependent consumer must observe this source pause independently
    /// of its own route state
    Paused,
}

impl ProducerRouteState {
    /// legacy boolean view expected by compatibility-facing code
    ///
    /// new router callers should pass [`ProducerRouteState`] directly
    #[must_use]
    pub const fn is_paused(self) -> bool {
        matches!(self, Self::Paused)
    }
}

/// receiver-local routing state for one consumer
///
/// this state belongs to the consumer and is evaluated alongside the producer
/// shadow without being derived from it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRouteState {
    /// the consumer allows routing when the producer shadow also allows it
    Active,
    /// the consumer locally disables routing for its own receiver path
    Paused,
}

impl ConsumerRouteState {
    /// legacy boolean view expected by compatibility-facing code
    ///
    /// new router callers should pass [`ConsumerRouteState`] directly
    #[must_use]
    pub const fn is_paused(self) -> bool {
        matches!(self, Self::Paused)
    }
}
