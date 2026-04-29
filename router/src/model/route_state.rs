//! Route-control vocabulary owned by the pure router.
//!
//! The router keeps producer-side pauses and consumer-local pauses as different
//! state axes. A producer route state is a source shadow that must be copied to
//! every dependent consumer. A consumer route state is the receiver's own
//! routing choice and must not be overwritten when the producer changes.

/// Source-side routing state for a producer.
///
/// This state belongs to the producer and is shadowed onto every dependent
/// consumer. It exists as a dedicated type so callers cannot confuse a
/// source-side pause with the consumer's local subscription pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerRouteState {
    /// Media from the producer may be routed when each consumer also allows it.
    Active,
    /// The producer is source-paused and every dependent consumer must observe
    /// that shadow independently of its own route state.
    Paused,
}

impl ProducerRouteState {
    /// Returns the legacy boolean view expected by compatibility-facing code.
    ///
    /// New router callers should pass [`ProducerRouteState`] directly at API
    /// boundaries. This helper is for code that still has to emit the old
    /// paused flag to clients or tests.
    #[must_use]
    pub const fn is_paused(self) -> bool {
        matches!(self, Self::Paused)
    }
}

/// Receiver-local routing state for one consumer.
///
/// This state belongs to the consumer, usually as the result of a subscription
/// or download-state choice. It is evaluated alongside the producer shadow but
/// it is not derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRouteState {
    /// The consumer allows routing when the producer shadow also allows it.
    Active,
    /// The consumer locally disables routing for its own receiver path.
    Paused,
}

impl ConsumerRouteState {
    /// Returns the legacy boolean view expected by compatibility-facing code.
    ///
    /// New router callers should pass [`ConsumerRouteState`] directly at API
    /// boundaries. This helper is for code that still has to emit the old
    /// paused flag to clients or tests.
    #[must_use]
    pub const fn is_paused(self) -> bool {
        matches!(self, Self::Paused)
    }
}
