//! User negotiation readiness
//!
//! This module keeps the room-facing readiness contract small while parsed
//! client RTP capabilities stay stored next to the user in room state. The
//! negotiation state only answers two questions for its callers:
//!
//! - can this user publish?
//! - can this user consume?
//!
//! It also reports the one transition where a user becomes ready to consume so
//! the caller can set up missing consumers exactly once.

/// Returned after each state transition to tell the caller what changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserNegotiationUpdate {
    Applied,
    BecameConsumerReady,
}

/// Room-facing readiness state for one live user connection.
///
/// Transport answer validation and parsed client RTP capabilities are folded
/// into one ready state, while the capability payload itself lives outside this
/// type in room state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum UserNegotiation {
    #[default]
    AwaitingAnswer,
    Ready,
}

impl UserNegotiation {
    #[must_use]
    pub(super) const fn can_publish(self) -> bool {
        matches!(self, Self::Ready)
    }

    #[must_use]
    pub(super) const fn can_consume(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Marks the user as fully negotiated for the current connection.
    ///
    /// This is the fast path used by the live websocket answer flow once the
    /// runtime has already validated and stored the parsed client capabilities.
    /// Callers still get the edge-triggered consumer readiness signal.
    pub(super) fn mark_ready(&mut self) -> UserNegotiationUpdate {
        let update = if self.can_consume() {
            UserNegotiationUpdate::Applied
        } else {
            UserNegotiationUpdate::BecameConsumerReady
        };
        *self = Self::Ready;
        update
    }
}
