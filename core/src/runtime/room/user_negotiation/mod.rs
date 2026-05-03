//! User negotiation readiness
//!
//! This module keeps the room-facing publish and consume readiness contract
//! small and explicit while the actual client RTP capabillities stay stored next
//! to the user in room state. The negotiation state machine only answers
//! two questions for its callers:
//!
//! - can this user publish?
//! - can this user consume ?
//!
//! It also reports the one transition where a user becomes consumer-ready so
//! the caller can bootstrap missing consumers exactly once when readiness
//! crosses that boundary.

#[cfg(test)]
mod test_support;

/// Identifies which transport side just became usable for one user.
///
/// The room tests use this to drive the same production readiness state
/// machine as the live runtime. Keeping the transition input typed
/// because publish and consume readiness unlock different work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserTransportReady {
    Publish,
    Consume,
}

/// Returned after each state transition to tell the caller what changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct UserNegotiationUpdate {
    /// Whether the user was found and the transition was applied.
    pub(crate) session_present: bool,
    /// True only on the exact transition that crosses the `can_consume()` threshold,
    /// so the room knows to start creating consumers for this user.
    pub(crate) became_consumer_ready: bool,
}

/// Explicit readiness states for one live room user.
///
/// The room runtime cares about legal readiness transition.
/// This enum keeps the intermediate states visible so later renegotiation work
/// has one authoritative model.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum UserNegotiationState {
    #[default]
    AwaitingCapabilities,
    CapabilitiesReady,
    PublishTransportReadyAwaitingCapabilities,
    ConsumeTransportReadyAwaitingCapabilities,
    BothTransportsReadyAwaitingCapabilities,
    PublishReady,
    ConsumeReady,
    Ready,
}

impl UserNegotiationState {
    const fn can_publish(self) -> bool {
        matches!(
            self,
            Self::PublishTransportReadyAwaitingCapabilities
                | Self::BothTransportsReadyAwaitingCapabilities
                | Self::PublishReady
                | Self::Ready
        )
    }

    const fn can_consume(self) -> bool {
        matches!(self, Self::ConsumeReady | Self::Ready)
    }

    const fn with_capabilities_received(self) -> Self {
        match self {
            Self::AwaitingCapabilities => Self::CapabilitiesReady,
            Self::PublishTransportReadyAwaitingCapabilities => Self::PublishReady,
            Self::ConsumeTransportReadyAwaitingCapabilities => Self::ConsumeReady,
            Self::BothTransportsReadyAwaitingCapabilities => Self::Ready,
            state => state,
        }
    }

    const fn with_transport_ready(self, readiness: UserTransportReady) -> Self {
        match (self, readiness) {
            (Self::AwaitingCapabilities, UserTransportReady::Publish) => {
                Self::PublishTransportReadyAwaitingCapabilities
            }
            (Self::AwaitingCapabilities, UserTransportReady::Consume) => {
                Self::ConsumeTransportReadyAwaitingCapabilities
            }
            (Self::CapabilitiesReady, UserTransportReady::Publish) => Self::PublishReady,
            (Self::CapabilitiesReady, UserTransportReady::Consume) => Self::ConsumeReady,
            (Self::PublishTransportReadyAwaitingCapabilities, UserTransportReady::Consume)
            | (Self::ConsumeTransportReadyAwaitingCapabilities, UserTransportReady::Publish) => {
                Self::BothTransportsReadyAwaitingCapabilities
            }
            (Self::PublishReady, UserTransportReady::Consume)
            | (Self::ConsumeReady, UserTransportReady::Publish) => Self::Ready,
            (state, _) => state,
        }
    }
}

/// Room-facing readiness state for one live user connection.
///
/// This stays intentionelly narrow: transport readiness and parsed client RTP
/// capabilities are folded into publish or consume readiness, but the parsed
/// capabillity payload itself lives outside this type in room state. That
/// split keeps lifecycle transitions here and avoid turning the readiness
/// state machine into a bag of protocol-shaped data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct UserNegotiation {
    state: UserNegotiationState,
}

impl UserNegotiation {
    #[must_use]
    pub(super) const fn can_publish(&self) -> bool {
        self.state.can_publish()
    }

    #[must_use]
    pub(super) const fn can_consume(&self) -> bool {
        self.state.can_consume()
    }

    /// Marks the user as fully negotiated for the current connection.
    ///
    /// This is the fast path used by the live websocket answer flow once the
    /// runtime has already validated and stored the parsed client capabilities.
    /// Callers still get the edge-triggered `became_consumer_ready` signal.
    pub(super) fn set_user_negotiated(&mut self) -> UserNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = self
            .state
            .with_transport_ready(UserTransportReady::Publish)
            .with_transport_ready(UserTransportReady::Consume)
            .with_capabilities_received();
        self.readiness_update(was_consumer_ready)
    }

    fn readiness_update(&self, was_consumer_ready: bool) -> UserNegotiationUpdate {
        UserNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        UserNegotiation, UserNegotiationState,
        UserTransportReady::{Consume, Publish},
    };

    #[test]
    fn session_negotiation_transitions_to_ready_when_capabilities_follow_connections() {
        let mut negotiation = UserNegotiation::default();

        let publish_update = negotiation.set_transport_ready_for_test(Publish);
        let consume_update = negotiation.set_transport_ready_for_test(Consume);
        let capabilities_update = negotiation.set_client_rtp_capabilities_for_test();

        assert!(publish_update.session_present);
        assert!(!publish_update.became_consumer_ready);
        assert!(consume_update.session_present);
        assert!(!consume_update.became_consumer_ready);
        assert!(capabilities_update.session_present);
        assert!(capabilities_update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state, UserNegotiationState::Ready);
    }

    #[test]
    fn session_negotiation_transitions_to_download_ready_when_download_follows_capabilities() {
        let mut negotiation = UserNegotiation::default();

        let capabilities_update = negotiation.set_client_rtp_capabilities_for_test();
        let consume_update = negotiation.set_transport_ready_for_test(Consume);

        assert!(capabilities_update.session_present);
        assert!(!capabilities_update.became_consumer_ready);
        assert!(consume_update.session_present);
        assert!(consume_update.became_consumer_ready);
        assert!(!negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state, UserNegotiationState::ConsumeReady);
    }

    #[test]
    fn session_negotiation_preserves_publish_readiness_before_capabilities_arrive() {
        let mut negotiation = UserNegotiation::default();

        let publish_update = negotiation.set_transport_ready_for_test(Publish);

        assert!(publish_update.session_present);
        assert!(!publish_update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(!negotiation.can_consume());
        assert_eq!(
            negotiation.state,
            UserNegotiationState::PublishTransportReadyAwaitingCapabilities
        );
    }

    #[test]
    fn user_negotiation_set_user_negotiated_jumps_directly_to_ready() {
        let mut negotiation = UserNegotiation::default();

        let update = negotiation.set_user_negotiated();

        assert!(update.session_present);
        assert!(update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state, UserNegotiationState::Ready);
    }

    #[test]
    fn session_negotiation_only_reports_consumer_readiness_once() {
        let mut negotiation = UserNegotiation::default();

        let _ = negotiation.set_transport_ready_for_test(Consume);
        let first_capabilities_update = negotiation.set_client_rtp_capabilities_for_test();
        let second_capabilities_update = negotiation.set_client_rtp_capabilities_for_test();

        assert!(first_capabilities_update.became_consumer_ready);
        assert!(!second_capabilities_update.became_consumer_ready);
        assert_eq!(negotiation.state, UserNegotiationState::ConsumeReady);
    }
}
