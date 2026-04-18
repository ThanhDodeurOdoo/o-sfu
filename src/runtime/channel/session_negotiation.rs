/// Tracks the two independent axes of session readiness: semantic transport
/// readiness for publishing/consuming and RTP capability exchange.
///
/// A session can only publish once its publish transport is ready, and can only
/// consume once both the consume transport is ready and RTP capabilities have
/// been received. The two axes can advance in any order; the state machine
/// merges them into a single enum so every legal combination is represented.
///
/// Each state is a combination of two independents axes:
/// 1. Transport readiness: None, Publish only, Consume only, or Both.
/// 2. RTP Capabilities: Not yet received, or Ready.
///
/// ```text
///                      TRANSPORT READINESS
///               None        Publish (P) Consume     Both (P)
///            ┌─────────────┬───────────┬────────────┬────────────┐
/// NO CAPS    │ `Awaiting`  │ `PubConn` │ `ConConn`  │ `TransConn`│
///            ├─────────────┼───────────┼────────────┼────────────┤
/// CAPS READY │ `CapsReady` │ `PubReady`│ `ConReady` │  `Ready`   │
///            └─────────────┴───────────┴────────────┴────────────┘
///                            (P)        (C)        (P, C)
/// ```
///
/// (P) = `can_publish()` is true
/// (C) = `can_consume()` is true
///
/// **Gate conditions:**
/// - `can_publish()`: true when the publish transport is ready, regardless of whether capabilities have arrived.
/// - `can_consume()`: true only when both the consume transport is ready and capabilities are available (`ConsumeReady` or `Ready`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SessionNegotiationState {
    /// Neither transport ready nor capabilities received.
    AwaitingCapabilities,
    /// Capabilities received, but no transport connected yet.
    #[cfg(test)]
    CapabilitiesReady,
    /// Publish transport ready; still waiting for capabilities.
    #[cfg(test)]
    PublishTransportReadyAwaitingCapabilities,
    /// Consume transport ready; still waiting for capabilities.
    #[cfg(test)]
    ConsumeTransportReadyAwaitingCapabilities,
    /// Both transports ready; still waiting for capabilities.
    #[cfg(test)]
    BothTransportsReadyAwaitingCapabilities,
    /// Publish transport ready and capabilities received; consume pending.
    #[cfg(test)]
    PublishReady,
    /// Consume transport ready and capabilities received; publish pending.
    #[cfg(test)]
    ConsumeReady,
    /// Fully negotiated: both transports ready and capabilities received.
    Ready,
}

// TODO: CLEANUP TESTING
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionTransportReady {
    Publish,
    Consume,
}

/// Returned after each state transition to tell the caller what changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionNegotiationUpdate {
    /// Whether the session was found and the transition was applied.
    pub(crate) session_present: bool,
    /// True only on the exact transition that crosses the `can_consume()` threshold,
    /// so the channel knows to start creating consumers for this session.
    pub(crate) became_consumer_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionNegotiation {
    state: SessionNegotiationState,
}

impl Default for SessionNegotiation {
    fn default() -> Self {
        Self {
            state: SessionNegotiationState::AwaitingCapabilities,
        }
    }
}

impl SessionNegotiation {
    #[cfg(test)]
    #[must_use]
    pub(super) fn state(&self) -> &SessionNegotiationState {
        &self.state
    }

    #[must_use]
    pub(super) fn can_publish(&self) -> bool {
        #[cfg(test)]
        {
            matches!(
                self.state,
                SessionNegotiationState::PublishTransportReadyAwaitingCapabilities
                    | SessionNegotiationState::BothTransportsReadyAwaitingCapabilities
                    | SessionNegotiationState::PublishReady
                    | SessionNegotiationState::Ready
            )
        }
        #[cfg(not(test))]
        {
            matches!(self.state, SessionNegotiationState::Ready)
        }
    }

    #[must_use]
    pub(super) fn can_consume(&self) -> bool {
        #[cfg(test)]
        {
            matches!(
                self.state,
                SessionNegotiationState::ConsumeReady | SessionNegotiationState::Ready
            )
        }
        #[cfg(not(test))]
        {
            matches!(self.state, SessionNegotiationState::Ready)
        }
    }

    #[cfg(test)]
    pub(super) fn set_client_rtp_capabilities(&mut self) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = match &self.state {
            SessionNegotiationState::AwaitingCapabilities
            | SessionNegotiationState::CapabilitiesReady => {
                SessionNegotiationState::CapabilitiesReady
            }
            SessionNegotiationState::PublishTransportReadyAwaitingCapabilities => {
                SessionNegotiationState::PublishReady
            }
            SessionNegotiationState::ConsumeTransportReadyAwaitingCapabilities
            | SessionNegotiationState::ConsumeReady => SessionNegotiationState::ConsumeReady,
            SessionNegotiationState::BothTransportsReadyAwaitingCapabilities => {
                SessionNegotiationState::Ready
            }
            SessionNegotiationState::PublishReady => SessionNegotiationState::PublishReady,
            SessionNegotiationState::Ready => SessionNegotiationState::Ready,
        };
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }

    #[cfg(test)]
    pub(super) fn set_transport_ready(
        &mut self,
        readiness: SessionTransportReady,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = match (&self.state, readiness) {
            (SessionNegotiationState::AwaitingCapabilities, SessionTransportReady::Publish) => {
                SessionNegotiationState::PublishTransportReadyAwaitingCapabilities
            }
            (SessionNegotiationState::AwaitingCapabilities, SessionTransportReady::Consume) => {
                SessionNegotiationState::ConsumeTransportReadyAwaitingCapabilities
            }
            (SessionNegotiationState::CapabilitiesReady, SessionTransportReady::Publish) => {
                SessionNegotiationState::PublishReady
            }
            (SessionNegotiationState::CapabilitiesReady, SessionTransportReady::Consume) => {
                SessionNegotiationState::ConsumeReady
            }
            (
                SessionNegotiationState::PublishTransportReadyAwaitingCapabilities,
                SessionTransportReady::Consume,
            )
            | (
                SessionNegotiationState::ConsumeTransportReadyAwaitingCapabilities,
                SessionTransportReady::Publish,
            ) => SessionNegotiationState::BothTransportsReadyAwaitingCapabilities,
            (SessionNegotiationState::PublishReady, SessionTransportReady::Consume)
            | (SessionNegotiationState::ConsumeReady, SessionTransportReady::Publish) => {
                SessionNegotiationState::Ready
            }
            _ => self.state.clone(),
        };
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }

    pub(super) fn set_session_negotiated(&mut self) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = SessionNegotiationState::Ready;
        SessionNegotiationUpdate {
            session_present: true,
            became_consumer_ready: !was_consumer_ready && self.can_consume(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionNegotiation, SessionNegotiationState, SessionTransportReady};

    #[test]
    fn session_negotiation_transitions_to_ready_when_capabilities_follow_connections() {
        let mut negotiation = SessionNegotiation::default();

        let publish_update = negotiation.set_transport_ready(SessionTransportReady::Publish);
        let consume_update = negotiation.set_transport_ready(SessionTransportReady::Consume);
        let capabilities_update = negotiation.set_client_rtp_capabilities();

        assert!(publish_update.session_present);
        assert!(!publish_update.became_consumer_ready);
        assert!(consume_update.session_present);
        assert!(!consume_update.became_consumer_ready);
        assert!(capabilities_update.session_present);
        assert!(capabilities_update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state(), &SessionNegotiationState::Ready);
    }

    #[test]
    fn session_negotiation_transitions_to_download_ready_when_download_follows_capabilities() {
        let mut negotiation = SessionNegotiation::default();

        let capabilities_update = negotiation.set_client_rtp_capabilities();
        let consume_update = negotiation.set_transport_ready(SessionTransportReady::Consume);

        assert!(capabilities_update.session_present);
        assert!(!capabilities_update.became_consumer_ready);
        assert!(consume_update.session_present);
        assert!(consume_update.became_consumer_ready);
        assert!(!negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state(), &SessionNegotiationState::ConsumeReady);
    }

    #[test]
    fn session_negotiation_set_session_negotiated_jumps_directly_to_ready() {
        let mut negotiation = SessionNegotiation::default();

        let update = negotiation.set_session_negotiated();

        assert!(update.session_present);
        assert!(update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state(), &SessionNegotiationState::Ready);
    }
}
