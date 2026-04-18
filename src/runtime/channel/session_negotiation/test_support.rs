use super::{SessionNegotiation, SessionNegotiationUpdate};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionTransportReady {
    Publish,
    Consume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionNegotiationState {
    AwaitingCapabilities,
    CapabilitiesReady,
    PublishTransportReadyAwaitingCapabilities,
    ConsumeTransportReadyAwaitingCapabilities,
    BothTransportsReadyAwaitingCapabilities,
    PublishReady,
    ConsumeReady,
    Ready,
}

impl SessionNegotiation {
    #[must_use]
    pub(super) fn state_for_test(&self) -> SessionNegotiationState {
        match (
            self.publish_transport_ready,
            self.consume_transport_ready,
            self.capabilities_received,
        ) {
            (false, false, false) => SessionNegotiationState::AwaitingCapabilities,
            (false, false, true) => SessionNegotiationState::CapabilitiesReady,
            (true, false, false) => {
                SessionNegotiationState::PublishTransportReadyAwaitingCapabilities
            }
            (false, true, false) => {
                SessionNegotiationState::ConsumeTransportReadyAwaitingCapabilities
            }
            (true, true, false) => SessionNegotiationState::BothTransportsReadyAwaitingCapabilities,
            (true, false, true) => SessionNegotiationState::PublishReady,
            (false, true, true) => SessionNegotiationState::ConsumeReady,
            (true, true, true) => SessionNegotiationState::Ready,
        }
    }

    pub(in crate::runtime::channel) fn set_client_rtp_capabilities_for_test(
        &mut self,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.capabilities_received = true;
        self.readiness_update(was_consumer_ready)
    }

    pub(in crate::runtime::channel) fn set_transport_ready_for_test(
        &mut self,
        readiness: SessionTransportReady,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        match readiness {
            SessionTransportReady::Publish => self.publish_transport_ready = true,
            SessionTransportReady::Consume => self.consume_transport_ready = true,
        }
        self.readiness_update(was_consumer_ready)
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionNegotiation, SessionNegotiationState, SessionTransportReady};

    #[test]
    fn session_negotiation_transitions_to_ready_when_capabilities_follow_connections() {
        let mut negotiation = SessionNegotiation::default();

        let publish_update =
            negotiation.set_transport_ready_for_test(SessionTransportReady::Publish);
        let consume_update =
            negotiation.set_transport_ready_for_test(SessionTransportReady::Consume);
        let capabilities_update = negotiation.set_client_rtp_capabilities_for_test();

        assert!(publish_update.session_present);
        assert!(!publish_update.became_consumer_ready);
        assert!(consume_update.session_present);
        assert!(!consume_update.became_consumer_ready);
        assert!(capabilities_update.session_present);
        assert!(capabilities_update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state_for_test(), SessionNegotiationState::Ready);
    }

    #[test]
    fn session_negotiation_transitions_to_download_ready_when_download_follows_capabilities() {
        let mut negotiation = SessionNegotiation::default();

        let capabilities_update = negotiation.set_client_rtp_capabilities_for_test();
        let consume_update =
            negotiation.set_transport_ready_for_test(SessionTransportReady::Consume);

        assert!(capabilities_update.session_present);
        assert!(!capabilities_update.became_consumer_ready);
        assert!(consume_update.session_present);
        assert!(consume_update.became_consumer_ready);
        assert!(!negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(
            negotiation.state_for_test(),
            SessionNegotiationState::ConsumeReady
        );
    }

    #[test]
    fn session_negotiation_set_session_negotiated_jumps_directly_to_ready() {
        let mut negotiation = SessionNegotiation::default();

        let update = negotiation.set_session_negotiated();

        assert!(update.session_present);
        assert!(update.became_consumer_ready);
        assert!(negotiation.can_publish());
        assert!(negotiation.can_consume());
        assert_eq!(negotiation.state_for_test(), SessionNegotiationState::Ready);
    }
}
