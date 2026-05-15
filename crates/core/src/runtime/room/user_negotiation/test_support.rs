use super::{UserNegotiation, UserNegotiationUpdate, UserTransportReady};

impl UserNegotiation {
    pub fn set_client_rtp_capabilities_for_test(&mut self) -> UserNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = self.state.with_capabilities_received();
        self.readiness_update(was_consumer_ready)
    }

    pub fn set_transport_ready_for_test(
        &mut self,
        readiness: UserTransportReady,
    ) -> UserNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = self.state.with_transport_ready(readiness);
        self.readiness_update(was_consumer_ready)
    }
}
