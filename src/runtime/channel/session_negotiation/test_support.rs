use super::{SessionNegotiation, SessionNegotiationUpdate, SessionTransportReady};

impl SessionNegotiation {
    pub(in crate::runtime::channel) fn set_client_rtp_capabilities_for_test(
        &mut self,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = self.state.with_capabilities_received();
        self.readiness_update(was_consumer_ready)
    }

    pub(in crate::runtime::channel) fn set_transport_ready_for_test(
        &mut self,
        readiness: SessionTransportReady,
    ) -> SessionNegotiationUpdate {
        let was_consumer_ready = self.can_consume();
        self.state = self.state.with_transport_ready(readiness);
        self.readiness_update(was_consumer_ready)
    }
}
