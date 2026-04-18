use o_sfu_protocol::shared::SessionId;
use o_sfu_router::MediaCapabilities;

use super::super::super::session_negotiation::{SessionNegotiationUpdate, SessionTransportReady};
use super::super::shared::ChannelState;

impl ChannelState {
    pub(in crate::runtime::channel) fn set_client_rtp_capabilities_for_test(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        capabilities: &MediaCapabilities,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.parsed_client_rtp_capabilities = Some(capabilities.clone());
        session.negotiation.set_client_rtp_capabilities_for_test()
    }

    pub(in crate::runtime::channel) fn set_transport_ready_for_test(
        &mut self,
        session_id: &SessionId,
        connection_id: u64,
        readiness: SessionTransportReady,
    ) -> SessionNegotiationUpdate {
        let Some(session) = self.session_mut_for_connection(session_id, connection_id) else {
            return SessionNegotiationUpdate::default();
        };
        session.negotiation.set_transport_ready_for_test(readiness)
    }
}
