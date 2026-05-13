use o_sfu_router::MediaCapabilities;

use super::super::{
    super::user_negotiation::{UserNegotiationUpdate, UserTransportReady},
    shared::RoomState,
};
use crate::runtime::{ConnectionId, UserId};

impl RoomState {
    pub(in crate::runtime::room) fn set_client_rtp_capabilities_for_test(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: &MediaCapabilities,
    ) -> UserNegotiationUpdate {
        let Some(user) = self.user_mut_for_connection(user_id, connection_id) else {
            return UserNegotiationUpdate::default();
        };
        user.parsed_client_rtp_capabilities = Some(capabilities.clone());
        user.negotiation.set_client_rtp_capabilities_for_test()
    }

    pub(in crate::runtime::room) fn set_transport_ready_for_test(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        readiness: UserTransportReady,
    ) -> UserNegotiationUpdate {
        let Some(user) = self.user_mut_for_connection(user_id, connection_id) else {
            return UserNegotiationUpdate::default();
        };
        user.negotiation.set_transport_ready_for_test(readiness)
    }
}
