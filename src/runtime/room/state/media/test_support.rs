use o_sfu_protocol::shared::UserId;
use o_sfu_router::MediaCapabilities;

use super::super::shared::RoomState;
use crate::runtime::ConnectionId;

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PublishPrerequisites {
    connection_id: ConnectionId,
    router_capabilities: MediaCapabilities,
}

impl RoomState {
    pub(in crate::runtime::room) fn publish_prerequisites(
        &self,
        user_id: &UserId,
    ) -> Option<PublishPrerequisites> {
        let user = self.users.get(user_id)?;
        if !user.negotiation.can_publish() {
            return None;
        }
        Some(PublishPrerequisites {
            connection_id: user.connection_id,
            router_capabilities: self.topology.rtp_capabilities().clone(),
        })
    }
}

impl PublishPrerequisites {
    pub(in crate::runtime::room) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(in crate::runtime::room) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }
}
