use o_sfu_router::MediaCapabilities;

use super::super::shared::RoomState;
use crate::runtime::{ConnectionId, UserId};

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PublishPrerequisites {
    connection_id: ConnectionId,
    capabilities: MediaCapabilities,
}

impl RoomState {
    pub fn publish_prerequisites(&self, user_id: &UserId) -> Option<PublishPrerequisites> {
        let user = self.users.get(user_id)?;
        if !user.negotiation.can_publish() {
            return None;
        }
        Some(PublishPrerequisites {
            connection_id: user.connection_id,
            capabilities: self.topology.rtp_capabilities().clone(),
        })
    }
}

impl PublishPrerequisites {
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn capabilities(&self) -> MediaCapabilities {
        self.capabilities.clone()
    }
}
