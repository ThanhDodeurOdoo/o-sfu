use o_sfu_protocol::shared::SessionId;
use o_sfu_router::MediaCapabilities;

use super::super::shared::ChannelState;
use crate::runtime::ConnectionId;

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PublishPrerequisites {
    connection_id: ConnectionId,
    router_capabilities: MediaCapabilities,
}

impl ChannelState {
    pub(in crate::runtime::channel) fn publish_prerequisites(
        &self,
        session_id: &SessionId,
    ) -> Option<PublishPrerequisites> {
        let session = self.sessions.get(session_id)?;
        if !session.negotiation.can_publish() {
            return None;
        }
        Some(PublishPrerequisites {
            connection_id: session.connection_id,
            router_capabilities: self.topology.rtp_capabilities().clone(),
        })
    }
}

impl PublishPrerequisites {
    pub(in crate::runtime::channel) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(in crate::runtime::channel) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }
}
