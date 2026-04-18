use o_sfu_protocol::shared::SessionId;

use super::{super::shared::ChannelState, PendingConsumerBootstrapTarget};

impl ChannelState {
    pub(in crate::runtime::channel) fn missing_consumer_targets(
        &self,
        session_id: &SessionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        let Some(session) = self.sessions.get(session_id) else {
            return Vec::new();
        };
        if !session.negotiation.can_consume() {
            return Vec::new();
        }
        self.collect_missing_consumer_targets(session_id, session.connection_id)
    }
}
