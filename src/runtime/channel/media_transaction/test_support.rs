use o_sfu_protocol::shared::SessionId;

use super::{Channel, PendingPublishTransactions};

impl PendingPublishTransactions {
    pub(in crate::runtime::channel) fn staged_count_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> usize {
        self.staged
            .keys()
            .filter(|key| key.session_id == *session_id && key.connection_id == connection_id)
            .count()
    }
}

impl Channel {
    pub(in crate::runtime::channel) async fn staged_publish_count_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> usize {
        self.pending_publish_transactions
            .lock()
            .await
            .staged_count_for_connection(session_id, connection_id)
    }
}
