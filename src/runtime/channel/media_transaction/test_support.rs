use o_sfu_protocol::shared::{SessionId, StreamType};

use super::{Channel, PendingPublishTransactions};
use crate::runtime::{ConnectionId, transport_adapter::TransportMediaId};

impl PendingPublishTransactions {
    pub(in crate::runtime::channel) fn staged_count_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> usize {
        self.staged
            .keys()
            .filter(|key| key.session_id == *session_id && key.connection_id == connection_id)
            .count()
    }

    pub(in crate::runtime::channel) fn staged_transport_media_id(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.staged
            .iter()
            .find(|(key, _)| {
                key.session_id == *session_id
                    && key.connection_id == connection_id
                    && key.stream_type == stream_type
            })
            .map(|(_, transaction)| transaction.transport_media_id())
    }
}

impl Channel {
    pub(in crate::runtime::channel) async fn staged_publish_count_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> usize {
        self.pending_publish_transactions
            .lock()
            .await
            .staged_count_for_connection(session_id, connection_id)
    }

    pub(in crate::runtime::channel) async fn staged_publish_transport_media_id(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.pending_publish_transactions
            .lock()
            .await
            .staged_transport_media_id(session_id, connection_id, stream_type)
    }
}
