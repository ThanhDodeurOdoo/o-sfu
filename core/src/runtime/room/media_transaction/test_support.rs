use super::{PendingPublishTransactions, Room};
use crate::runtime::{ConnectionId, StreamType, UserId, transport_adapter::TransportMediaId};

impl PendingPublishTransactions {
    pub(in crate::runtime::room) fn staged_count_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        self.staged
            .keys()
            .filter(|key| key.user_id == *user_id && key.connection_id == connection_id)
            .count()
    }

    pub(in crate::runtime::room) fn staged_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.staged
            .iter()
            .find(|(key, _)| {
                key.user_id == *user_id
                    && key.connection_id == connection_id
                    && key.stream_type == stream_type
            })
            .map(|(_, transaction)| transaction.transport_media_id())
    }
}

impl Room {
    pub(in crate::runtime::room) async fn staged_publish_count_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        self.pending_publish_transactions
            .lock()
            .await
            .staged_count_for_connection(user_id, connection_id)
    }

    pub(in crate::runtime::room) async fn staged_publish_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<TransportMediaId> {
        self.pending_publish_transactions
            .lock()
            .await
            .staged_transport_media_id(user_id, connection_id, stream_type)
    }
}
