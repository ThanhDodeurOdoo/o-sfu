use super::{PendingPublishTransactions, Room};
use crate::runtime::{
    ConnectionId, TestSourceKind, UserId, media_transport::TransportMediaId,
    source_model::test_support::stream_id_for_source,
};

impl PendingPublishTransactions {
    pub(in crate::runtime::room) fn staged_count_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        self.staged
            .keys()
            .filter(|key| key.user == *user_id && key.connection == connection_id)
            .count()
    }

    pub(in crate::runtime::room) fn staged_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        let stream_id = stream_id_for_source(stream_type);
        self.staged
            .iter()
            .find(|(key, _)| {
                key.user == *user_id && key.connection == connection_id && key.stream == stream_id
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
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        self.pending_publish_transactions
            .lock()
            .await
            .staged_transport_media_id(user_id, connection_id, stream_type)
    }
}
