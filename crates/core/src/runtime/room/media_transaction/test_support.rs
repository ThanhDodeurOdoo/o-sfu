use o_sfu_router::MediaStream as RouterRtpParameters;

use super::{PendingPublishTransaction, RoomUserOperation};
#[cfg(test)]
use super::{PendingPublishTransactions, Room};
use crate::runtime::source_model::UserStreamId;
#[cfg(test)]
use crate::runtime::{
    ConnectionId, TestSourceKind, UserId, media_transport::TransportMediaId,
    source_model::test_support::stream_id_for_source, sync::lock_unpoisoned,
};

impl PendingPublishTransaction {
    pub(in crate::runtime::room) async fn commit_with_parameters(
        self,
        operation: RoomUserOperation<'_>,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<UserStreamId> {
        self.commit_with_parameters_and_upload_encodings(
            operation,
            consumable_rtp_parameters,
            Vec::new(),
        )
        .await
    }
}

#[cfg(test)]
impl PendingPublishTransactions {
    pub fn staged_count_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        self.staged
            .keys()
            .filter(|key| key.user == *user_id && key.connection == connection_id)
            .count()
    }

    pub fn staged_transport_media_id(
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

#[cfg(test)]
impl Room {
    #[allow(
        clippy::unused_async,
        reason = "test facade stays async to match room inspection helpers used by existing scenarios"
    )]
    pub(in crate::runtime::room) async fn staged_publish_count_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        lock_unpoisoned(&self.pending_publish_transactions)
            .staged_count_for_connection(user_id, connection_id)
    }

    #[allow(
        clippy::unused_async,
        reason = "test facade stays async to match room inspection helpers used by existing scenarios"
    )]
    pub(in crate::runtime::room) async fn staged_publish_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        lock_unpoisoned(&self.pending_publish_transactions).staged_transport_media_id(
            user_id,
            connection_id,
            stream_type,
        )
    }
}
