use o_sfu_router::MediaStream as RouterRtpParameters;
#[cfg(test)]
use {
    super::{PendingPublishTransactions, Room},
    crate::runtime::{
        ConnectionId, TestSourceKind, UserId, media_transport::TransportMediaId,
        room::state::ValidatedPublishDescriptor, source_model::test_support::stream_id_for_source,
        sync::lock_unpoisoned,
    },
};

use super::{PendingPublishTransaction, RoomUserOperation};
use crate::runtime::source_model::UserStreamId;

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
    pub(in crate::runtime::room) fn stage_duplicate_after_next_publish_reservation_for_test(
        &self,
        transport_media_id: TransportMediaId,
    ) {
        *lock_unpoisoned(&self.duplicate_staged_publish_after_reservation) =
            Some(transport_media_id);
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target) = None;
    }

    pub(in crate::runtime::room) fn duplicate_staged_publish_cleanup_target_for_test(
        &self,
    ) -> Option<TransportMediaId> {
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target)
    }

    pub(in crate::runtime::room) fn inject_duplicate_staged_publish_after_reservation_for_test(
        &self,
        descriptor: &ValidatedPublishDescriptor,
        cleanup_target: TransportMediaId,
    ) {
        let Some(staged_transport_media_id) =
            lock_unpoisoned(&self.duplicate_staged_publish_after_reservation).take()
        else {
            return;
        };
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target) = Some(cleanup_target);
        let transaction =
            PendingPublishTransaction::new(descriptor.clone(), staged_transport_media_id);
        let key = transaction.key();
        let mut pending_publish_transactions = lock_unpoisoned(&self.pending_publish_transactions);
        assert!(
            !pending_publish_transactions.staged.contains_key(&key),
            "test duplicate staged publish slot should be empty before injection"
        );
        pending_publish_transactions.staged.insert(key, transaction);
    }

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
