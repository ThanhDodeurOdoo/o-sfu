use o_sfu_router::MediaStream as RouterRtpParameters;
#[cfg(test)]
use {
    super::{Room, StagedPublishRegistry},
    crate::engine::{
        ConnectionId, TestSourceKind, UserId,
        media_transport::{TransportMediaId, TransportSessionKey},
        room::media_graph::ValidatedPublish,
        source_model::test_support::stream_id_for_source,
        sync::lock_unpoisoned,
    },
};

use super::{RoomUserOperation, StagedPublish};
use crate::engine::source_model::UserStreamId;

impl StagedPublish {
    pub(crate) async fn commit_with_parameters(
        self,
        operation: RoomUserOperation<'_>,
        rtp: RouterRtpParameters,
    ) -> Option<UserStreamId> {
        self.commit_with_negotiated_parameters(operation, rtp, Vec::new())
            .await
    }
}

#[cfg(test)]
impl StagedPublishRegistry {
    pub fn staged_count(&self, user_id: &UserId, connection_id: ConnectionId) -> usize {
        self.staged
            .keys()
            .filter(|key| key.user == *user_id && key.connection == connection_id)
            .count()
    }

    pub fn staged_media_id(
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
    pub(crate) fn stage_next_duplicate_for_test(&self, transport_media_id: TransportMediaId) {
        *lock_unpoisoned(&self.duplicate_staged_publish_after_reservation) =
            Some(transport_media_id);
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target) = None;
    }

    pub(crate) fn duplicate_cleanup_target_for_test(&self) -> Option<TransportMediaId> {
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target)
    }

    pub(super) fn inject_next_duplicate_for_test(
        &self,
        descriptor: &ValidatedPublish,
        session_key: TransportSessionKey,
        cleanup_target: TransportMediaId,
    ) {
        let Some(staged_transport_media_id) =
            lock_unpoisoned(&self.duplicate_staged_publish_after_reservation).take()
        else {
            return;
        };
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target) = Some(cleanup_target);
        let transaction =
            StagedPublish::new(descriptor.clone(), session_key, staged_transport_media_id);
        let key = transaction.key();
        let mut staged_publish_registry = lock_unpoisoned(&self.staged_publish_registry);
        assert!(
            !staged_publish_registry.staged.contains_key(&key),
            "test duplicate staged publish slot should be empty before injection"
        );
        staged_publish_registry.staged.insert(key, transaction);
    }

    #[allow(
        clippy::unused_async,
        reason = "test facade stays async to match room inspection helpers used by existing scenarios"
    )]
    pub(crate) async fn staged_count(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        lock_unpoisoned(&self.staged_publish_registry).staged_count(user_id, connection_id)
    }

    #[allow(
        clippy::unused_async,
        reason = "test facade stays async to match room inspection helpers used by existing scenarios"
    )]
    pub(crate) async fn staged_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        lock_unpoisoned(&self.staged_publish_registry).staged_media_id(
            user_id,
            connection_id,
            stream_type,
        )
    }
}
