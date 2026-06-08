use o_sfu_router::MediaStream as RouterRtpParameters;
#[cfg(any(test, feature = "testing-transport"))]
use {
    super::Room,
    crate::engine::{ConnectionId, UserId},
};

use super::{RoomUserOperation, StagedPublish};
use crate::engine::source_model::UserStreamId;
#[cfg(test)]
use crate::engine::{
    TestSourceKind, media_transport::TransportMediaId, room::media_graph::ValidatedPublish,
    sync::lock_unpoisoned,
};

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

#[cfg(any(test, feature = "testing-transport"))]
impl Room {
    #[allow(
        clippy::unused_async,
        reason = "test facade stays async to match room inspection helpers used by existing scenarios"
    )]
    pub(crate) async fn staged_count(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        self.staged_publishes.staged_count(user_id, connection_id)
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
        cleanup_target: TransportMediaId,
    ) {
        let Some(staged_transport_media_id) =
            lock_unpoisoned(&self.duplicate_staged_publish_after_reservation).take()
        else {
            return;
        };
        *lock_unpoisoned(&self.duplicate_staged_publish_cleanup_target) = Some(cleanup_target);
        let transaction = StagedPublish::new(descriptor.clone(), staged_transport_media_id);
        self.staged_publishes.insert_for_test(transaction);
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
        self.staged_publishes
            .staged_media_id(user_id, connection_id, stream_type)
    }
}
