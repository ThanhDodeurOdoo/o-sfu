use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Mutex,
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use tracing::warn;

#[cfg(test)]
use crate::engine::{TestSourceKind, source_model::test_support::stream_id_for_source};
use crate::{
    TransportEffectOutcome,
    engine::{
        ConnectionId, UserId,
        media_transport::{AppliedSessionAnswer, SessionUploadEncoding, TransportMediaId},
        room::{
            RoomUserOperation,
            cleanup::TransportCleanupOperation,
            effects::{self, batch::RoomEffectContext},
            media_graph::ValidatedPublish,
        },
        source_model::UserStreamId,
        sync::lock_unpoisoned,
    },
};

#[derive(Debug, Default)]
pub struct StagedPublishes {
    staged: Mutex<BTreeMap<StagedPublishKey, StagedPublish>>,
}

type StagedPublishKey = (UserId, ConnectionId, UserStreamId);

#[derive(Debug)]
pub struct StagedPublish {
    descriptor: ValidatedPublish,
    media: TransportMediaId,
    armed: bool,
}

impl StagedPublishes {
    pub fn contains(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        lock_unpoisoned(&self.staged).contains_key(&staged_publish_key(
            user_id,
            connection_id,
            stream_id,
        ))
    }

    pub async fn stage(
        &self,
        publish: StagedPublish,
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> bool {
        let key = publish.key();
        {
            let mut staged = lock_unpoisoned(&self.staged);
            if let Entry::Vacant(slot) = staged.entry(key) {
                slot.insert(publish);
                return true;
            }
        }
        publish
            .cleanup_reserved_media(operation, failure_message)
            .await;
        false
    }

    pub async fn rollback(
        &self,
        stream_id: &UserStreamId,
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> Option<TransportEffectOutcome> {
        let staged = self.take(operation.user_id, operation.connection_id, stream_id)?;
        Some(
            staged
                .cleanup_reserved_media(operation, failure_message)
                .await,
        )
    }

    pub async fn commit_answer(
        &self,
        operation: RoomUserOperation<'_>,
        applied_answer: &AppliedSessionAnswer,
    ) -> Vec<UserStreamId> {
        let mut committed = Vec::new();
        for staged in self.take_for_connection(operation.user_id, operation.connection_id) {
            if let Some(stream_id) = staged.commit_from_answer(operation, applied_answer).await {
                committed.push(stream_id);
            }
        }
        committed
    }

    pub fn cleanup_operations_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<TransportCleanupOperation> {
        self.take_for_connection(user_id, connection_id)
            .into_iter()
            .map(StagedPublish::into_cleanup_operation)
            .collect()
    }

    fn take(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<StagedPublish> {
        lock_unpoisoned(&self.staged).remove(&staged_publish_key(user_id, connection_id, stream_id))
    }

    fn take_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<StagedPublish> {
        lock_unpoisoned(&self.staged)
            .extract_if(.., |key, _publish| {
                &key.0 == user_id && key.1 == connection_id
            })
            .map(|(_key, publish)| publish)
            .collect()
    }

    #[cfg(test)]
    pub fn insert_for_test(&self, transaction: StagedPublish) {
        let key = transaction.key();
        let mut staged = lock_unpoisoned(&self.staged);
        assert!(
            !staged.contains_key(&key),
            "test duplicate staged publish slot should be empty before injection"
        );
        staged.insert(key, transaction);
    }

    #[cfg(test)]
    pub fn staged_count(&self, user_id: &UserId, connection_id: ConnectionId) -> usize {
        lock_unpoisoned(&self.staged)
            .keys()
            .filter(|(user, connection, _stream)| user == user_id && *connection == connection_id)
            .count()
    }

    #[cfg(test)]
    pub fn staged_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        let stream_id = stream_id_for_source(stream_type);
        lock_unpoisoned(&self.staged)
            .iter()
            .find(|((user, connection, stream), _publish)| {
                user == user_id && *connection == connection_id && stream == &stream_id
            })
            .map(|(_, publish)| publish.media)
    }
}

fn staged_publish_key(
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_id: &UserStreamId,
) -> StagedPublishKey {
    (user_id.clone(), connection_id, stream_id.clone())
}

impl StagedPublish {
    pub fn new(descriptor: ValidatedPublish, transport_media_id: TransportMediaId) -> Self {
        Self {
            descriptor,
            media: transport_media_id,
            armed: true,
        }
    }

    fn key(&self) -> StagedPublishKey {
        staged_publish_key(
            &self.descriptor.owner_user_id,
            self.descriptor.owner_connection_id,
            &self.descriptor.stream_id,
        )
    }

    pub(super) async fn commit_from_answer(
        self,
        operation: RoomUserOperation<'_>,
        applied_answer: &AppliedSessionAnswer,
    ) -> Option<UserStreamId> {
        let media = self.media;
        let Some(rtp) = applied_answer
            .negotiated_producer_parameters(media)
            .cloned()
        else {
            let user = self.descriptor.owner_user_id.clone();
            let connection = self.descriptor.owner_connection_id;
            let stream_id = self.descriptor.stream_id.clone();
            self.cleanup_reserved_media(
                operation,
                "media transport failed to remove staged publish media after answered negotiation omitted producer parameters",
            )
            .await;
            warn!(
                user_id = ?user,
                connection_id = ?connection,
                stream_id = %stream_id,
                transport_media_id = ?media,
                "answered negotiation did not include staged publish parameters during room commit"
            );
            return None;
        };
        let encodings = applied_answer
            .negotiated_producer_upload_encodings(media)
            .to_vec();
        self.commit_with_negotiated_parameters(operation, rtp, encodings)
            .await
    }

    pub(super) async fn commit_with_negotiated_parameters(
        self,
        operation: RoomUserOperation<'_>,
        rtp: RouterRtpParameters,
        upload_encodings: Vec<SessionUploadEncoding>,
    ) -> Option<UserStreamId> {
        let room = operation.room;
        let user = self.descriptor.owner_user_id.clone();
        let connection = self.descriptor.owner_connection_id;
        let stream_id = self.descriptor.stream_id.clone();
        let media = self.media;
        let committed = {
            let mut state = room.state.write().await;
            state.commit_publish_reservation(self.descriptor.clone(), rtp, &upload_encodings, media)
        };
        let Some(commit) = committed else {
            self.cleanup_reserved_media(
                operation,
                "media transport failed to remove published transport media after room commit failed",
            )
                .await;
            warn!(
                user_id = ?user,
                connection_id = ?connection,
                stream_id = %stream_id,
                transport_media_id = ?media,
                "room rejected staged negotiated publish during commit"
            );
            return None;
        };
        self.commit();
        effects::batch::build_publish_commit(room, commit)
            .execute(room, RoomEffectContext::runtime(operation.media_transport))
            .await;
        Some(stream_id)
    }

    pub(super) async fn cleanup_reserved_media(
        self,
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        let mut publish = self;
        let cleanup = [publish.release_cleanup_operation()];
        let outcome = operation
            .room
            .execute_transport_cleanup_operations(operation.media_transport, &cleanup)
            .await;
        if outcome == TransportEffectOutcome::Failed {
            warn!(
                user_id = ?publish.descriptor.owner_user_id,
                connection_id = ?publish.descriptor.owner_connection_id,
                transport_media_id = ?publish.media,
                "{failure_message}"
            );
        }
        outcome
    }

    pub(super) fn into_cleanup_operation(self) -> TransportCleanupOperation {
        let mut publish = self;
        publish.release_cleanup_operation()
    }

    fn release_cleanup_operation(&mut self) -> TransportCleanupOperation {
        debug_assert!(self.armed);
        self.armed = false;
        TransportCleanupOperation::RemoveMedia {
            session_key: self.descriptor.session_key.clone(),
            transport_media_id: self.media,
        }
    }

    fn commit(mut self) {
        debug_assert!(self.armed);
        self.armed = false;
    }
}

impl Drop for StagedPublish {
    fn drop(&mut self) {
        #[cfg(test)]
        assert!(
            !self.armed,
            "publish reservation dropped while still reserved"
        );
        #[cfg(all(debug_assertions, not(test)))]
        debug_assert!(
            !self.armed,
            "publish reservation dropped while still reserved"
        );
    }
}
