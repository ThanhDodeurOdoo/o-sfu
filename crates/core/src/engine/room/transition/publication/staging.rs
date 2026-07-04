use std::{
    collections::{BTreeMap, btree_map::Entry},
    marker::PhantomData,
    slice,
};

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::TransportEffectOutcome;
use crate::engine::{
    ConnectionId, UserId,
    media_transport::{AppliedSessionAnswer, SessionUploadEncoding, TransportMediaId},
    room::{
        RoomUserOperation,
        cleanup::TransportCleanupOperation,
        effects::batch::{RoomCommit, RoomEffectContext, RoomEffects},
        media_graph::ValidatedPublish,
    },
    source_model::UserStreamId,
};
#[cfg(test)]
use crate::engine::{TestSourceKind, source_model::test_support::stream_id_for_source};

#[derive(Debug, Default)]
pub struct StagedPublishes {
    staged: BTreeMap<StagedPublishKey, StagedPublish>,
}

type StagedPublishKey = (UserId, ConnectionId, UserStreamId);

#[derive(Debug)]
pub struct StagedPublish {
    pub(super) descriptor: ValidatedPublish,
    pub(super) media: TransportMediaId,
    reservation: PublishReservation<Reserved>,
}

impl StagedPublishes {
    pub fn contains(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.staged
            .contains_key(&staged_publish_key(user_id, connection_id, stream_id))
    }

    pub fn stage(&mut self, publish: StagedPublish) -> Option<StagedPublish> {
        let key = publish.key();
        if let Entry::Vacant(slot) = self.staged.entry(key) {
            slot.insert(publish);
            return None;
        }
        Some(publish)
    }

    pub fn cleanup_operations_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<TransportCleanupOperation> {
        self.take_for_connection(user_id, connection_id)
            .into_iter()
            .map(StagedPublish::into_cleanup_operation)
            .collect()
    }

    pub(in crate::engine::room) fn take(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<StagedPublish> {
        self.staged
            .remove(&staged_publish_key(user_id, connection_id, stream_id))
    }

    pub(in crate::engine::room) fn take_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<StagedPublish> {
        self.staged
            .extract_if(.., |key, _publish| {
                &key.0 == user_id && key.1 == connection_id
            })
            .map(|(_key, publish)| publish)
            .collect()
    }

    #[cfg(test)]
    pub fn staged_count(&self, user_id: &UserId, connection_id: ConnectionId) -> usize {
        self.staged
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
        self.staged
            .get(&staged_publish_key(user_id, connection_id, &stream_id))
            .map(|publish| publish.media)
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
            reservation: PublishReservation::new(),
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
        let encodings = applied_answer.negotiated_producer_upload_encodings(media);
        self.commit_with_negotiated_parameters(operation, rtp, encodings)
            .await
    }

    pub(super) async fn commit_with_negotiated_parameters(
        self,
        operation: RoomUserOperation<'_>,
        rtp: RouterRtpParameters,
        upload_encodings: &[SessionUploadEncoding],
    ) -> Option<UserStreamId> {
        let room = operation.room;
        let user = self.descriptor.owner_user_id.clone();
        let connection = self.descriptor.owner_connection_id;
        let stream_id = self.descriptor.stream_id.clone();
        let media = self.media;
        let committed = {
            let mut state = room.state.write().await;
            state.commit_publish_reservation(self.descriptor.clone(), rtp, upload_encodings, media)
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
        self.commit_reservation();
        RoomEffects::from_commit(room, RoomCommit::Publish(commit))
            .execute(room, RoomEffectContext::runtime(operation.media_transport))
            .await;
        Some(stream_id)
    }

    pub(super) async fn cleanup_reserved_media(
        self,
        operation: RoomUserOperation<'_>,
        failure_message: &str,
    ) -> TransportEffectOutcome {
        let media = self.media;
        let cleanup = self.into_cleanup_operation();
        let outcome = operation
            .room
            .execute_transport_cleanup_operations(
                operation.media_transport,
                slice::from_ref(&cleanup),
            )
            .await;
        if outcome == TransportEffectOutcome::Failed {
            warn!(
                user_id = ?cleanup.user_id(),
                connection_id = ?cleanup.connection_id(),
                transport_media_id = ?media,
                "{failure_message}"
            );
        }
        outcome
    }

    pub(super) fn into_cleanup_operation(self) -> TransportCleanupOperation {
        let Self {
            descriptor,
            media,
            reservation,
        } = self;
        let _released = reservation.release();
        TransportCleanupOperation::RemoveMedia {
            session_key: descriptor.session_key,
            transport_media_id: media,
        }
    }

    fn commit_reservation(self) {
        let _committed = self.reservation.commit();
    }
}

#[derive(Debug)]
struct Reserved;

#[derive(Debug)]
struct Committed;

#[derive(Debug)]
struct Released;

#[derive(Debug)]
#[must_use = "publish reservations must be committed or released"]
struct PublishReservation<State> {
    guard: PublishReservationGuard,
    _state: PhantomData<fn() -> State>,
}

#[derive(Debug)]
struct PublishReservationGuard {
    armed: bool,
}

impl PublishReservation<Reserved> {
    fn new() -> Self {
        Self {
            guard: PublishReservationGuard { armed: true },
            _state: PhantomData,
        }
    }

    fn commit(mut self) -> PublishReservation<Committed> {
        self.guard.disarm();
        PublishReservation {
            guard: self.guard,
            _state: PhantomData,
        }
    }

    fn release(mut self) -> PublishReservation<Released> {
        self.guard.disarm();
        PublishReservation {
            guard: self.guard,
            _state: PhantomData,
        }
    }
}

impl PublishReservationGuard {
    fn disarm(&mut self) {
        debug_assert!(self.armed);
        self.armed = false;
    }
}

impl Drop for PublishReservationGuard {
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
