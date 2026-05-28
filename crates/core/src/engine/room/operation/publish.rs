use o_sfu_router::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::{super::media_transaction::PendingPublishTransaction, RoomUserOperation};
use crate::{
    PublishStageOutcome, RollbackStagedPublishOutcome,
    engine::{
        media_transport::{AppliedSessionAnswer, TransportAdapterError},
        source_model::{SourcePublishIntent, UserStreamId},
    },
};

impl RoomUserOperation<'_> {
    #[must_use]
    pub(crate) fn has_staged_publish(self, stream_id: &UserStreamId) -> bool {
        self.room().pending_publish_transactions().contains(
            self.user_id(),
            self.connection_id(),
            stream_id,
        )
    }

    /// Validates room ownership and reserves transport media for a negotiated publish.
    ///
    /// `PublishStageOutcome::Staged` means the publish is staged, not live.
    /// The caller must still drive renegotiation and later call
    /// `commit_staged_publishes` after the answer lands. The method avoids
    /// holding room state or pending-registry locks across the transport call.
    ///
    /// If another task stages the same stream during the transport await, this
    /// method consumes the duplicate reservation through cleanup and reports
    /// `PublishStageOutcome::DuplicateAfterReservation`.
    pub(crate) async fn stage_negotiated_publish(
        self,
        intent: &SourcePublishIntent,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let room = self.room();
        let Some(validated_descriptor) = ({
            let state = room.state.read().await;
            state.validate_publish_descriptor(self.user_id(), self.connection_id(), intent)
        }) else {
            return Ok(PublishStageOutcome::Rejected);
        };
        if room.pending_publish_transactions().contains(
            self.user_id(),
            self.connection_id(),
            intent.stream_id(),
        ) {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let session_key = self.transport_user_key();
        let rtp_parameters = answer_derived_publish_parameters();
        let transport_media_id = match self
            .media_transport()
            .publish_media(&session_key, intent.media_kind(), &rtp_parameters)
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(error) => {
                warn!(
                    user_id = ?self.user_id(),
                    connection_id = ?self.connection_id(),
                    stream_id = %intent.stream_id(),
                    media_kind = ?intent.media_kind(),
                    "failed to stage negotiated publish stream"
                );
                return Err(error);
            }
        };
        #[cfg(test)]
        room.inject_duplicate_staged_publish_after_reservation_for_test(
            &validated_descriptor,
            transport_media_id,
        );
        let transaction = PendingPublishTransaction::new(validated_descriptor, transport_media_id);
        if let Some(staged_publish) = {
            let mut pending_publish_transactions = room.pending_publish_transactions();
            pending_publish_transactions.stage(transaction).err()
        } {
            let cleanup = staged_publish
                .cleanup_reserved_media(
                    self,
                    "media transport failed to remove duplicated staged publish media",
                )
                .await;
            return Ok(PublishStageOutcome::DuplicateAfterReservation { cleanup });
        }
        Ok(PublishStageOutcome::Staged)
    }

    /// Cancels one staged publish before it becomes a live producer.
    ///
    /// This is the explicit unpublish-before-answer path. A successful rollback
    /// consumes the reservation even when transport cleanup fails, because the
    /// publish must not remain commit-capable after the user requested removal.
    pub(crate) async fn rollback_staged_publish(
        self,
        stream_id: &UserStreamId,
    ) -> RollbackStagedPublishOutcome {
        let Some(staged_publish) = self.room().pending_publish_transactions().take(
            self.user_id(),
            self.connection_id(),
            stream_id,
        ) else {
            return RollbackStagedPublishOutcome::NotStaged;
        };
        let cleanup = staged_publish
            .cleanup_reserved_media(
                self,
                "media transport failed to remove staged publish media during rollback",
            )
            .await;
        RollbackStagedPublishOutcome::RolledBack { cleanup }
    }

    /// Cleans up every staged publish owned by a websocket connection.
    ///
    /// User replacement, logical disconnect and websocket drop use this to
    /// drain all in-flight reservations before the connection can disappear.
    /// Cleanup remains best-effort because transport teardown may already be in
    /// progress
    pub(crate) async fn rollback_staged_publishes_for_connection(self) {
        let staged_publishes = self
            .room()
            .pending_publish_transactions()
            .take_for_connection(self.user_id(), self.connection_id());
        for staged_publish in staged_publishes {
            staged_publish
                .cleanup_reserved_media(
                    self,
                    "media transport failed to remove staged publish media during connection cleanup",
                )
                .await;
        }
    }

    /// Commits every staged publish for a connection after negotiation
    /// answered successfully
    ///
    /// The registry is drained before commit work starts so a later websocket
    /// message cannot commit the same reservation twice. Each transaction then
    /// re-checks current room state before creating a live producer. If that
    /// state is stale, the transaction consumes its transport reservation
    /// through cleanup instead.
    pub(crate) async fn commit_staged_publishes(
        self,
        applied_answer: &AppliedSessionAnswer,
    ) -> Vec<UserStreamId> {
        let staged_publishes = self
            .room()
            .pending_publish_transactions()
            .take_for_connection(self.user_id(), self.connection_id());
        let mut committed_stream_ids = Vec::new();
        for staged_publish in staged_publishes {
            if let Some(stream_id) = staged_publish.commit(self, applied_answer).await {
                committed_stream_ids.push(stream_id);
            }
        }
        committed_stream_ids
    }
}

/// Marker parameters for a protocol publish whose concrete SSRC and RID
/// bindings are projected from the accepted SDP answer.
fn answer_derived_publish_parameters() -> RouterRtpParameters {
    RouterRtpParameters::default()
}
