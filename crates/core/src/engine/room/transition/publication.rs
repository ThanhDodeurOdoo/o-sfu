//! publication transitions keep unnegotiated media out of the room graph
//!
//! ```text
//! publish intent
//!   |
//!   +-- existing producer --> activity commit --> effects after lock
//!   |
//!   +-- offer in flight ----> queued intent ---> answer ---> stage next offer
//!   |
//!   +-- new producer -------> StagedPublish ---> answer-proven RTP
//!                              |                  |
//!                              |                  v
//!                              |                room graph commit
//!                              |                  |
//!                              v                  v
//!                         rollback cleanup    effects after lock
//! ```
//!
//! only answer-proven RTP enters the room graph
//! cleanup, worker route updates and fanout run after state mutation releases
//! the room lock

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::super::{
    Room, RoomUserOperation,
    cleanup::TransportCleanupOperation,
    effects::{self, batch::RoomEffectContext},
    media_graph::{ProducerActivityCommit, PublishIntentPlan, ValidatedPublish},
};
use crate::{
    PublishIntentOutcome, TransportEffectOutcome, UnpublishIntentOutcome,
    engine::{
        ConnectionId, UserId,
        media_transport::{AppliedSessionAnswer, TransportAdapterError},
        source_model::{SourcePublishIntent, UserStreamId},
    },
};

mod staging;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/publication_support.rs"]
mod test_support;

pub use staging::{StagedPublish, StagedPublishes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStageOutcome {
    Staged,
    Duplicate,
    DuplicateAfterReservation,
    #[cfg(test)]
    Rejected,
}

impl RoomUserOperation<'_> {
    #[cfg(test)]
    pub async fn stage_negotiated_publish(
        self,
        intent: &SourcePublishIntent,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let room = self.room;
        let Some(validated_descriptor) = ({
            let state = room.state.read().await;
            state.validate_publish(self.user_id, self.connection_id, intent)
        }) else {
            return Ok(PublishStageOutcome::Rejected);
        };
        self.stage_validated_publish(validated_descriptor).await
    }

    async fn stage_validated_publish(
        self,
        validated_descriptor: ValidatedPublish,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let room = self.room;
        if room.staged_publishes.contains(
            self.user_id,
            self.connection_id,
            &validated_descriptor.stream_id,
        ) {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let session_key = validated_descriptor.session_key.clone();
        let rtp_parameters = RouterRtpParameters::default();
        let media = match self
            .media_transport
            .publish_media(
                &session_key,
                validated_descriptor.media_kind,
                &rtp_parameters,
            )
            .await
        {
            Ok(media) => media,
            Err(error) => {
                warn!(
                    user_id = ?self.user_id,
                    connection_id = ?self.connection_id,
                    stream_id = %validated_descriptor.stream_id,
                    media_kind = ?validated_descriptor.media_kind,
                    "failed to stage negotiated publish stream"
                );
                return Err(error);
            }
        };
        #[cfg(test)]
        room.inject_next_duplicate_for_test(&validated_descriptor, media);
        let reserved_publish = StagedPublish::new(validated_descriptor, media);
        if !room
            .staged_publishes
            .stage(
                reserved_publish,
                self,
                "media transport failed to remove duplicated staged publish media",
            )
            .await
        {
            return Ok(PublishStageOutcome::DuplicateAfterReservation);
        }
        Ok(PublishStageOutcome::Staged)
    }

    pub(crate) async fn start_publish(
        self,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> Result<PublishIntentOutcome, TransportAdapterError> {
        let stream_id = intent.stream_id();
        if self
            .room
            .staged_publishes
            .contains(self.user_id, self.connection_id, stream_id)
        {
            return Ok(PublishIntentOutcome::Noop);
        }
        let plan = {
            let mut state = self.room.state.write().await;
            state.apply_publish_intent(self.user_id, self.connection_id, intent, can_stage)
        };
        match plan {
            PublishIntentPlan::Activate(commit) => {
                self.execute_publication_activity(commit).await;
                Ok(PublishIntentOutcome::Activated)
            }
            PublishIntentPlan::Noop => Ok(PublishIntentOutcome::Noop),
            PublishIntentPlan::Queue => Ok(PublishIntentOutcome::Queue),
            PublishIntentPlan::Stage(validated) => {
                if self.stage_validated_publish(validated).await? == PublishStageOutcome::Staged {
                    Ok(PublishIntentOutcome::Staged)
                } else {
                    Ok(PublishIntentOutcome::Noop)
                }
            }
        }
    }

    pub async fn rollback_staged_publish(
        self,
        stream_id: &UserStreamId,
    ) -> Option<TransportEffectOutcome> {
        self.room
            .staged_publishes
            .rollback(
                stream_id,
                self,
                "media transport failed to remove staged publish media during rollback",
            )
            .await
    }

    pub(crate) async fn stop_publish(self, stream_id: &UserStreamId) -> UnpublishIntentOutcome {
        if self.rollback_staged_publish(stream_id).await.is_some() {
            return UnpublishIntentOutcome::RolledBack;
        }
        if self.unpublish(stream_id).await {
            UnpublishIntentOutcome::Unpublished
        } else {
            UnpublishIntentOutcome::Noop
        }
    }

    pub(crate) async fn commit_staged_publishes(
        self,
        applied_answer: &AppliedSessionAnswer,
    ) -> Vec<UserStreamId> {
        self.room
            .staged_publishes
            .commit_answer(self, applied_answer)
            .await
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(crate) async fn set_publication_active(
        self,
        stream_id: &UserStreamId,
        active: bool,
    ) -> Option<()> {
        let room = self.room;
        let commit = {
            let mut state = room.state.write().await;
            state.apply_publication_activity(self.user_id, self.connection_id, stream_id, active)
        };
        let commit = commit.ok()?;
        self.execute_publication_activity(commit).await;
        Some(())
    }

    async fn execute_publication_activity(self, commit: ProducerActivityCommit) {
        effects::batch::build_publication_activity(
            self.room,
            self.user_id,
            self.connection_id,
            commit,
        )
        .execute(self.room, RoomEffectContext::runtime(self.media_transport))
        .await;
    }

    async fn unpublish(self, stream_id: &UserStreamId) -> bool {
        let room = self.room;
        let user_id = self.user_id;
        let connection_id = self.connection_id;
        let media_port = self.media_transport;
        let commit = {
            let mut state = room.state.write().await;
            let commit = state.unpublish_track(user_id, connection_id, stream_id);
            drop(state);
            commit
        };
        let Some(commit) = commit else {
            return false;
        };
        effects::batch::build_unpublish(commit)
            .execute(room, RoomEffectContext::runtime(media_port))
            .await;
        room.reconcile_spillover_routers().await;
        true
    }
}

impl Room {
    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.staged_publishes
            .contains(user_id, connection_id, stream_id)
    }

    pub fn drain_staged_publish_cleanup_operations(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Vec<TransportCleanupOperation> {
        self.staged_publishes
            .cleanup_operations_for_connection(user_id, connection_id)
    }
}

#[cfg(test)]
#[path = "TESTS/publication.rs"]
mod tests;
