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
//!                         rollback teardown   effects after lock
//! ```
//!
//! only answer-proven RTP enters the room graph
//! teardown, worker route updates and fanout run after state mutation releases
//! the room lock

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use tracing::warn;

use super::super::{
    Room, RoomUserOperation,
    effects::batch::{RoomCommit, RoomEffectContext, RoomEffects},
    media_graph::{ProducerActivityCommit, PublishIntentPlan, ValidatedPublish},
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::{ConnectionId, UserId};
use crate::engine::{
    media_transport::{AppliedSessionAnswer, TransportAdapterError},
    source_model::{SourcePublishIntent, SourceUnpublishIntent, UserStreamId},
};

mod staging;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/publication_support.rs"]
mod test_support;

pub use staging::{StagedPublish, StagedPublishes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishIntentOutcome {
    Noop,
    Queue,
    Activated,
    Staged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnpublishIntentOutcome {
    Noop,
    RolledBack,
    Unpublished,
}

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
        let Some(validated_descriptor) = ({
            let state = self.room.state.read().await;
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
        let is_duplicate = {
            let state = self.room.state.read().await;
            state.staged_publishes.contains(
                self.user_id,
                self.connection_id,
                &validated_descriptor.stream_id,
            )
        };
        if is_duplicate {
            return Ok(PublishStageOutcome::Duplicate);
        }
        let rtp_parameters = RouterRtpParameters::default();
        let media = match self
            .media_transport
            .publish_media(
                &validated_descriptor.session_key,
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
        let reserved_publish = StagedPublish::new(validated_descriptor, media);
        let duplicate = {
            let mut state = self.room.state.write().await;
            if state
                .validate_publish_commit(&reserved_publish.descriptor, reserved_publish.media)
                .is_some()
            {
                state.staged_publishes.stage(reserved_publish)
            } else {
                Some(reserved_publish)
            }
        };
        if let Some(duplicate) = duplicate {
            duplicate.release_reserved_media(self).await;
            return Ok(PublishStageOutcome::DuplicateAfterReservation);
        }
        Ok(PublishStageOutcome::Staged)
    }

    pub(crate) async fn start_publish(
        self,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> Result<PublishIntentOutcome, TransportAdapterError> {
        let has_staged_publish = {
            let state = self.room.state.read().await;
            state
                .staged_publishes
                .contains(self.user_id, self.connection_id, intent.stream_id())
        };
        if has_staged_publish {
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

    pub async fn rollback_staged_publish(self, stream_id: &UserStreamId) -> bool {
        let Some(staged) = ({
            let mut state = self.room.state.write().await;
            state
                .staged_publishes
                .take(self.user_id, self.connection_id, stream_id)
        }) else {
            return false;
        };
        staged.release_reserved_media(self).await;
        true
    }

    pub(crate) async fn stop_publish(
        self,
        intent: &SourceUnpublishIntent,
    ) -> UnpublishIntentOutcome {
        if self.rollback_staged_publish(intent.stream_id()).await {
            return UnpublishIntentOutcome::RolledBack;
        }
        if self.unpublish(intent).await {
            UnpublishIntentOutcome::Unpublished
        } else {
            UnpublishIntentOutcome::Noop
        }
    }

    pub(crate) async fn commit_staged_publishes(self, applied_answer: &AppliedSessionAnswer) {
        let staged = {
            let mut state = self.room.state.write().await;
            state
                .staged_publishes
                .take_for_connection(self.user_id, self.connection_id)
        };
        for publish in staged {
            publish.commit_from_answer(self, applied_answer).await;
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(crate) async fn set_publication_active(
        self,
        stream_id: &UserStreamId,
        active: bool,
    ) -> Option<()> {
        let commit = {
            let mut state = self.room.state.write().await;
            state.apply_publication_activity(
                self.user_id,
                self.connection_id,
                stream_id,
                active,
                None,
            )
        };
        let commit = commit.ok()?;
        self.execute_publication_activity(commit).await;
        Some(())
    }

    async fn execute_publication_activity(self, commit: ProducerActivityCommit) {
        RoomEffects::from_commit(self.room, RoomCommit::PublicationActivity(commit))
            .execute(self.room, RoomEffectContext::runtime(self.media_transport))
            .await;
    }

    async fn unpublish(self, intent: &SourceUnpublishIntent) -> bool {
        let commit = {
            let mut state = self.room.state.write().await;
            state.unpublish_track(self.user_id, self.connection_id, intent)
        };
        let Some(commit) = commit else {
            return false;
        };
        RoomEffects::from_commit(self.room, RoomCommit::Unpublish(commit))
            .execute(self.room, RoomEffectContext::runtime(self.media_transport))
            .await;
        self.room.reconcile_spillover_routers().await;
        true
    }
}

impl Room {
    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.state
            .read()
            .await
            .staged_publishes
            .contains(user_id, connection_id, stream_id)
    }
}

#[cfg(test)]
#[path = "TESTS/publication.rs"]
mod tests;
