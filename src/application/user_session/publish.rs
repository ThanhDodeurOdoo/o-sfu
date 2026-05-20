use o_sfu_protocol::shared::StreamType;
use tracing::{info, instrument, warn};

use super::{User, UserError, UserOutput, compat::publication_info_update};
use crate::{
    application::stream_catalog::{
        source_publish_intent_for_stream_type, stream_id_for_stream_type,
    },
    core::prelude::{
        PublicationActivity, PublicationActivityOutcome, RollbackStagedPublishOutcome,
        TransportEffectOutcome, UnpublishOutcome, UserInfoRefresh,
    },
    runtime::telemetry::schema::event as telemetry_event,
};

/// Media-side result of an unpublish request after queued work is removed.
///
/// The media facade may have already consumed staged ownership or attempted
/// transport cleanup before this value is returned. The caller uses this shape
/// to keep user-info fanout and renegotiation decisions explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnpublishMediaDisposition {
    /// A not-yet-committed publish was cancelled.
    RolledBackStagedPublish { cleanup: TransportEffectOutcome },
    /// A live room publication was removed and peers need a follow-up offer.
    RemovedLivePublication { cleanup: TransportEffectOutcome },
}

impl User {
    pub(super) async fn update_publication_info(
        &self,
        stream_type: StreamType,
        active: bool,
    ) -> Result<(), UserError> {
        let Some(info) = publication_info_update(stream_type, active) else {
            return Ok(());
        };
        self.media()
            .update_user_info(info, UserInfoRefresh::NotNeeded)
            .await;
        Ok(())
    }

    /// Accept a client intent to publish one compatibility stream.
    ///
    /// This method translates the Odoo stream type through
    /// `stream_catalog`, stages media through core when needed and emits a
    /// renegotiation request only when a new offer is required. Duplicate
    /// publish requests are accepted as idempotent no-ops.
    ///
    /// If the stream is already live, the method only marks its user-visible
    /// activity as active and updates presence state for camera or screen.
    /// Publish requests received while another negotiation is pending are
    /// queued for a follow-up offer after the current answer is applied.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] for stale connections and
    /// [`UserError::InternalError`] when core cannot stage media for a publish
    /// that requires negotiation.
    #[instrument(
        name = "publish.intent",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            stream_type = ?stream_type
        )
    )]
    pub async fn publish(&mut self, stream_type: StreamType) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        let stream_id = stream_id_for_stream_type(stream_type);
        let has_queued_publish = self.state.negotiation_state.has_queued_publish(stream_type);
        {
            let media = self.media();
            if has_queued_publish || media.has_staged_publish(&stream_id) {
                return Ok(UserOutput::new());
            }
            if media.is_stream_published(&stream_id).await {
                let outcome = media
                    .set_publication_activity(&stream_id, PublicationActivity::Active)
                    .await;
                if matches!(outcome, PublicationActivityOutcome::Applied { .. }) {
                    self.update_publication_info(stream_type, true).await?;
                }
                return Ok(UserOutput::new());
            }
        }
        if self.state.negotiation_state.awaiting_answer() {
            self.state.negotiation_state.queue_publish_slot(stream_type);
            let _disposition = self.state.negotiation_state.schedule_renegotiation();
            return Ok(UserOutput::new());
        }
        if !self.stage_publish_slot(stream_type).await? {
            return Ok(UserOutput::new());
        }
        self.renegotiate().await
    }

    /// Accept a client intent to stop publishing one compatibility stream.
    ///
    /// The request first cancels queued or staged publish work for this
    /// connection. If the stream is already live, core removes the room
    /// publication and this session requests renegotiation so the browser can
    /// drop the media section.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] for stale connections and
    /// [`UserError::InternalError`] when core cannot remove a live publication
    /// cleanly.
    pub async fn unpublish(&mut self, stream_type: StreamType) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        if self
            .state
            .negotiation_state
            .clear_queued_publish(stream_type)
        {
            return Ok(UserOutput::new());
        }
        let media_disposition = {
            let media = self.media();
            let stream_id = stream_id_for_stream_type(stream_type);
            match media.rollback_staged_publish(&stream_id).await {
                RollbackStagedPublishOutcome::RolledBack { cleanup } => {
                    Some(UnpublishMediaDisposition::RolledBackStagedPublish { cleanup })
                }
                RollbackStagedPublishOutcome::NotStaged => {
                    match media.unpublish(&stream_id).await {
                        UnpublishOutcome::Unpublished { cleanup } => {
                            Some(UnpublishMediaDisposition::RemovedLivePublication { cleanup })
                        }
                        UnpublishOutcome::MissingPublication => None,
                    }
                }
            }
        };
        match media_disposition {
            Some(UnpublishMediaDisposition::RolledBackStagedPublish { cleanup }) => {
                Self::log_staged_publish_rollback(stream_type, cleanup);
                let _disposition = self.state.negotiation_state.schedule_renegotiation();
                Ok(UserOutput::new())
            }
            Some(UnpublishMediaDisposition::RemovedLivePublication { cleanup }) => {
                Self::log_live_unpublish(stream_type, cleanup);
                self.update_publication_info(stream_type, false).await?;
                self.renegotiate().await
            }
            None => Ok(UserOutput::new()),
        }
    }

    pub(super) async fn stage_queued_publish_slots(&mut self) -> Result<bool, UserError> {
        let queued_publish_slots = self.state.negotiation_state.take_queued_publish_slots();
        let mut staged_any = false;
        for slot in queued_publish_slots {
            if self.stage_publish_slot(slot).await? {
                staged_any = true;
            }
        }
        Ok(staged_any)
    }

    async fn stage_publish_slot(&self, stream_type: StreamType) -> Result<bool, UserError> {
        let intent = source_publish_intent_for_stream_type(stream_type);
        let media_kind = intent.media_kind();
        let outcome = self.media().stage_publish(&intent).await.map_err(|error| {
            warn!(
                event = telemetry_event::PUBLISH_ABORTED,
                operation = "publish_prepare",
                outcome = "transport_error",
                media_kind = ?media_kind,
                stream_type = ?stream_type,
                ?error,
                "failed to stage publish stream for negotiation"
            );
            UserError::InternalError
        })?;
        let event = if outcome.staged() {
            telemetry_event::PUBLISH_PREPARED
        } else {
            telemetry_event::PUBLISH_ABORTED
        };
        info!(
            event,
            operation = "publish_prepare",
            outcome = ?outcome,
            media_kind = ?media_kind,
            stream_type = ?stream_type,
            "processed publish staging intent"
        );
        Ok(outcome.staged())
    }

    fn log_staged_publish_rollback(stream_type: StreamType, cleanup: TransportEffectOutcome) {
        info!(
            event = telemetry_event::PUBLISH_ABORTED,
            operation = "publish_rollback",
            outcome = ?cleanup,
            stream_type = ?stream_type,
            "rolled back staged publish stream before commit"
        );
    }

    fn log_live_unpublish(stream_type: StreamType, cleanup: TransportEffectOutcome) {
        info!(
            operation = "publish_unpublish",
            outcome = ?cleanup,
            stream_type = ?stream_type,
            "removed live publish stream"
        );
    }
}
