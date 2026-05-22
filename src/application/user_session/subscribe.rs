use std::collections::BTreeMap;

use o_sfu_protocol::wire::{DownloadStates, StreamType, UserId};
use tracing::{info, instrument};

use super::{User, UserError, UserOutput};
use crate::{
    application::stream_catalog::stream_id_for_stream_type,
    core::prelude::{SourceSubscriptionIntent, SubscriptionUpdateOutcome},
    runtime::telemetry::schema::event as telemetry_event,
};

impl User {
    /// Persist this user's download intent for another room user.
    ///
    /// The compatibility [`DownloadStates`] payload is projected into generic
    /// source subscription intent before core sees it. The target user id must
    /// already be normalized by the websocket edge.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::Kicked`] if this connection is stale. Returns
    /// [`UserError::ProtocolViolation`] if room state rejects the subscription
    /// update as stale during commit.
    #[instrument(
        name = "subscribe.intent",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            target_session_id = ?target_user_id
        )
    )]
    pub async fn subscribe_to(
        &self,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        info!(
            event = telemetry_event::SUBSCRIBE_PREPARED,
            operation = "consume_prepare",
            outcome = "request_received",
            "received subscribe intent"
        );
        let source_intents = [
            (StreamType::Audio, states.audio, None),
            (StreamType::Camera, states.camera, states.camera_layout),
            (StreamType::Screen, states.screen, states.screen_layout),
        ]
        .into_iter()
        .filter(|(_, media, layout)| media.is_some() || layout.is_some())
        .map(|(stream_type, media, layout)| {
            (
                stream_id_for_stream_type(stream_type),
                SourceSubscriptionIntent::new(media, layout),
            )
        })
        .collect::<BTreeMap<_, _>>();
        let outcome = self
            .media()
            .subscription()
            .update(target_user_id, &source_intents)
            .await;
        if outcome == SubscriptionUpdateOutcome::StaleConnection {
            return Err(UserError::ProtocolViolation);
        }
        info!(
            event = telemetry_event::SUBSCRIBE_SUCCEEDED,
            operation = "consume_prepare",
            outcome = ?outcome,
            "applied subscribe intent"
        );
        Ok(UserOutput::new())
    }
}
