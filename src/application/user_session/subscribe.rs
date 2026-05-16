use o_sfu_protocol::shared::{DownloadStates, UserId};
use tracing::{info, instrument};

use super::{User, UserError, UserOutput, compat::subscription_intents_from_download_states};
use crate::{
    core::SubscriptionUpdateOutcome, runtime::telemetry::schema::event as telemetry_event,
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
        let source_intents = subscription_intents_from_download_states(states);
        let outcome = self
            .media()
            .update_subscription(target_user_id, &source_intents)
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
