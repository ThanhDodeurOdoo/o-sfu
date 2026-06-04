use std::collections::BTreeMap;

use o_sfu_protocol::wire::{DownloadStates, UserId};
use tracing::{info, instrument};

use super::{User, UserError, UserOutput};
use crate::{
    application::stream_catalog::DiscussStream, core::prelude::SubscriptionUpdateOutcome,
    runtime::telemetry::schema::event as telemetry_event,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeIntent {
    target_user_id: UserId,
    states: DownloadStates,
}

impl SubscribeIntent {
    pub fn new(target_user_id: UserId, states: DownloadStates) -> Self {
        Self {
            target_user_id: target_user_id.normalized_for_runtime(),
            states,
        }
    }
}

impl User {
    /// Persist this user's download intent for another room user.
    ///
    /// The compatibility [`DownloadStates`] payload is projected into core
    /// source subscription intent before core sees it. The target user id is
    /// normalized by [`SubscribeIntent`] before room state sees it.
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
            target_session_id = ?intent.target_user_id
        )
    )]
    pub async fn subscribe(&self, intent: SubscribeIntent) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        info!(
            event = telemetry_event::SUBSCRIBE_PREPARED,
            operation = "consume_prepare",
            outcome = "request_received",
            "received subscribe intent"
        );
        let SubscribeIntent {
            target_user_id,
            states,
        } = intent;
        let source_intents = DiscussStream::all()
            .filter_map(|stream| stream.subscription_intent_if_requested(&states))
            .collect::<BTreeMap<_, _>>();
        let outcome = self
            .media()
            .subscription()
            .update(&target_user_id, &source_intents)
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
