//! Receiver intent and consumer-route realization.
//!
//! Intent survives a missing publication or unready receiver. Readiness reserves
//! routes under the room lock, declares transport without it and commits only if
//! the reservation still matches the publication and receiver connection.

use std::collections::BTreeMap;

use o_sfu_router::rtp::MediaCapabilities;

use super::super::{
    RoomUserOperation,
    effects::batch::{RoomEffectContext, RoomEffects},
};
use crate::engine::{
    UserId,
    source_model::{SourceSubscriptionIntent, UserStreamId},
};

impl RoomUserOperation<'_> {
    pub(crate) async fn apply_session_negotiated(
        self,
        capabilities: MediaCapabilities,
    ) -> Option<()> {
        let became_ready = {
            let mut state = self.room.state.write().await;
            state.set_user_negotiated(self.user_id, self.connection_id, capabilities)
        }?;
        if became_ready {
            self.apply_receiver_readiness().await
        } else {
            Some(())
        }
    }

    pub(crate) async fn apply_session_refreshed(self) -> Option<()> {
        self.apply_receiver_readiness().await
    }

    async fn apply_receiver_readiness(self) -> Option<()> {
        let commit = {
            let mut state = self.room.state.write().await;
            state.refresh_consumer_readiness(self.user_id, self.connection_id)
        };
        let commit = commit?;
        RoomEffects::from_consumer_readiness(commit)
            .execute(self.room, RoomEffectContext::runtime(self.media_transport))
            .await;
        Some(())
    }

    pub(crate) async fn apply_receiver_intent(
        self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Option<()> {
        let commit = {
            let mut state = self.room.state.write().await;
            state.apply_receiver_intent(self.user_id, self.connection_id, target_user_id, intents)
        };
        let commit = commit?;
        RoomEffects::from_receiver_intent(commit)
            .execute(self.room, RoomEffectContext::runtime(self.media_transport))
            .await;
        Some(())
    }
}

#[cfg(test)]
#[path = "TESTS/subscription.rs"]
mod tests;
