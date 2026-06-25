//! subscription transitions bind receiver intent to negotiated consumer routes
//!
//! receiver intent stays remembered even when no producer is routable
//! the readiness transition owns missing consumer setup plus active video
//! keyframe refresh once the accepted answer makes the receiver consumable

use std::collections::BTreeMap;

use o_sfu_router::rtp::MediaCapabilities;

use super::super::{
    RoomUserOperation,
    effects::batch::{RoomCommit, RoomEffectContext, RoomEffects},
    user_negotiation::UserNegotiationUpdate,
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
        let update = {
            let mut state = self.room.state.write().await;
            state.set_user_negotiated(self.user_id, self.connection_id, capabilities)
        };
        match update {
            Some(UserNegotiationUpdate::BecameConsumerReady) => {
                self.apply_receiver_readiness().await
            }
            Some(UserNegotiationUpdate::Applied) => Some(()),
            None => None,
        }
    }

    pub(crate) async fn apply_session_refreshed(self) -> Option<()> {
        self.apply_receiver_readiness().await
    }

    async fn apply_receiver_readiness(self) -> Option<()> {
        self.setup_missing_consumers().await?;
        self.request_active_video_keyframes().await;
        Some(())
    }

    pub(crate) async fn apply_receiver_intent(
        self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Option<()> {
        let room = self.room;
        let commit = {
            let mut state = room.state.write().await;
            let commit = state.apply_receiver_intent(
                self.user_id,
                self.connection_id,
                target_user_id,
                intents,
            );
            drop(state);
            commit
        };
        let commit = commit?;
        RoomEffects::from_commit(room, RoomCommit::ReceiverIntent(commit))
            .execute(room, RoomEffectContext::runtime(self.media_transport))
            .await;
        Some(())
    }

    async fn setup_missing_consumers(self) -> Option<()> {
        let room = self.room;
        let commit = {
            let mut state = room.state.write().await;
            let commit = state.refresh_consumer_readiness(self.user_id, self.connection_id);
            drop(state);
            commit
        };
        let commit = commit?;
        RoomEffects::from_commit(room, RoomCommit::ConsumerReadiness(commit))
            .execute(room, RoomEffectContext::runtime(self.media_transport))
            .await;
        Some(())
    }

    async fn request_active_video_keyframes(self) {
        let room = self.room;
        let Some(targets) = ({
            let state = room.state.read().await;
            let targets = state.active_video_keyframe_targets(self.user_id, self.connection_id);
            drop(state);
            targets
        }) else {
            return;
        };
        RoomEffects::keyframe_refresh(targets)
            .execute(room, RoomEffectContext::runtime(self.media_transport))
            .await;
    }
}

#[cfg(test)]
#[path = "TESTS/subscription.rs"]
mod tests;
