//! subscription transitions bind receiver intent to negotiated consumer routes
//!
//! ```text
//! subscribe intent
//!   |
//!   v
//! remembered receiver state
//!   |
//!   +-- existing route -------> activity update ----> effects after lock
//!   |
//!   +-- route missing
//!         |
//!         +-- producer missing -> wait for publish commit
//!         |
//!         +-- consumer pending -> wait for session answer
//!         |
//!         +-- consumer ready --> setup reservation
//!                               |
//!                               +-- same worker  --> effects after lock
//!                               |
//!                               +-- cross-worker -> relay setup -> effects after lock
//!
//! initial answer ----> mark consumer-ready ---> setup missing consumers
//! refresh answer ----> keyframes required ----> setup missing consumers
//! ```
//!
//! intent stays remembered even when no producer is routable
//! keyframes are requested only after live video consumer routes are known

use std::collections::BTreeMap;

use o_sfu_router::MediaCapabilities;

use super::super::{
    RoomUserOperation,
    effects::{self, batch::RoomEffectContext},
    media_graph::ConsumerRouteTarget,
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
                self.refresh_after_initial_answer().await
            }
            Some(UserNegotiationUpdate::Applied) => Some(()),
            None => None,
        }
    }

    pub(crate) async fn apply_session_refreshed(self) -> Option<()> {
        self.request_video_keyframes_required().await?;
        self.setup_missing_consumers().await
    }

    async fn refresh_after_initial_answer(self) -> Option<()> {
        self.setup_missing_consumers().await?;
        self.request_video_keyframes_if_present().await
    }

    pub(crate) async fn apply_receiver_intent(
        self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Option<()> {
        let room = self.room;
        let commit = {
            let mut state = room.state.write().await;
            let worker_lookup = state.worker_lookup();
            let commit = state.apply_receiver_intent(
                self.user_id,
                self.connection_id,
                target_user_id,
                intents,
                worker_lookup,
            );
            drop(state);
            commit
        };
        let commit = commit?;
        effects::batch::build_receiver_intent(room, self.user_id, self.connection_id, commit)
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
        effects::batch::build_consumer_readiness(commit)
            .execute(room, RoomEffectContext::runtime(self.media_transport))
            .await;
        Some(())
    }

    async fn request_video_keyframes_if_present(self) -> Option<()> {
        let Some(targets) = self.video_keyframe_targets().await else {
            return Some(());
        };
        self.request_video_keyframes(targets).await;
        Some(())
    }

    async fn request_video_keyframes_required(self) -> Option<()> {
        let targets = self.video_keyframe_targets().await?;
        self.request_video_keyframes(targets).await;
        Some(())
    }

    async fn video_keyframe_targets(self) -> Option<Vec<ConsumerRouteTarget>> {
        let room = self.room;
        {
            let state = room.state.read().await;
            let targets = state.active_video_keyframe_targets(self.user_id, self.connection_id);
            drop(state);
            targets
        }
    }

    async fn request_video_keyframes(self, targets: Vec<ConsumerRouteTarget>) {
        effects::batch::build_keyframe_refresh(targets)
            .execute(self.room, RoomEffectContext::runtime(self.media_transport))
            .await;
    }
}

#[cfg(test)]
#[path = "TESTS/subscription.rs"]
mod tests;
