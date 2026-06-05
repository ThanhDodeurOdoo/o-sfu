use o_sfu_router::MediaCapabilities;
use tracing::warn;

use super::{
    super::{SourcePolicyEvent, user_negotiation::UserNegotiationUpdate},
    RoomUserOperation,
};
use crate::{
    SessionNegotiationOutcome, UserInfoRefresh,
    engine::{
        UserInfo,
        media_transport::{TransportConsumerRoute, TransportSourceKey},
    },
};

impl RoomUserOperation<'_> {
    pub(crate) async fn update_user_info(self, info: UserInfo, refresh: UserInfoRefresh) {
        let need_refresh = refresh.is_needed();
        let room = self.room();
        let outcome = {
            let mut state = room.state.write().await;
            state.apply_presence_update(self.user_id(), self.connection_id(), &info, need_refresh)
        };
        if let Some(outcome) = outcome {
            room.handle_source_policy_event(
                SourcePolicyEvent::ReceiverIntentChanged,
                Some(self.media_transport()),
            )
            .await;
            outcome.emit();
        } else {
            warn!(
                user_id = ?self.user_id(),
                connection_id = ?self.connection_id(),
                ?info,
                need_refresh,
                "user info update was rejected by room state"
            );
        }
    }

    pub(crate) async fn apply_session_negotiated(
        self,
        capabilities: MediaCapabilities,
    ) -> SessionNegotiationOutcome {
        let update = {
            let mut state = self.room().state.write().await;
            state.set_user_negotiated(self.user_id(), self.connection_id(), capabilities)
        };
        self.apply_negotiation_update(update).await
    }

    pub(crate) async fn apply_session_refreshed(self) -> SessionNegotiationOutcome {
        if !self.request_video_keyframes().await {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if !self.setup_missing_consumers().await {
            return SessionNegotiationOutcome::StaleConnection;
        }
        SessionNegotiationOutcome::Applied
    }

    async fn apply_negotiation_update(
        self,
        update: Option<UserNegotiationUpdate>,
    ) -> SessionNegotiationOutcome {
        let Some(update) = update else {
            return SessionNegotiationOutcome::StaleConnection;
        };
        if update == UserNegotiationUpdate::BecameConsumerReady {
            if !self.setup_missing_consumers().await {
                return SessionNegotiationOutcome::StaleConnection;
            }
            self.request_video_keyframes().await;
        }
        SessionNegotiationOutcome::Applied
    }

    async fn request_video_keyframes(self) -> bool {
        let room = self.room();
        let keyframe_refresh_targets = {
            let state = room.state.read().await;
            let Some(targets) =
                state.active_video_keyframe_targets(self.user_id(), self.connection_id())
            else {
                return false;
            };
            let consumer_session_key =
                state.transport_user_key(self.user_id(), self.connection_id());
            let mut refresh_targets = Vec::with_capacity(targets.len());
            for target in targets {
                let producer_session_key = state
                    .transport_user_key(&target.producer_user_id, target.producer_connection_id);
                let route = TransportConsumerRoute::new(
                    consumer_session_key.clone(),
                    target.consumer_media,
                    TransportSourceKey::new(producer_session_key, target.source_media),
                );
                refresh_targets.push((target, route));
            }
            drop(state);
            refresh_targets
        };
        for (target, route) in keyframe_refresh_targets {
            if self
                .media_transport()
                .request_consumer_keyframe(&route)
                .await
                .is_err()
            {
                warn!(
                    user_id = ?self.user_id(),
                    connection_id = ?self.connection_id(),
                    producer_user_id = ?target.producer_user_id,
                    source_transport_media_id = ?target.source_media,
                    "media transport failed to request a refreshed consumer keyframe"
                );
            }
        }
        true
    }
}
