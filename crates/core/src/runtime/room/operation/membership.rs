use o_sfu_router::MediaCapabilities;
use tracing::warn;

use super::{
    super::{SourcePolicyEvent, user_negotiation::UserNegotiationUpdate},
    RoomUserOperation,
};
use crate::{
    SessionNegotiationOutcome, UserInfoRefresh,
    runtime::{
        UserInfo,
        media_transport::{TransportConsumerRoute, TransportSourceKey},
    },
};

impl RoomUserOperation<'_> {
    /// Apply client-visible user info for one live connection.
    ///
    /// Room state decides whether the update is still current, then returns a
    /// fan-out plan that is emitted after the lock is released. A refresh may
    /// trigger a full projection fan-out and a source selection policy sync
    /// because layout or presence changes can affect video priority.
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

    /// Commit the answer-derived negotiated capability set for one live session.
    ///
    /// This is called after the transport boundary has accepted the browser
    /// answer and projected the negotiated RTP capabilities. Room state records
    /// the session as consumer-ready, then any missing consumer bootstrap runs
    /// outside the state lock.
    pub(crate) async fn apply_session_negotiated(
        self,
        capabilities: MediaCapabilities,
    ) -> SessionNegotiationOutcome {
        let update = {
            let mut state = self.room().state.write().await;
            state.set_user_negotiated(self.user_id(), self.connection_id(), &capabilities)
        };
        self.apply_negotiation_update(update).await
    }

    /// Refresh consumer-side media after a renegotiation answer.
    ///
    /// This does not update the stored RTP capability set. It revalidates that
    /// the connection is still current, requests keyframes for active video
    /// consumers and bootstraps any consumers that became possible after the
    /// renegotiation.
    pub(crate) async fn apply_session_refreshed(self) -> SessionNegotiationOutcome {
        if !self.request_active_video_consumer_keyframes().await {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if !self.bootstrap_missing_consumers().await {
            return SessionNegotiationOutcome::StaleConnection;
        }
        SessionNegotiationOutcome::Applied
    }

    /// Apply the side effects that follow a negotiation state change.
    ///
    /// Consumer bootstrap is deferred until room state says the session became
    /// ready to receive. Keyframe refresh requests are best-effort transport
    /// hints and do not make the negotiation outcome fail.
    async fn apply_negotiation_update(
        self,
        update: UserNegotiationUpdate,
    ) -> SessionNegotiationOutcome {
        if !update.session_present {
            return SessionNegotiationOutcome::StaleConnection;
        }
        if update.became_consumer_ready {
            if !self.bootstrap_missing_consumers().await {
                return SessionNegotiationOutcome::StaleConnection;
            }
            self.request_active_video_consumer_keyframes().await;
        }
        SessionNegotiationOutcome::Applied
    }

    /// Request keyframes for active video consumers owned by one live session.
    ///
    /// The target list is an authoritative room-state snapshot for the current
    /// connection. Individual transport request failures are logged but kept
    /// best-effort because a later media packet or refresh can recover video.
    async fn request_active_video_consumer_keyframes(self) -> bool {
        let room = self.room();
        let Some(keyframe_refresh_targets) = ({
            let state = room.state.read().await;
            state.active_video_consumer_keyframe_refresh_targets(
                self.user_id(),
                self.connection_id(),
            )
        }) else {
            return false;
        };
        for target in keyframe_refresh_targets {
            let route = TransportConsumerRoute::new(
                self.transport_user_key(),
                target.consumer_media,
                TransportSourceKey::new(
                    room.transport_user_key(
                        &target.producer_user_id,
                        target.producer_connection_id,
                    ),
                    target.source_media,
                ),
            );
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
