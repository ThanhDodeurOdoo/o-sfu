//! source-policy synchronization without transport awaits under the room lock

use super::SourcePolicyEffectPlan;
use crate::engine::{
    media_transport::{ActiveSpeakerSource, MediaTransport},
    room::Room,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "source-policy events intentionally name the changed room dimension at call sites"
)]
pub enum SourcePolicyEvent {
    RouteGraphChanged,
    ReceiverIntentChanged,
    FanoutPressureChanged,
}

impl Room {
    pub async fn handle_source_policy_event(
        &self,
        event: SourcePolicyEvent,
        media_transport: Option<&MediaTransport>,
    ) {
        match event {
            SourcePolicyEvent::RouteGraphChanged => {
                self.observe_source_fanout_pressure().await;
                if let Some(media_transport) = media_transport {
                    self.sync_source_packet_selection_policy(media_transport)
                        .await;
                }
            }
            SourcePolicyEvent::ReceiverIntentChanged => {
                if let Some(media_transport) = media_transport {
                    self.sync_source_packet_selection_policy(media_transport)
                        .await;
                }
            }
            SourcePolicyEvent::FanoutPressureChanged => {
                self.observe_source_fanout_pressure().await;
            }
        }
    }

    pub async fn sync_source_packet_selection_policy(&self, media_transport: &MediaTransport) {
        let active_speaker_sources = media_transport.active_speaker_source_snapshot().await;
        self.sync_source_packet_selection_policy_from_observations(
            &active_speaker_sources,
            media_transport,
        )
        .await;
    }

    /// reads state twice so transport observability never runs under the room lock
    pub async fn sync_source_packet_selection_policy_from_observations(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        media_transport: &MediaTransport,
    ) {
        let session_keys = {
            let state = self.state.read().await;
            state
                .transport_user_entries()
                .into_iter()
                .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
                .collect::<Vec<_>>()
        };
        let receiver_bandwidth_snapshot =
            media_transport.receiver_bandwidth_snapshot(&session_keys);
        let effect_plan = {
            let state = self.state.read().await;
            SourcePolicyEffectPlan::from_state(
                &state,
                active_speaker_sources,
                &receiver_bandwidth_snapshot,
            )
        };
        if effect_plan.is_empty() {
            return;
        }
        effect_plan.execute(self, media_transport).await;
    }
}
