use super::{Channel, effects::SourcePacketPolicyEffectPlan};
use crate::runtime::transport_adapter::{ActiveSpeakerSource, MediaPort, ObservabilityPort};

impl Channel {
    pub(super) async fn sync_source_packet_selection_policy(
        &self,
        observability_port: Option<&impl ObservabilityPort>,
        media_port: &impl MediaPort,
    ) {
        let Some(observability_port) = observability_port else {
            return;
        };
        let active_speaker_sources = observability_port.active_speaker_source_snapshot().await;
        self.sync_source_packet_selection_policy_from_observations(
            &active_speaker_sources,
            observability_port,
            media_port,
        )
        .await;
    }

    pub(super) async fn sync_source_packet_selection_policy_from_observations(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        let session_keys = {
            let state = self.state.read().await;
            state
                .transport_session_entries()
                .into_iter()
                .map(|(session_id, connection_id)| {
                    self.transport_session_key(&session_id, connection_id)
                })
                .collect::<Vec<_>>()
        };
        let receiver_bandwidth_snapshot =
            observability_port.receiver_bandwidth_snapshot(&session_keys);
        let effect_plan = {
            let state = self.state.read().await;
            SourcePacketPolicyEffectPlan::from_state(
                &state,
                active_speaker_sources,
                &receiver_bandwidth_snapshot,
            )
        };
        if effect_plan.is_empty() {
            return;
        }
        effect_plan.execute(self, media_port).await;
    }
}
