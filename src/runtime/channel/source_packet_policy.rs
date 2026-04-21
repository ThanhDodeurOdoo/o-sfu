use crate::runtime::transport_adapter::{ActiveSpeakerSource, MediaPort, ObservabilityPort};

use super::{Channel, effects::SourcePacketPolicyEffectPlan};

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
        self.sync_source_packet_selection_policy_from_active_speakers(
            &active_speaker_sources,
            media_port,
        )
        .await;
    }

    pub(super) async fn sync_source_packet_selection_policy_from_active_speakers(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        media_port: &impl MediaPort,
    ) {
        let effect_plan = {
            let state = self.state.read().await;
            SourcePacketPolicyEffectPlan::from_state(&state, active_speaker_sources)
        };
        if effect_plan.is_empty() {
            return;
        }
        effect_plan.execute(self, media_port).await;
    }
}
