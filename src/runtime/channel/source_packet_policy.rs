use crate::runtime::transport_adapter::{ActiveSpeakerSource, RuntimeTransportAdapter};

use super::{Channel, effects::SourcePacketPolicyEffectPlan};

impl Channel {
    pub(super) async fn sync_source_packet_selection_policy(
        &self,
        transport_adapter: Option<&RuntimeTransportAdapter>,
    ) {
        let Some(transport_adapter) = transport_adapter else {
            return;
        };
        let active_speaker_sources = transport_adapter.active_speaker_source_snapshot().await;
        self.sync_source_packet_selection_policy_from_active_speakers(
            &active_speaker_sources,
            transport_adapter,
        )
        .await;
    }

    pub(super) async fn sync_source_packet_selection_policy_from_active_speakers(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let effect_plan = {
            let state = self.state.read().await;
            SourcePacketPolicyEffectPlan::from_state(&state, active_speaker_sources)
        };
        if effect_plan.is_empty() {
            return;
        }
        effect_plan.execute(self, transport_adapter).await;
    }
}
