use o_sfu_router::MediaKind;

use super::{
    action::ConsumerPacketSelectionUpdate, input::SourcePolicySnapshot,
    turn::SourcePolicyTransaction,
};
use crate::engine::source_model::PolicyPauseReason;

pub(super) fn append_audio_route_activity(
    tx: &mut SourcePolicyTransaction,
    input: &SourcePolicySnapshot<'_>,
) {
    for route in &input.routes {
        if route.source.descriptor.media_kind() != MediaKind::Audio {
            continue;
        }
        let active_speaker = input
            .active_speaker_media_ids
            .contains(&route.route.source_transport_media_id());
        let admitted = input
            .admitted_audio_media_ids
            .contains(&route.route.source_transport_media_id());
        let next_reason =
            (active_speaker && !admitted).then_some(PolicyPauseReason::AudioSpeakerLimit);
        if next_reason.is_none()
            && route.selection.policy_pause_reason() != Some(PolicyPauseReason::AudioSpeakerLimit)
        {
            continue;
        }
        if let Some(update) = ConsumerPacketSelectionUpdate::route_activity(
            route.key.clone(),
            route.source.descriptor.source_id(),
            route.route.clone(),
            route.selection,
            next_reason,
        ) {
            tx.push_route_update(update);
        }
    }
}
