use o_sfu_router::MediaKind;

use super::{action::ConsumerPacketSelectionUpdate, input::SourcePolicyInput};
use crate::engine::source_model::PolicyPauseReason;

pub(super) fn audio_route_activity_updates(
    input: &SourcePolicyInput<'_>,
) -> Vec<ConsumerPacketSelectionUpdate> {
    let mut updates = Vec::with_capacity(input.routes.len());
    for route in &input.routes {
        if route.source.media_kind() != MediaKind::Audio {
            continue;
        }
        let active_speaker = input
            .active_speaker_media_ids
            .contains(&route.route.source_media);
        let admitted = input
            .admitted_audio_media_ids
            .contains(&route.route.source_media);
        let next_reason =
            (active_speaker && !admitted).then_some(PolicyPauseReason::AudioSpeakerLimit);
        if next_reason.is_none()
            && route.current_selection.policy_pause_reason()
                != Some(PolicyPauseReason::AudioSpeakerLimit)
        {
            continue;
        }
        if let Some(update) = ConsumerPacketSelectionUpdate::route_activity(
            route.route.clone(),
            route.source.source_id(),
            route.current_selection,
            next_reason,
        ) {
            updates.push(update);
        }
    }
    updates
}
