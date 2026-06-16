use o_sfu_router::MediaKind;

use super::{action::ConsumerPacketSelectionUpdate, input::SourcePolicyInput};
use crate::engine::source_model::PolicyPauseReason;

pub(super) fn audio_route_activity_updates(
    input: &SourcePolicyInput<'_>,
) -> Vec<ConsumerPacketSelectionUpdate> {
    input
        .routes
        .iter()
        .filter_map(|route| {
            if route.source.media_kind() != MediaKind::Audio {
                return None;
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
                return None;
            }
            ConsumerPacketSelectionUpdate::route_activity(
                route.route.clone(),
                route.transport_route.clone(),
                route.source.source_id(),
                route.current_selection,
                next_reason,
            )
        })
        .collect()
}
