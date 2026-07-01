use o_sfu_router::MediaKind;

use super::{
    action::ConsumerPacketSelectionUpdate, input::SourcePolicySnapshot,
    turn::SourcePolicyTransaction,
};
use crate::engine::{room::state::RoomState, source_model::PolicyPauseReason};

pub(super) fn append_audio_route_activity(
    tx: &mut SourcePolicyTransaction,
    state: &RoomState,
    input: &SourcePolicySnapshot<'_>,
) {
    for route in &input.routes {
        if route.source.media_kind() != MediaKind::Audio {
            continue;
        }
        let active_speaker = input
            .active_speaker_media_ids
            .contains(&route.transport_ref.source_media);
        let admitted = input
            .admitted_audio_media_ids
            .contains(&route.transport_ref.source_media);
        let next_reason =
            (active_speaker && !admitted).then_some(PolicyPauseReason::AudioSpeakerLimit);
        if next_reason.is_none()
            && route.current_selection.policy_pause_reason()
                != Some(PolicyPauseReason::AudioSpeakerLimit)
        {
            continue;
        }
        if let Some(update) = ConsumerPacketSelectionUpdate::route_activity(
            route.transport_ref.clone(),
            route.source.source_id(),
            route.current_selection,
            next_reason,
        ) {
            let target = state
                .topology
                .consumer_route_target_for_source(update.transport_ref.clone(), route.source);
            tx.push_route_update(update, &target);
        }
    }
}
