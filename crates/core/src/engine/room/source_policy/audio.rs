use o_sfu_router::MediaKind;

use super::{
    action::{ConsumerPacketSelectionUpdate, TransportPacketSelectionUpdate},
    input::SourcePolicyInput,
};
use crate::engine::{room::media_graph::RoomTopology, source_model::PolicyPauseReason};

pub(super) fn audio_route_activity_updates(
    topology: &RoomTopology,
    input: &SourcePolicyInput<'_>,
) -> Vec<TransportPacketSelectionUpdate> {
    let mut updates = Vec::with_capacity(input.routes.len());
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
            let target = topology
                .consumer_route_target_for_source(update.transport_ref.clone(), route.source);
            updates.push(TransportPacketSelectionUpdate { update, target });
        }
    }
    updates
}
