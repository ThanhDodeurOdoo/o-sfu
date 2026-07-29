use o_sfu_router::MediaKind;

use super::{
    action::ConsumerPacketSelectionUpdate, input::SourcePolicySnapshot,
    turn::SourcePolicyTransaction,
};
use crate::engine::{room::media_graph::ConsumerRouteView, source_model::PolicyPauseReason};

const AUDIO_PAUSE_REASONS: [PolicyPauseReason; 2] = [
    PolicyPauseReason::AudioSpeakerLimit,
    PolicyPauseReason::ReceiverDeafened,
];

pub(super) fn append_audio_route_activity(
    tx: &mut SourcePolicyTransaction,
    input: &SourcePolicySnapshot<'_>,
) {
    for route in &input.routes {
        if route.source.descriptor.media_kind() != MediaKind::Audio {
            continue;
        }
        let next_reason = audio_pause_reason(input, route);
        if next_reason.is_none() && !owns_pause_reason(route.selection.policy_pause_reason()) {
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

fn audio_pause_reason(
    input: &SourcePolicySnapshot<'_>,
    route: &ConsumerRouteView<'_>,
) -> Option<PolicyPauseReason> {
    if input
        .deaf_receiver_connection_ids
        .contains(&route.route.consumer_session_key().connection_id())
    {
        return Some(PolicyPauseReason::ReceiverDeafened);
    }
    let source_media_id = route.route.source_transport_media_id();
    let active_speaker = input.active_speaker_media_ids.contains(&source_media_id);
    let admitted = input.admitted_audio_media_ids.contains(&source_media_id);
    (active_speaker && !admitted).then_some(PolicyPauseReason::AudioSpeakerLimit)
}

fn owns_pause_reason(current_reason: Option<PolicyPauseReason>) -> bool {
    current_reason.is_some_and(|reason| AUDIO_PAUSE_REASONS.contains(&reason))
}
