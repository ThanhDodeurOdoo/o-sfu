use std::collections::BTreeMap;

use super::{
    super::{action::ReceiverVideoPolicyPlan, input::SourcePolicyInput},
    input::{ReceiverVideoRouteInput, receiver_video_routes},
    receiver,
};
use crate::engine::{
    UserId,
    media_transport::ReceiverBweTargetUpdate,
    room::{media_graph::RoomTopology, state::RoomState},
};

pub(in crate::engine::room::source_policy) fn receiver_video_policy_plan(
    state: &RoomState,
    input: &SourcePolicyInput<'_>,
) -> ReceiverVideoPolicyPlan {
    let routes = receiver_video_routes(state, input);
    receiver_video_selection_plan(
        &state.topology,
        &routes,
        input.receiver_bwe_targets.clone(),
        input.media_limits.max_video_downloads_per_receiver(),
    )
}

fn receiver_video_selection_plan(
    topology: &RoomTopology,
    routes: &[ReceiverVideoRouteInput<'_>],
    mut receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
    max_video_downloads_per_receiver: usize,
) -> ReceiverVideoPolicyPlan {
    let mut state_packet_updates = Vec::new();
    let mut transport_packet_updates = Vec::new();
    for receiver_routes in routes.chunk_by(|left, right| {
        left.transport_ref.consumer_user_id == right.transport_ref.consumer_user_id
    }) {
        let Some(first_route) = receiver_routes.first() else {
            continue;
        };
        let consumer_user_id = &first_route.transport_ref.consumer_user_id;
        let plan = receiver::plan(topology, receiver_routes, max_video_downloads_per_receiver);
        if let Some(target) = receiver_bwe_targets.get_mut(consumer_user_id) {
            target.set_target(plan.receiver_bwe_target);
        }
        state_packet_updates.extend(plan.state_packet_updates);
        transport_packet_updates.extend(plan.transport_packet_updates);
    }
    ReceiverVideoPolicyPlan {
        state_packet_updates,
        transport_packet_updates,
        receiver_bwe_targets: receiver_bwe_targets.into_values().collect(),
    }
}

#[cfg(test)]
#[path = "TESTS/planner.rs"]
mod tests;
