use std::collections::BTreeMap;

use super::{
    super::{action::ReceiverVideoPolicyPlan, input::SourcePolicyInput},
    input::{ReceiverVideoRouteInput, receiver_video_routes},
    receiver,
};
use crate::engine::{UserId, media_transport::ReceiverBweTargetUpdate, room::state::RoomState};

pub fn receiver_video_policy_plan(
    state: &RoomState,
    input: &SourcePolicyInput<'_>,
) -> ReceiverVideoPolicyPlan {
    let routes = receiver_video_routes(state, input);
    receiver_video_selection_plan(
        &routes,
        input.receiver_bwe_targets.clone(),
        input.media_limits.max_video_downloads_per_receiver(),
    )
}

fn receiver_video_selection_plan(
    routes: &[ReceiverVideoRouteInput<'_>],
    mut receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
    max_video_downloads_per_receiver: usize,
) -> ReceiverVideoPolicyPlan {
    let mut selection_updates = Vec::with_capacity(routes.len());
    for receiver_routes in
        routes.chunk_by(|left, right| left.consumer_user_id() == right.consumer_user_id())
    {
        let Some(first_route) = receiver_routes.first() else {
            continue;
        };
        let consumer_user_id = first_route.consumer_user_id();
        let plan = receiver::plan(receiver_routes, max_video_downloads_per_receiver);
        if let Some(target) = receiver_bwe_targets.get_mut(consumer_user_id) {
            target.set_target(plan.receiver_bwe_target);
        }
        selection_updates.extend(plan.selection_updates);
    }
    ReceiverVideoPolicyPlan {
        consumer_packet_updates: selection_updates,
        receiver_bwe_targets: receiver_bwe_targets.into_values().collect(),
    }
}

#[cfg(test)]
#[path = "TESTS/planner.rs"]
mod tests;
