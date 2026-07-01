use std::collections::BTreeMap;

use super::{
    super::{input::SourcePolicySnapshot, turn::SourcePolicyTransaction},
    input::{ReceiverVideoRouteInput, receiver_video_routes},
    receiver,
};
use crate::engine::{UserId, media_transport::ReceiverBweTargetUpdate, room::state::RoomState};

pub(in crate::engine::room::source_policy) fn append_receiver_video_policy(
    tx: &mut SourcePolicyTransaction,
    state: &RoomState,
    input: &SourcePolicySnapshot<'_>,
    receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
) {
    let routes = receiver_video_routes(state, input);
    append_receiver_video_selection(
        tx,
        state,
        &routes,
        receiver_bwe_targets,
        input.media_limits.max_video_downloads_per_receiver(),
    );
}

fn append_receiver_video_selection(
    tx: &mut SourcePolicyTransaction,
    state: &RoomState,
    routes: &[ReceiverVideoRouteInput<'_>],
    mut receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
    max_video_downloads_per_receiver: usize,
) {
    for receiver_routes in routes.chunk_by(|left, right| {
        left.transport_ref.consumer_user_id == right.transport_ref.consumer_user_id
    }) {
        let Some(first_route) = receiver_routes.first() else {
            continue;
        };
        let consumer_user_id = &first_route.transport_ref.consumer_user_id;
        let receiver_bwe_target = receiver::append_policy_updates(
            tx,
            state,
            receiver_routes,
            max_video_downloads_per_receiver,
        );
        if let Some(target) = receiver_bwe_targets.get_mut(consumer_user_id) {
            target.set_target(receiver_bwe_target);
        }
    }
    tx.set_receiver_bwe_targets(receiver_bwe_targets.into_values().collect());
}

#[cfg(test)]
#[path = "TESTS/planner.rs"]
mod tests;
