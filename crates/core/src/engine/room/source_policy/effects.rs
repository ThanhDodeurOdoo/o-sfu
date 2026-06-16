use tracing::{debug, warn};

use super::{
    super::{
        Room, RoomEventMessage, effects::consumer_route::ConsumerRouteEffect,
        outbound::MessageFanout, state::RoomState,
    },
    ConsumerPacketSelectionUpdate, FeaturedUserUpdate, audio,
    input::SourcePolicyInput,
    video,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, ConsumerPacketGateUpdate, MediaTransport, ReceiverBandwidthSnapshot,
        ReceiverBweTargetUpdate,
    },
    metrics::{self, BudgetSolverOutcome},
};

#[cfg(test)]
#[path = "TESTS/effects_support.rs"]
mod test_support;

#[derive(Debug, Default)]
pub struct SourcePolicyEffectPlan {
    packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
    featured_users: Vec<FeaturedUserUpdate>,
}

impl SourcePolicyEffectPlan {
    pub fn from_state(
        state: &RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let input = SourcePolicyInput::from_state(
            state,
            active_speaker_sources,
            receiver_bandwidth_snapshot,
        );
        let mut packet_updates = audio::audio_route_activity_updates(&input);
        let video_plan = video::receiver_video_policy_plan(state, &input);
        packet_updates.extend(video_plan.consumer_packet_updates);
        let featured_users = input.featured_user_updates;
        Self {
            packet_updates,
            receiver_bwe_targets: video_plan.receiver_bwe_targets,
            featured_users,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.packet_updates.is_empty()
            && self.receiver_bwe_targets.is_empty()
            && self.featured_users.is_empty()
    }

    pub async fn execute(self, room: &Room, media_port: &MediaTransport) {
        let Self {
            packet_updates,
            receiver_bwe_targets,
            featured_users,
        } = self;
        media_port
            .set_receiver_bwe_targets(&receiver_bwe_targets)
            .await;
        let applied_packet_updates = apply_packet_updates(media_port, packet_updates).await;
        if applied_packet_updates.is_empty() && featured_users.is_empty() {
            return;
        }
        Self::record_source_selection_metrics(room, &applied_packet_updates);
        let info_fanout = {
            let mut state = room.state.write().await;
            commit_packet_updates(&mut state, &applied_packet_updates);
            commit_featured_user_updates(&mut state, &featured_users)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }

    fn record_source_selection_metrics(room: &Room, updates: &[ConsumerPacketSelectionUpdate]) {
        for update in updates {
            if update.packet_gate.is_some() {
                room.metrics
                    .record_source_selection_update(metrics::source_selection_kind(
                        update.selector,
                    ));
            }
            let outcomes = update.outcomes;
            if outcomes.is_degraded() {
                room.metrics
                    .record_budget_solver_outcome(BudgetSolverOutcome::Degraded);
            }
            if outcomes.is_paused() {
                room.metrics
                    .record_budget_solver_outcome(BudgetSolverOutcome::Paused);
            }
            if outcomes.is_resumed() {
                room.metrics
                    .record_budget_solver_outcome(BudgetSolverOutcome::Resumed);
            }
            if outcomes.is_protected_over_budget() {
                room.metrics
                    .record_budget_solver_outcome(BudgetSolverOutcome::ProtectedOverBudget);
            }
        }
    }
}

async fn apply_packet_updates(
    media_port: &MediaTransport,
    updates: Vec<ConsumerPacketSelectionUpdate>,
) -> Vec<ConsumerPacketSelectionUpdate> {
    let mut packet_gate_results = media_port
        .set_consumer_packet_gates(&packet_gate_updates(&updates))
        .await
        .into_iter();
    let mut applied_updates = Vec::with_capacity(updates.len());
    for update in updates {
        if let Some(update) =
            accepted_packet_update(media_port, update, &mut packet_gate_results).await
        {
            applied_updates.push(update);
        }
    }
    applied_updates
}

async fn accepted_packet_update<E>(
    media_port: &MediaTransport,
    update: ConsumerPacketSelectionUpdate,
    packet_gate_results: &mut impl Iterator<Item = Result<(), E>>,
) -> Option<ConsumerPacketSelectionUpdate> {
    if update.packet_gate.is_some() && !matches!(packet_gate_results.next(), Some(Ok(()))) {
        warn!(
            route = ?update.route,
            "media transport rejected the receiver-driven packet selection update"
        );
        return None;
    }
    let outcome = ConsumerRouteEffect::new(&update.transport_route)
        .with_activity_if(update.route_activity_update, update.route_active())
        .with_keyframe(update.request_keyframe)
        .execute(media_port)
        .await;
    if outcome.activity_failed {
        warn!(
            route = ?update.route,
            route_active = update.route_active(),
            "media transport failed to apply source policy route activity"
        );
        return None;
    }
    log_keyframe_outcome(&update, outcome.keyframe_failed);
    debug!(
        route = ?update.route,
        selector = ?update.selector,
        policy_pause_reason = ?update.policy_pause_reason,
        packet_gate = ?update.packet_gate,
        request_keyframe = update.request_keyframe,
        "media transport accepted receiver-driven packet selection update"
    );
    Some(update)
}

fn log_keyframe_outcome(update: &ConsumerPacketSelectionUpdate, failed: bool) {
    if failed {
        warn!(
            route = ?update.route,
            "media transport failed to request an adaptation keyframe refresh"
        );
    } else if update.request_keyframe {
        debug!(
            route = ?update.route,
            "media transport accepted adaptation keyframe refresh"
        );
    }
}

fn packet_gate_updates(updates: &[ConsumerPacketSelectionUpdate]) -> Vec<ConsumerPacketGateUpdate> {
    updates
        .iter()
        .filter_map(|update| {
            debug!(
                route = ?update.route,
                selector = ?update.selector,
                policy_pause_reason = ?update.policy_pause_reason,
                packet_gate = ?update.packet_gate,
                request_keyframe = update.request_keyframe,
                "prepared receiver-driven packet selection update"
            );
            update.packet_gate.as_ref().map(|packet_gate| {
                ConsumerPacketGateUpdate::new(update.transport_route.clone(), packet_gate.clone())
            })
        })
        .collect()
}

fn commit_packet_updates(state: &mut RoomState, updates: &[ConsumerPacketSelectionUpdate]) {
    for update in updates {
        state.update_source_policy_consumer_selection(
            &update.route,
            update.source_id,
            |selection| {
                selection.set_selector(update.selector);
                selection.set_policy_pause_reason(update.policy_pause_reason);
                selection.set_budget(update.budget);
                selection.set_adaptation_observations(
                    update.pressure_observations,
                    update.upgrade_observations,
                );
            },
        );
    }
}

fn commit_featured_user_updates(
    state: &mut RoomState,
    updates: &[FeaturedUserUpdate],
) -> Option<MessageFanout> {
    let mut changed_user_ids = Vec::new();
    for update in updates {
        if state.update_source_policy_featured_user(&update.user_id, update.featured) {
            changed_user_ids.push(update.user_id.clone());
        }
    }
    if changed_user_ids.is_empty() {
        return None;
    }
    let snapshot = changed_user_ids
        .into_iter()
        .filter_map(|user_id| state.user_info_snapshot(&user_id))
        .collect();
    Some(state.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)))
}
