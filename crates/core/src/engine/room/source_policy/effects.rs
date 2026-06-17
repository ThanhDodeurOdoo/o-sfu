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
        ReceiverBweTargetUpdate, TransportAdapterError, TransportConsumerRoute,
    },
    metrics::{self, BudgetSolverOutcome},
};

#[cfg(test)]
#[path = "TESTS/effects_support.rs"]
mod test_support;

#[derive(Debug, Default)]
pub struct SourcePolicyEffectPlan {
    state_only_packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    transport_effect_packet_updates: Vec<PacketUpdateWithRoute>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
    featured_users: Vec<FeaturedUserUpdate>,
}

type PacketUpdateWithRoute = (ConsumerPacketSelectionUpdate, TransportConsumerRoute);

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
        let audio_updates = audio::audio_route_activity_updates(&input);
        let video_plan = video::receiver_video_policy_plan(state, &input);
        let (state_only_packet_updates, transport_effect_packet_updates) = split_packet_updates(
            state,
            audio_updates
                .into_iter()
                .chain(video_plan.consumer_packet_updates),
        );
        let featured_users = input.featured_user_updates;
        Self {
            state_only_packet_updates,
            transport_effect_packet_updates,
            receiver_bwe_targets: video_plan.receiver_bwe_targets,
            featured_users,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state_only_packet_updates.is_empty()
            && self.transport_effect_packet_updates.is_empty()
            && self.receiver_bwe_targets.is_empty()
            && self.featured_users.is_empty()
    }

    pub async fn execute(self, room: &Room, media_port: &MediaTransport) {
        let Self {
            state_only_packet_updates,
            transport_effect_packet_updates,
            receiver_bwe_targets,
            featured_users,
        } = self;
        media_port
            .set_receiver_bwe_targets(&receiver_bwe_targets)
            .await;
        let applied_packet_updates = apply_packet_updates(
            media_port,
            state_only_packet_updates,
            transport_effect_packet_updates,
        )
        .await;
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
    state_only_updates: Vec<ConsumerPacketSelectionUpdate>,
    transport_effect_updates: Vec<PacketUpdateWithRoute>,
) -> Vec<ConsumerPacketSelectionUpdate> {
    if transport_effect_updates.is_empty() {
        return state_only_updates;
    }
    let packet_gate_updates = packet_gate_updates(&transport_effect_updates);
    let packet_gate_results = if packet_gate_updates.is_empty() {
        Vec::new()
    } else {
        media_port
            .set_consumer_packet_gates(&packet_gate_updates)
            .await
    };
    let mut packet_gate_results = packet_gate_results.into_iter();
    let mut applied_updates = state_only_updates;
    applied_updates.reserve(transport_effect_updates.len());
    for update in transport_effect_updates {
        if let Some(update) =
            apply_transport_packet_update(media_port, update, &mut packet_gate_results).await
        {
            applied_updates.push(update);
        }
    }
    applied_updates
}

async fn apply_transport_packet_update(
    media_port: &MediaTransport,
    (update, captured_route): PacketUpdateWithRoute,
    packet_gate_results: &mut impl Iterator<Item = Result<(), TransportAdapterError>>,
) -> Option<ConsumerPacketSelectionUpdate> {
    if update.packet_gate.is_some() && !matches!(packet_gate_results.next(), Some(Ok(()))) {
        warn!(
            route = ?update.route,
            "media transport rejected the receiver-driven packet selection update"
        );
        return None;
    }
    let outcome = ConsumerRouteEffect::new(&captured_route)
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

fn split_packet_updates(
    state: &RoomState,
    updates: impl IntoIterator<Item = ConsumerPacketSelectionUpdate>,
) -> (
    Vec<ConsumerPacketSelectionUpdate>,
    Vec<PacketUpdateWithRoute>,
) {
    let mut state_only_updates = Vec::new();
    let mut transport_effect_updates = Vec::new();
    for update in updates {
        if requires_media_transport_effect(&update) {
            let transport_route = state.transport_consumer_route(&update.route);
            transport_effect_updates.push((update, transport_route));
        } else {
            state_only_updates.push(update);
        }
    }
    (state_only_updates, transport_effect_updates)
}

fn requires_media_transport_effect(update: &ConsumerPacketSelectionUpdate) -> bool {
    update.packet_gate.is_some() || update.route_activity_update || update.request_keyframe
}

fn packet_gate_updates(updates: &[PacketUpdateWithRoute]) -> Vec<ConsumerPacketGateUpdate> {
    updates
        .iter()
        .filter_map(|(update, transport_route)| {
            debug!(
                route = ?update.route,
                selector = ?update.selector,
                policy_pause_reason = ?update.policy_pause_reason,
                packet_gate = ?update.packet_gate,
                request_keyframe = update.request_keyframe,
                "prepared receiver-driven packet selection update"
            );
            update.packet_gate.as_ref().map(|packet_gate| {
                ConsumerPacketGateUpdate::new(transport_route.clone(), packet_gate.clone())
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
