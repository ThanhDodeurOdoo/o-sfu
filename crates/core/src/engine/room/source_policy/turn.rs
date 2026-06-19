//! source-policy turn ownership without transport awaits under the room lock

use tracing::{debug, warn};

use super::{
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
    audio,
    input::SourcePolicyInput,
    video,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome,
        MediaTransport, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate, RouteControlPlan,
        TransportConsumerRoute,
    },
    metrics::{self, BudgetSolverOutcome},
    room::{Room, RoomEventMessage, outbound::MessageFanout, state::RoomState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourcePolicyTrigger {
    RouteGraph,
    PacketSelection,
    FanoutPressure,
}

impl SourcePolicyTrigger {
    pub const fn merge(self, next: Self) -> Self {
        use SourcePolicyTrigger::{FanoutPressure, PacketSelection, RouteGraph};

        match (self, next) {
            (RouteGraph, _)
            | (_, RouteGraph)
            | (PacketSelection, FanoutPressure)
            | (FanoutPressure, PacketSelection) => RouteGraph,
            (PacketSelection, PacketSelection) => PacketSelection,
            (FanoutPressure, FanoutPressure) => FanoutPressure,
        }
    }
}

#[derive(Debug)]
pub struct SourcePolicyTurn<'a> {
    room: &'a Room,
    trigger: SourcePolicyTrigger,
    media_transport: Option<&'a MediaTransport>,
    active_speakers: Option<&'a [ActiveSpeakerSource]>,
}

impl<'a> SourcePolicyTurn<'a> {
    pub const fn new(
        room: &'a Room,
        trigger: SourcePolicyTrigger,
        media_transport: Option<&'a MediaTransport>,
    ) -> Self {
        Self {
            room,
            trigger,
            media_transport,
            active_speakers: None,
        }
    }

    pub const fn with_active_speakers(mut self, sources: &'a [ActiveSpeakerSource]) -> Self {
        self.active_speakers = Some(sources);
        self
    }

    pub async fn run(self) {
        if matches!(
            self.trigger,
            SourcePolicyTrigger::RouteGraph | SourcePolicyTrigger::FanoutPressure
        ) {
            self.room.observe_source_fanout_pressure().await;
        }
        let Some(media_transport) = self.media_transport else {
            return;
        };
        if self.trigger == SourcePolicyTrigger::FanoutPressure {
            return;
        }
        if let Some(sources) = self.active_speakers {
            self.run_packet_selection(sources, media_transport).await;
        } else {
            let sources = media_transport.active_speaker_source_snapshot().await;
            self.run_packet_selection(&sources, media_transport).await;
        }
    }

    async fn run_packet_selection(
        &self,
        active_speakers: &[ActiveSpeakerSource],
        media_transport: &MediaTransport,
    ) {
        let sessions = {
            let state = self.room.state.read().await;
            state
                .transport_user_entries()
                .into_iter()
                .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
                .collect::<Vec<_>>()
        };
        let bandwidth = media_transport.receiver_bandwidth_snapshot(&sessions);
        let plan = {
            let state = self.room.state.read().await;
            SourcePolicyPlan::from_state(&state, active_speakers, &bandwidth)
        };
        if !plan.is_empty() {
            plan.execute(self.room, media_transport).await;
        }
    }
}

#[cfg(test)]
#[path = "TESTS/turn_support.rs"]
mod test_support;

#[derive(Debug, Default)]
pub struct SourcePolicyPlan {
    state_only_packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    transport_effect_packet_updates: Vec<PacketUpdateWithRoute>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
    featured_users: Vec<FeaturedUserUpdate>,
}

type PacketUpdateWithRoute = (ConsumerPacketSelectionUpdate, TransportConsumerRoute);

impl SourcePolicyPlan {
    pub fn from_state(
        state: &RoomState,
        active_speakers: &[ActiveSpeakerSource],
        bandwidth: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let input = SourcePolicyInput::from_state(state, active_speakers, bandwidth);
        let audio_updates = audio::audio_route_activity_updates(&input);
        let video_plan = video::receiver_video_policy_plan(state, &input);
        let (state_only_packet_updates, transport_effect_packet_updates) = split_packet_updates(
            state,
            audio_updates
                .into_iter()
                .chain(video_plan.consumer_packet_updates),
        );
        Self {
            state_only_packet_updates,
            transport_effect_packet_updates,
            receiver_bwe_targets: video_plan.receiver_bwe_targets,
            featured_users: input.featured_user_updates,
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
        let outcomes =
            if receiver_bwe_targets.is_empty() && transport_effect_packet_updates.is_empty() {
                Vec::new()
            } else {
                let mut plan = RouteControlPlan::new();
                plan.set_receiver_bwe_targets(receiver_bwe_targets);
                push_packet_updates(&mut plan, &transport_effect_packet_updates);
                let outcome = media_port.apply_route_control(plan.ready()).await;
                drop(outcome.receiver_bwe_targets);
                outcome.consumers
            };
        let applied_packet_updates = apply_packet_updates(
            state_only_packet_updates,
            transport_effect_packet_updates,
            outcomes,
        );
        if applied_packet_updates.is_empty() && featured_users.is_empty() {
            return;
        }
        record_source_selection_metrics(room, &applied_packet_updates);
        let info_fanout = {
            let mut state = room.state.write().await;
            commit_packet_updates(&mut state, &applied_packet_updates);
            commit_featured_user_updates(&mut state, &featured_users)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }
}

fn record_source_selection_metrics(room: &Room, updates: &[ConsumerPacketSelectionUpdate]) {
    for update in updates {
        if update.packet_gate.is_some() {
            room.metrics
                .record_source_selection_update(metrics::source_selection_kind(update.selector));
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

fn apply_packet_updates(
    state_only_updates: Vec<ConsumerPacketSelectionUpdate>,
    transport_effect_updates: Vec<PacketUpdateWithRoute>,
    outcomes: Vec<ConsumerRouteControlOutcome>,
) -> Vec<ConsumerPacketSelectionUpdate> {
    if transport_effect_updates.is_empty() {
        return state_only_updates;
    }
    debug_assert_eq!(transport_effect_updates.len(), outcomes.len());
    let mut applied_updates = state_only_updates;
    applied_updates.reserve(transport_effect_updates.len());
    for ((update, _route), outcome) in transport_effect_updates.into_iter().zip(outcomes) {
        if let Some(update) = apply_transport_packet_update(update, outcome) {
            applied_updates.push(update);
        }
    }
    applied_updates
}

fn apply_transport_packet_update(
    update: ConsumerPacketSelectionUpdate,
    outcome: ConsumerRouteControlOutcome,
) -> Option<ConsumerPacketSelectionUpdate> {
    if outcome.packet_gate_failed() {
        warn!(
            route = ?update.route,
            "media transport rejected the receiver-driven packet selection update"
        );
        return None;
    }
    if outcome.activity_failed() {
        warn!(
            route = ?update.route,
            route_active = update.route_active(),
            "media transport failed to apply source policy route activity"
        );
        return None;
    }
    log_keyframe_outcome(&update, outcome.keyframe_failed());
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

fn push_packet_updates(plan: &mut RouteControlPlan, updates: &[PacketUpdateWithRoute]) {
    for (update, transport_route) in updates {
        debug!(
            route = ?update.route,
            selector = ?update.selector,
            policy_pause_reason = ?update.policy_pause_reason,
            packet_gate = ?update.packet_gate,
            request_keyframe = update.request_keyframe,
            "prepared receiver-driven packet selection update"
        );
        let mut control =
            ConsumerRouteControl::new(transport_route.clone()).keyframe(update.request_keyframe);
        if update.route_activity_update {
            control = control.activity(ConsumerActivity::from_active(update.route_active()));
        }
        if let Some(packet_gate) = &update.packet_gate {
            control = control.packet_gate(packet_gate.clone());
        }
        plan.push_consumer(control);
    }
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
