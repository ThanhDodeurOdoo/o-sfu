//! source-policy apply ownership without transport awaits under the room lock

use std::mem;

use tracing::warn;

use super::{
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
    audio,
    input::SourcePolicySnapshot,
    video,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome,
        MediaTransport, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate, RouteControlPlan,
        TransportConsumerRoute,
    },
    metrics::{self, BudgetSolverOutcome},
    room::{
        Room, RoomEventMessage, media_graph::ConsumerRouteTarget, outbound::MessageFanout,
        state::RoomState,
    },
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

/// deferred source-policy trigger emitted by room state transitions
///
/// multiple wakeups collapse to one strongest trigger
/// [`Self::plan`] runs policy planning after route effects and pre-policy output
/// drain, then room effects execute transport work before committing state
#[derive(Debug, Default, Clone, Copy)]
pub struct SourcePolicyWakeups {
    trigger: Option<SourcePolicyTrigger>,
}

impl SourcePolicyWakeups {
    pub fn route_graph_changed(&mut self) {
        self.push(SourcePolicyTrigger::RouteGraph);
    }

    pub fn receiver_intent_changed(&mut self) {
        self.push(SourcePolicyTrigger::PacketSelection);
    }

    pub fn fanout_pressure_changed(&mut self) {
        self.push(SourcePolicyTrigger::FanoutPressure);
    }

    pub fn extend(&mut self, wakeups: Self) {
        if let Some(trigger) = wakeups.trigger {
            self.push(trigger);
        }
    }

    pub async fn plan(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
    ) -> Option<SourcePolicyTransaction> {
        plan(room, self.trigger?, media_transport, None).await
    }

    fn push(&mut self, trigger: SourcePolicyTrigger) {
        self.trigger = Some(
            self.trigger
                .map_or(trigger, |current| current.merge(trigger)),
        );
    }
}

pub async fn plan(
    room: &Room,
    trigger: SourcePolicyTrigger,
    media_transport: Option<&MediaTransport>,
    active_speakers: Option<&[ActiveSpeakerSource]>,
) -> Option<SourcePolicyTransaction> {
    if matches!(
        trigger,
        SourcePolicyTrigger::RouteGraph | SourcePolicyTrigger::FanoutPressure
    ) {
        room.observe_source_fanout_pressure().await;
    }
    let media_transport = media_transport?;
    if trigger == SourcePolicyTrigger::FanoutPressure {
        return None;
    }
    if let Some(sources) = active_speakers {
        run_packet_selection(room, sources, media_transport).await
    } else {
        let sources = media_transport.active_speaker_source_snapshot().await;
        run_packet_selection(room, &sources, media_transport).await
    }
}

async fn run_packet_selection(
    room: &Room,
    active_speakers: &[ActiveSpeakerSource],
    media_transport: &MediaTransport,
) -> Option<SourcePolicyTransaction> {
    let sessions = {
        let state = room.state.read().await;
        state
            .transport_user_entries()
            .into_iter()
            .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let bandwidth = media_transport.receiver_bandwidth_snapshot(&sessions);
    let state = room.state.read().await;
    SourcePolicyTransaction::plan_from_state(&state, active_speakers, &bandwidth)
}

#[cfg(test)]
#[path = "TESTS/turn_support.rs"]
mod test_support;

#[derive(Debug)]
struct RouteFinish {
    selection: ConsumerPacketSelectionUpdate,
    route: TransportConsumerRoute,
}

#[derive(Debug, Default)]
pub(in crate::engine::room) struct SourcePolicyTransaction {
    route_control: RouteControlPlan<(), RouteFinish>,
    state_updates: Vec<ConsumerPacketSelectionUpdate>,
    featured_users: Vec<FeaturedUserUpdate>,
}

impl SourcePolicyTransaction {
    pub(in crate::engine::room) fn plan_from_state(
        state: &RoomState,
        active_speakers: &[ActiveSpeakerSource],
        bandwidth: &ReceiverBandwidthSnapshot,
    ) -> Option<Self> {
        let mut input = SourcePolicySnapshot::from_state(state, active_speakers, bandwidth);
        let mut tx = Self::default();
        audio::append_audio_route_activity(&mut tx, state, &input);
        let receiver_bwe_targets = mem::take(&mut input.receiver_bwe_targets);
        video::append_receiver_video_policy(&mut tx, state, &input, receiver_bwe_targets);
        tx.featured_users = input.featured_user_updates;
        (!tx.is_empty()).then_some(tx)
    }

    pub(super) fn push_state_update(&mut self, update: ConsumerPacketSelectionUpdate) {
        self.state_updates.push(update);
    }

    pub(super) fn push_route_update(
        &mut self,
        selection: ConsumerPacketSelectionUpdate,
        target: &ConsumerRouteTarget,
    ) {
        let finish = RouteFinish {
            selection,
            route: target.transport_route().clone(),
        };
        let control = finish.control();
        self.route_control.push_consumer(control, finish);
    }

    pub(super) fn set_receiver_bwe_targets(&mut self, targets: Vec<ReceiverBweTargetUpdate>) {
        self.route_control.set_receiver_bwe_targets(targets);
    }

    pub(in crate::engine::room) async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) {
        let Self {
            route_control,
            mut state_updates,
            featured_users,
        } = self;
        if !route_control.is_empty() {
            let outcome = media_transport.apply_route_control(route_control).await;
            for (update, result) in outcome.consumers {
                update.finish(result, &mut state_updates);
            }
        }
        commit_accepted_updates(room, &state_updates, &featured_users).await;
    }

    fn is_empty(&self) -> bool {
        self.state_updates.is_empty()
            && self.route_control.is_empty()
            && self.featured_users.is_empty()
    }
}

impl RouteFinish {
    fn control(&self) -> ConsumerRouteControl {
        let mut control = ConsumerRouteControl::new(self.route.clone())
            .request_keyframe(self.selection.request_keyframe);
        if self.selection.route_activity_changed {
            let active = self.selection.policy_pause_reason.is_none();
            control = control.activity(ConsumerActivity::from_active(active));
        }
        if let Some(packet_gate) = &self.selection.packet_gate {
            control = control.packet_gate(packet_gate.clone());
        }
        control
    }

    fn finish(
        self,
        result: ConsumerRouteControlOutcome,
        updates: &mut Vec<ConsumerPacketSelectionUpdate>,
    ) {
        if result.packet_gate_failed() || result.activity_failed() {
            warn!(
                route = ?self.route,
                route_active = self.selection.policy_pause_reason.is_none(),
                "media transport rejected the receiver-driven packet selection update"
            );
            return;
        }
        if result.keyframe_failed() {
            warn!(
                route = ?self.route,
                "media transport failed to request an adaptation keyframe refresh"
            );
        }
        updates.push(self.selection);
    }
}

async fn commit_accepted_updates(
    room: &Room,
    state_updates: &[ConsumerPacketSelectionUpdate],
    featured_users: &[FeaturedUserUpdate],
) {
    if state_updates.is_empty() && featured_users.is_empty() {
        return;
    }
    record_source_selection_metrics(room, state_updates);
    let info_fanout = {
        let mut state = room.state.write().await;
        commit_packet_updates(&mut state, state_updates);
        commit_featured_user_updates(&mut state, featured_users)
    };
    if let Some(info_fanout) = info_fanout {
        info_fanout.emit();
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

fn commit_packet_updates(state: &mut RoomState, updates: &[ConsumerPacketSelectionUpdate]) {
    for update in updates {
        state.topology.update_consumer_source_selection(
            &update.transport_ref,
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
        let Some(user) = state.user_mut_for_connection(&update.user_id, update.connection_id)
        else {
            continue;
        };
        if user.featured() == update.featured {
            continue;
        }
        user.set_featured(update.featured);
        changed_user_ids.push(update.user_id.clone());
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
