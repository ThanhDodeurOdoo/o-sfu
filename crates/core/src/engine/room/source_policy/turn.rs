//! source-policy apply ownership without transport awaits under the room lock

use super::{
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate, TransportPacketSelectionUpdate},
    audio,
    input::SourcePolicyInput,
    video,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, MediaTransport, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
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
    ) -> Option<SourcePolicyCommit> {
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
) -> Option<SourcePolicyCommit> {
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
) -> Option<SourcePolicyCommit> {
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
    SourcePolicyPlan::from_state(&state, active_speakers, &bandwidth).into_commit()
}

#[cfg(test)]
#[path = "TESTS/turn_support.rs"]
mod test_support;

#[derive(Debug)]
pub(in crate::engine::room) struct SourcePolicyPlan {
    pub(in crate::engine::room) state_packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    pub(in crate::engine::room) route_packet_updates: Vec<TransportPacketSelectionUpdate>,
    pub(in crate::engine::room) receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
    pub(in crate::engine::room) featured_users: Vec<FeaturedUserUpdate>,
}

#[derive(Debug)]
pub(in crate::engine::room) struct SourcePolicyCommit(pub(in crate::engine::room) SourcePolicyPlan);

impl SourcePolicyPlan {
    pub(in crate::engine::room) fn from_state(
        state: &RoomState,
        active_speakers: &[ActiveSpeakerSource],
        bandwidth: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let input = SourcePolicyInput::from_state(state, active_speakers, bandwidth);
        let mut packet_updates = audio::audio_route_activity_updates(&state.topology, &input);
        let video_plan = video::receiver_video_policy_plan(state, &input);
        packet_updates.extend(video_plan.transport_packet_updates);
        Self {
            state_packet_updates: video_plan.state_packet_updates,
            route_packet_updates: packet_updates,
            receiver_bwe_targets: video_plan.receiver_bwe_targets,
            featured_users: input.featured_user_updates,
        }
    }

    #[must_use]
    pub(in crate::engine::room) fn into_commit(self) -> Option<SourcePolicyCommit> {
        (!self.is_empty()).then_some(SourcePolicyCommit(self))
    }

    fn is_empty(&self) -> bool {
        self.state_packet_updates.is_empty()
            && self.route_packet_updates.is_empty()
            && self.receiver_bwe_targets.is_empty()
            && self.featured_users.is_empty()
    }
}

impl SourcePolicyCommit {
    pub(in crate::engine::room) async fn commit(self, room: &Room) {
        let Self(SourcePolicyPlan {
            state_packet_updates,
            featured_users,
            ..
        }) = self;
        if state_packet_updates.is_empty() && featured_users.is_empty() {
            return;
        }
        record_source_selection_metrics(room, &state_packet_updates);
        let info_fanout = {
            let mut state = room.state.write().await;
            commit_packet_updates(&mut state, &state_packet_updates);
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
