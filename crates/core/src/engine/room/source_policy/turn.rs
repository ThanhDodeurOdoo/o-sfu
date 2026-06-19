//! source-policy apply ownership without transport awaits under the room lock

use super::{
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
    audio,
    input::SourcePolicyInput,
    video,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, MediaTransport, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
        TransportConsumerRoute,
    },
    metrics::{self, BudgetSolverOutcome},
    room::{
        Room, RoomEventMessage, effects::RoomRouteEffects, outbound::MessageFanout,
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

    pub async fn execute(self, room: &Room, media_transport: Option<&MediaTransport>) {
        if let Some(trigger) = self.trigger {
            apply(room, trigger, media_transport, None).await;
        }
    }

    fn push(&mut self, trigger: SourcePolicyTrigger) {
        self.trigger = Some(
            self.trigger
                .map_or(trigger, |current| current.merge(trigger)),
        );
    }
}

pub async fn apply(
    room: &Room,
    trigger: SourcePolicyTrigger,
    media_transport: Option<&MediaTransport>,
    active_speakers: Option<&[ActiveSpeakerSource]>,
) {
    if matches!(
        trigger,
        SourcePolicyTrigger::RouteGraph | SourcePolicyTrigger::FanoutPressure
    ) {
        room.observe_source_fanout_pressure().await;
    }
    let Some(media_transport) = media_transport else {
        return;
    };
    if trigger == SourcePolicyTrigger::FanoutPressure {
        return;
    }
    if let Some(sources) = active_speakers {
        run_packet_selection(room, sources, media_transport).await;
    } else {
        let sources = media_transport.active_speaker_source_snapshot().await;
        run_packet_selection(room, &sources, media_transport).await;
    }
}

async fn run_packet_selection(
    room: &Room,
    active_speakers: &[ActiveSpeakerSource],
    media_transport: &MediaTransport,
) {
    let sessions = {
        let state = room.state.read().await;
        state
            .transport_user_entries()
            .into_iter()
            .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let bandwidth = media_transport.receiver_bandwidth_snapshot(&sessions);
    let plan = {
        let state = room.state.read().await;
        SourcePolicyPlan::from_state(&state, active_speakers, &bandwidth)
    };
    if !plan.is_empty() {
        plan.execute(room, media_transport).await;
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

    pub async fn execute(self, room: &Room, media_transport: &MediaTransport) {
        let Self {
            state_only_packet_updates,
            transport_effect_packet_updates,
            receiver_bwe_targets,
            featured_users,
        } = self;
        let mut applied_packet_updates = state_only_packet_updates;
        let mut routes = RoomRouteEffects::default();
        routes.set_receiver_bwe_targets(receiver_bwe_targets);
        for (update, route) in transport_effect_packet_updates {
            routes.push_source_selection(update, route);
        }
        applied_packet_updates.extend(routes.execute(media_transport).await.packet_updates);
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
            let transport_route = state.topology.transport_consumer_route(&update.route);
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

fn commit_packet_updates(state: &mut RoomState, updates: &[ConsumerPacketSelectionUpdate]) {
    for update in updates {
        state.topology.update_consumer_source_selection(
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
