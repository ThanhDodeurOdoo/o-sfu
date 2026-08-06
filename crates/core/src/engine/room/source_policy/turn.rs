//! source-policy apply ownership without transport awaits under the room lock

use std::mem;

use super::{
    action::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
    audio,
    input::SourcePolicySnapshot,
    video,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, MediaTransport, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
        TransportBitrateSnapshot,
    },
    metrics::{self, BudgetSolverOutcome},
    room::{
        Room, RoomEventMessage, effects::transport::RoomRouteEffects, outbound::MessageFanout,
        state::RoomState,
    },
};

/// deferred source-policy turn executed after route effects and pre-policy output
#[derive(Debug, Default)]
pub struct SourcePolicyTurn {
    requested: bool,
}

impl SourcePolicyTurn {
    pub const fn packet_selection() -> Self {
        Self { requested: true }
    }

    pub fn request(&mut self) {
        self.requested = true;
    }

    pub async fn execute(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
        active_speaker_sources: Option<&[ActiveSpeakerSource]>,
    ) {
        if !self.requested {
            return;
        }
        let _guard = room.source_policy_turn.lock().await;
        self.execute_guarded(room, media_transport, active_speaker_sources)
            .await;
    }

    pub(in crate::engine::room) async fn execute_guarded(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
        active_speaker_sources: Option<&[ActiveSpeakerSource]>,
    ) {
        if !self.requested {
            return;
        }
        let Some(media_transport) = media_transport else {
            return;
        };
        let transaction = if let Some(sources) = active_speaker_sources {
            run_packet_selection(room, sources, media_transport).await
        } else {
            let sources = media_transport.active_speaker_source_snapshot().await;
            run_packet_selection(room, &sources, media_transport).await
        };
        if let Some(transaction) = transaction {
            transaction.commit(room, media_transport).await;
        }
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
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let receiver_bandwidth = media_transport.receiver_bandwidth_snapshot(&sessions);
    let source_bitrate = media_transport.transport_bitrate_snapshot(&sessions);
    let state = room.state.read().await;
    SourcePolicyTransaction::plan_from_state_with_source_bitrate(
        &state,
        active_speakers,
        &receiver_bandwidth,
        &source_bitrate,
    )
}

#[derive(Debug, Default)]
pub(in crate::engine::room) struct SourcePolicyTransaction {
    route_effects: RoomRouteEffects,
    state_updates: Vec<ConsumerPacketSelectionUpdate>,
    featured_users: Vec<FeaturedUserUpdate>,
}

impl SourcePolicyTransaction {
    #[cfg(test)]
    pub(in crate::engine::room) fn plan_from_state(
        state: &RoomState,
        active_speakers: &[ActiveSpeakerSource],
        receiver_bandwidth: &ReceiverBandwidthSnapshot,
    ) -> Option<Self> {
        Self::plan_from_state_with_source_bitrate(
            state,
            active_speakers,
            receiver_bandwidth,
            &TransportBitrateSnapshot::default(),
        )
    }

    pub(in crate::engine::room) fn plan_from_state_with_source_bitrate(
        state: &RoomState,
        active_speakers: &[ActiveSpeakerSource],
        receiver_bandwidth: &ReceiverBandwidthSnapshot,
        source_bitrate: &TransportBitrateSnapshot,
    ) -> Option<Self> {
        let mut input = SourcePolicySnapshot::from_state(
            state,
            active_speakers,
            receiver_bandwidth,
            source_bitrate,
        );
        let mut tx = Self::default();
        audio::append_audio_route_activity(&mut tx, &input);
        let receiver_bwe_targets = mem::take(&mut input.receiver_bwe_targets);
        video::append_receiver_video_policy(&mut tx, state, &input, receiver_bwe_targets);
        tx.featured_users = input.featured_user_updates;
        (!tx.is_empty()).then_some(tx)
    }

    pub(super) fn push_state_update(&mut self, update: ConsumerPacketSelectionUpdate) {
        self.state_updates.push(update);
    }

    pub(super) fn push_route_update(&mut self, update: ConsumerPacketSelectionUpdate) {
        self.route_effects.source_policy_update(update);
    }

    pub(super) fn set_receiver_bwe_targets(&mut self, targets: Vec<ReceiverBweTargetUpdate>) {
        self.route_effects.set_receiver_bwe_targets(targets);
    }

    async fn commit(self, room: &Room, media_transport: &MediaTransport) {
        let mut state_updates = self.state_updates;
        if !self.route_effects.is_empty() {
            state_updates.extend(
                self.route_effects
                    .execute(room.uuid(), media_transport)
                    .await,
            );
        }
        if commit_accepted_updates(room, &state_updates, &self.featured_users).await {
            media_transport.schedule_source_policy_follow_up(room.instance_id());
        }
    }

    #[cfg(test)]
    pub(in crate::engine::room) async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) {
        self.commit(room, media_transport).await;
    }

    fn is_empty(&self) -> bool {
        self.state_updates.is_empty()
            && self.route_effects.is_empty()
            && self.featured_users.is_empty()
    }
}

async fn commit_accepted_updates(
    room: &Room,
    state_updates: &[ConsumerPacketSelectionUpdate],
    featured_users: &[FeaturedUserUpdate],
) -> bool {
    if state_updates.is_empty() && featured_users.is_empty() {
        return false;
    }
    record_source_selection_metrics(room, state_updates);
    let (info_fanout, requires_follow_up) = {
        let mut state = room.state.write().await;
        let requires_follow_up = commit_packet_updates(&mut state, state_updates);
        let info_fanout = commit_featured_user_updates(&mut state, featured_users);
        drop(state);
        (info_fanout, requires_follow_up)
    };
    if let Some(info_fanout) = info_fanout {
        info_fanout.emit();
    }
    requires_follow_up
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
    }
}

fn commit_packet_updates(state: &mut RoomState, updates: &[ConsumerPacketSelectionUpdate]) -> bool {
    let mut requires_follow_up = false;
    for update in updates {
        let committed = state.topology.update_consumer_source_selection(
            &update.key,
            update.source_id,
            &update.route,
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
        requires_follow_up |= committed && update.requires_follow_up();
    }
    requires_follow_up
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
