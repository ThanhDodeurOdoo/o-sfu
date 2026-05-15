//! Async effect executor for room-owned video source policy.
//!
//! The pure video-policy planner returns source-domain selector updates and
//! featured-state changes while the room lock is held. This executor owns
//! the post-lock part of the transaction: apply packet gates to the transport
//! adapter, request keyframes for accepted upswitches, and commit only the
//! updates that still match the live room route after those awaits.

use std::collections::BTreeSet;

use tracing::{debug, warn};

use super::super::{
    Room,
    state::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate, RoomState},
};
use crate::runtime::{
    media_transport::{
        ActiveSpeakerSource, ConsumerActivity, ConsumerPacketGateUpdate, MediaTransport,
        ReceiverBandwidthSnapshot,
    },
    metrics::{self, BudgetSolverOutcome},
};

/// Executes one source-policy refresh after pure room planning.
///
/// Consumer packet updates touch the transport before room state records the
/// new selector. That keeps stale transport failures local to one update
/// instead of rolling back a room transition that may already have moved on.
/// Featured-user projection is part of the same plan so outbound layout
/// state and selector floors derive from the same active-speaker observation.
#[derive(Debug, Default)]
pub(in crate::runtime::room) struct SourcePolicyEffectPlan {
    consumer_packets: Vec<ConsumerPacketSelectionUpdate>,
    featured_sessions: Vec<FeaturedUserUpdate>,
}

impl SourcePolicyEffectPlan {
    /// Builds a cold-path effect plan from one transport observation snapshot.
    ///
    /// The caller has already released any long-lived transport work. This
    /// constructor only reads room state and does not mutate transport state.
    pub fn from_state(
        state: &RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let consumer_packets = state
            .consumer_packet_selection_updates(active_speaker_sources, receiver_bandwidth_snapshot);
        let featured_sessions = state.featured_session_updates(active_speaker_sources);
        Self {
            consumer_packets,
            featured_sessions,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.consumer_packets.is_empty() && self.featured_sessions.is_empty()
    }

    /// Applies transport-visible gates before committing selector state.
    ///
    /// The room write lock is acquired only after transport I/O finishes.
    /// Only updates accepted by the transport are committed back into
    /// `RoomState`; rejected or stale updates are left for the next policy
    /// refresh.
    pub async fn execute(self, room: &Room, media_port: &MediaTransport) {
        let applied_consumer_packet_updates =
            Self::apply_consumer_packet_updates(room, media_port, self.consumer_packets).await;
        if applied_consumer_packet_updates.is_empty() && self.featured_sessions.is_empty() {
            return;
        }
        Self::record_source_selection_metrics(room, &applied_consumer_packet_updates);
        let info_fanout = {
            let mut state = room.state.write().await;
            state.commit_consumer_packet_selection_updates(&applied_consumer_packet_updates);
            state.commit_featured_user_updates(&self.featured_sessions)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }

    fn record_source_selection_metrics(room: &Room, updates: &[ConsumerPacketSelectionUpdate]) {
        for update in updates {
            if update.packet_gate().is_some() {
                room.metrics
                    .record_source_selection_update(metrics::source_selection_kind(
                        update.selector(),
                    ));
            }
            let outcomes = update.outcomes();
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

    async fn apply_consumer_packet_updates(
        room: &Room,
        media_port: &MediaTransport,
        updates: Vec<ConsumerPacketSelectionUpdate>,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let packet_gate_plan = Self::packet_gate_update_plan(room, &updates);
        let rejected_packet_gate_update_offsets =
            Self::rejected_packet_gate_updates(media_port, packet_gate_plan.updates()).await;
        let rejected_packet_gate_updates =
            packet_gate_plan.rejected_update_indexes(rejected_packet_gate_update_offsets);
        let mut applied_updates = Vec::with_capacity(updates.len());
        for (index, update) in updates.into_iter().enumerate() {
            if rejected_packet_gate_updates.contains(&index) {
                Self::warn_rejected_packet_update(&update);
                continue;
            }
            if let Some(update) = Self::accepted_packet_update(room, media_port, update).await {
                applied_updates.push(update);
            }
        }
        applied_updates
    }

    fn packet_gate_update_plan(
        room: &Room,
        updates: &[ConsumerPacketSelectionUpdate],
    ) -> PacketGateUpdatePlan {
        let mut plan = PacketGateUpdatePlan::with_capacity(updates.len());
        for (index, update) in updates.iter().enumerate() {
            Self::log_prepared_packet_update(update);
            let Some(packet_gate) = update.packet_gate() else {
                continue;
            };
            plan.push(
                index,
                ConsumerPacketGateUpdate::new(
                    room.transport_user_key(
                        update.consumer_user_id(),
                        update.consumer_connection_id(),
                    ),
                    update.consumer_transport_media_id(),
                    room.transport_user_key(update.source_user_id(), update.source_connection_id()),
                    update.source_transport_media_id(),
                    packet_gate.clone(),
                ),
            );
        }
        plan
    }

    async fn accepted_packet_update(
        room: &Room,
        media_port: &MediaTransport,
        update: ConsumerPacketSelectionUpdate,
    ) -> Option<ConsumerPacketSelectionUpdate> {
        Self::log_accepted_packet_update(&update);
        if !Self::apply_route_activity_update(room, media_port, &update).await {
            warn!(
                consumer_user_id = ?update.consumer_user_id(),
                source_user_id = ?update.source_user_id(),
                source_transport_media_id = ?update.source_transport_media_id(),
                consumer_transport_media_id = ?update.consumer_transport_media_id(),
                route_active = update.route_active(),
                "media transport failed to apply receiver video policy route activity"
            );
            return None;
        }
        if Self::request_adaptation_keyframe(room, media_port, &update).await {
            return Some(update);
        }
        warn!(
            consumer_user_id = ?update.consumer_user_id(),
            source_user_id = ?update.source_user_id(),
            source_transport_media_id = ?update.source_transport_media_id(),
            consumer_transport_media_id = ?update.consumer_transport_media_id(),
            "media transport failed to request an adaptation keyframe refresh"
        );
        Some(update)
    }

    async fn apply_route_activity_update(
        room: &Room,
        media_port: &MediaTransport,
        update: &ConsumerPacketSelectionUpdate,
    ) -> bool {
        if !update.route_activity_update() {
            return true;
        }
        media_port
            .set_consumer_active(
                &room
                    .transport_user_key(update.consumer_user_id(), update.consumer_connection_id()),
                update.consumer_transport_media_id(),
                &room.transport_user_key(update.source_user_id(), update.source_connection_id()),
                update.source_transport_media_id(),
                ConsumerActivity::from_active(update.route_active()),
            )
            .await
            .is_ok()
    }

    async fn rejected_packet_gate_updates(
        media_port: &MediaTransport,
        packet_gate_updates: &[ConsumerPacketGateUpdate],
    ) -> BTreeSet<usize> {
        let mut rejected_updates = BTreeSet::new();
        let results = media_port
            .set_consumer_packet_gates(packet_gate_updates)
            .await;
        for index in 0..packet_gate_updates.len() {
            if !matches!(results.get(index), Some(Ok(()))) {
                rejected_updates.insert(index);
            }
        }
        rejected_updates
    }

    async fn request_adaptation_keyframe(
        room: &Room,
        media_port: &MediaTransport,
        update: &ConsumerPacketSelectionUpdate,
    ) -> bool {
        if !update.request_keyframe() {
            debug!(
                consumer_user_id = ?update.consumer_user_id(),
                source_user_id = ?update.source_user_id(),
                source_transport_media_id = ?update.source_transport_media_id(),
                consumer_transport_media_id = ?update.consumer_transport_media_id(),
                "receiver-driven packet selection did not request a keyframe refresh"
            );
            return true;
        }
        debug!(
            consumer_user_id = ?update.consumer_user_id(),
            source_user_id = ?update.source_user_id(),
            source_transport_media_id = ?update.source_transport_media_id(),
            consumer_transport_media_id = ?update.consumer_transport_media_id(),
            "requesting adaptation keyframe refresh"
        );
        let accepted = media_port
            .request_consumer_keyframe(
                &room
                    .transport_user_key(update.consumer_user_id(), update.consumer_connection_id()),
                update.consumer_transport_media_id(),
                &room.transport_user_key(update.source_user_id(), update.source_connection_id()),
                update.source_transport_media_id(),
            )
            .await
            .is_ok();
        if accepted {
            debug!(
                consumer_user_id = ?update.consumer_user_id(),
                source_user_id = ?update.source_user_id(),
                source_transport_media_id = ?update.source_transport_media_id(),
                consumer_transport_media_id = ?update.consumer_transport_media_id(),
                "media transport accepted adaptation keyframe refresh"
            );
        }
        accepted
    }

    fn log_prepared_packet_update(update: &ConsumerPacketSelectionUpdate) {
        debug!(
            consumer_user_id = ?update.consumer_user_id(),
            source_user_id = ?update.source_user_id(),
            source_transport_media_id = ?update.source_transport_media_id(),
            consumer_transport_media_id = ?update.consumer_transport_media_id(),
            selector = ?update.selector(),
            policy_pause_reason = ?update.policy_pause_reason(),
            packet_gate = ?update.packet_gate(),
            request_keyframe = update.request_keyframe(),
            "prepared receiver-driven packet selection update"
        );
    }

    fn log_accepted_packet_update(update: &ConsumerPacketSelectionUpdate) {
        debug!(
            consumer_user_id = ?update.consumer_user_id(),
            source_user_id = ?update.source_user_id(),
            source_transport_media_id = ?update.source_transport_media_id(),
            consumer_transport_media_id = ?update.consumer_transport_media_id(),
            selector = ?update.selector(),
            policy_pause_reason = ?update.policy_pause_reason(),
            packet_gate = ?update.packet_gate(),
            request_keyframe = update.request_keyframe(),
            "media transport accepted receiver-driven packet selection update"
        );
    }

    fn warn_rejected_packet_update(update: &ConsumerPacketSelectionUpdate) {
        warn!(
            consumer_user_id = ?update.consumer_user_id(),
            source_user_id = ?update.source_user_id(),
            source_transport_media_id = ?update.source_transport_media_id(),
            consumer_transport_media_id = ?update.consumer_transport_media_id(),
            "media transport rejected the receiver-driven packet selection update"
        );
    }
}

struct PacketGateUpdatePlan {
    updates: Vec<ConsumerPacketGateUpdate>,
    update_indexes: Vec<usize>,
}

impl PacketGateUpdatePlan {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            updates: Vec::with_capacity(capacity),
            update_indexes: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, update_index: usize, update: ConsumerPacketGateUpdate) {
        self.update_indexes.push(update_index);
        self.updates.push(update);
    }

    fn updates(&self) -> &[ConsumerPacketGateUpdate] {
        &self.updates
    }

    fn rejected_update_indexes(&self, rejected_offsets: BTreeSet<usize>) -> BTreeSet<usize> {
        rejected_offsets
            .into_iter()
            .filter_map(|offset| self.update_indexes.get(offset).copied())
            .collect()
    }
}
