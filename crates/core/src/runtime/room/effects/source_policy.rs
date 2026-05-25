//! Async effect executor for room-owned source policy.
//!
//! The pure source-policy planners return source-domain selector updates and
//! featured-state changes while the room lock is held. This executor owns
//! the post-lock part of the transaction: apply packet gates to the transport
//! adapter, request keyframes for accepted upswitches, and commit only the
//! updates that still match the live room route after those awaits.

use tracing::{debug, warn};

use super::super::{
    Room,
    state::{
        ConsumerPacketSelectionUpdate, FeaturedUserUpdate, RoomState, rank_active_speaker_sources,
    },
};
use crate::runtime::{
    media_transport::{
        ActiveSpeakerSource, ConsumerActivity, ConsumerPacketGateUpdate, MediaTransport,
        ReceiverBandwidthSnapshot,
    },
    metrics::{self, BudgetSolverOutcome},
};

#[cfg(test)]
mod test_support;

/// Executes one source-policy refresh after pure room planning.
///
/// Consumer packet updates touch the transport before room state records the
/// new selector. That keeps stale transport failures local to one update
/// instead of rolling back a room transition that may already have moved on.
/// Featured-user projection is part of the same plan so outbound layout
/// state and selector floors derive from the same active-speaker observation.
#[derive(Debug, Default)]
pub(in crate::runtime::room) struct SourcePolicyEffectPlan {
    consumer_packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    featured_users: Vec<FeaturedUserUpdate>,
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
        let ranked_active_speaker_sources = rank_active_speaker_sources(active_speaker_sources);
        let mut consumer_packets =
            state.audio_route_activity_updates(&ranked_active_speaker_sources);
        consumer_packets.extend(state.consumer_packet_selection_updates(
            &ranked_active_speaker_sources,
            receiver_bandwidth_snapshot,
        ));
        let featured_users = state.featured_user_updates(&ranked_active_speaker_sources);
        Self {
            consumer_packet_updates: consumer_packets,
            featured_users,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.consumer_packet_updates.is_empty() && self.featured_users.is_empty()
    }

    /// Applies transport-visible gates before committing selector state.
    ///
    /// The room write lock is acquired only after transport I/O finishes.
    /// Only updates accepted by the transport are committed back into
    /// `RoomState`; rejected or stale updates are left for the next policy
    /// refresh.
    pub async fn execute(self, room: &Room, media_port: &MediaTransport) {
        let applied_consumer_packet_updates =
            Self::apply_consumer_packet_updates(room, media_port, self.consumer_packet_updates)
                .await;
        if applied_consumer_packet_updates.is_empty() && self.featured_users.is_empty() {
            return;
        }
        Self::record_source_selection_metrics(room, &applied_consumer_packet_updates);
        let info_fanout = {
            let mut state = room.state.write().await;
            state.commit_consumer_packet_selection_updates(&applied_consumer_packet_updates);
            state.commit_featured_user_updates(&self.featured_users)
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
        let packet_gate_updates = Self::packet_gate_updates(room, &updates);
        let mut packet_gate_results = media_port
            .set_consumer_packet_gates(&packet_gate_updates)
            .await
            .into_iter();
        let mut applied_updates = Vec::with_capacity(updates.len());
        for update in updates {
            if update.packet_gate().is_some() && !matches!(packet_gate_results.next(), Some(Ok(())))
            {
                Self::warn_rejected_packet_update(&update);
                continue;
            }
            if let Some(update) = Self::accepted_packet_update(room, media_port, update).await {
                applied_updates.push(update);
            }
        }
        applied_updates
    }

    fn packet_gate_updates(
        room: &Room,
        updates: &[ConsumerPacketSelectionUpdate],
    ) -> Vec<ConsumerPacketGateUpdate> {
        let mut packet_gate_updates = Vec::with_capacity(updates.len());
        for update in updates {
            Self::log_prepared_packet_update(update);
            let Some(packet_gate) = update.packet_gate() else {
                continue;
            };
            let route = update.route();
            packet_gate_updates.push(ConsumerPacketGateUpdate::new(
                room.transport_consumer_route(route),
                packet_gate.clone(),
            ));
        }
        packet_gate_updates
    }

    async fn accepted_packet_update(
        room: &Room,
        media_port: &MediaTransport,
        update: ConsumerPacketSelectionUpdate,
    ) -> Option<ConsumerPacketSelectionUpdate> {
        Self::log_accepted_packet_update(&update);
        let route = update.route();
        if !Self::apply_route_activity_update(room, media_port, &update).await {
            warn!(
                ?route,
                route_active = update.route_active(),
                "media transport failed to apply source policy route activity"
            );
            return None;
        }
        if Self::request_adaptation_keyframe(room, media_port, &update).await {
            return Some(update);
        }
        warn!(
            ?route,
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
        let route = update.route();
        let transport_route = room.transport_consumer_route(route);
        media_port
            .set_consumer_active(
                &transport_route,
                ConsumerActivity::from_active(update.route_active()),
            )
            .await
            .is_ok()
    }

    async fn request_adaptation_keyframe(
        room: &Room,
        media_port: &MediaTransport,
        update: &ConsumerPacketSelectionUpdate,
    ) -> bool {
        let route = update.route();
        if !update.request_keyframe() {
            debug!(
                ?route,
                "receiver-driven packet selection did not request a keyframe refresh"
            );
            return true;
        }
        debug!(?route, "requesting adaptation keyframe refresh");
        let transport_route = room.transport_consumer_route(route);
        let accepted = media_port
            .request_consumer_keyframe(&transport_route)
            .await
            .is_ok();
        if accepted {
            debug!(
                ?route,
                "media transport accepted adaptation keyframe refresh"
            );
        }
        accepted
    }

    fn log_prepared_packet_update(update: &ConsumerPacketSelectionUpdate) {
        let route = update.route();
        debug!(
            ?route,
            selector = ?update.selector(),
            policy_pause_reason = ?update.policy_pause_reason(),
            packet_gate = ?update.packet_gate(),
            request_keyframe = update.request_keyframe(),
            "prepared receiver-driven packet selection update"
        );
    }

    fn log_accepted_packet_update(update: &ConsumerPacketSelectionUpdate) {
        let route = update.route();
        debug!(
            ?route,
            selector = ?update.selector(),
            policy_pause_reason = ?update.policy_pause_reason(),
            packet_gate = ?update.packet_gate(),
            request_keyframe = update.request_keyframe(),
            "media transport accepted receiver-driven packet selection update"
        );
    }

    fn warn_rejected_packet_update(update: &ConsumerPacketSelectionUpdate) {
        let route = update.route();
        warn!(
            ?route,
            "media transport rejected the receiver-driven packet selection update"
        );
    }
}
