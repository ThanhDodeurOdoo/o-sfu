//! Async effect executor for room source policy.
//!
//! The pure source-policy planners return source-domain selector updates and
//! featured-state changes while the room lock is held. This executor owns
//! the post-lock part of the transaction: apply packet gates to the transport
//! adapter, request keyframes for accepted upswitches, and commit only the
//! updates that still match the live room route after those awaits.

use tracing::{debug, warn};

use super::{
    super::{Room, state::RoomState},
    ConsumerPacketSelectionUpdate, FeaturedUserUpdate, ReceiverBweTargetPlan,
    rank_active_speaker_sources,
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, ConsumerActivity, ConsumerPacketGateUpdate, MediaTransport,
        ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate, TransportConsumerRoute,
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
pub(in crate::engine::room) struct SourcePolicyEffectPlan {
    consumer_packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_bwe_targets: Vec<ReceiverBweTargetPlan>,
    featured_users: Vec<FeaturedUserUpdate>,
}

struct ResolvedConsumerPacketUpdate {
    selection: ConsumerPacketSelectionUpdate,
    transport_route: TransportConsumerRoute,
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
        let mut consumer_packet_updates =
            state.audio_route_activity_updates(&ranked_active_speaker_sources);
        let video_plan = state.receiver_video_policy_plan(
            &ranked_active_speaker_sources,
            receiver_bandwidth_snapshot,
        );
        consumer_packet_updates.extend(video_plan.consumer_packet_updates);
        let featured_users = state.featured_user_updates(&ranked_active_speaker_sources);
        Self {
            consumer_packet_updates,
            receiver_bwe_targets: video_plan.receiver_bwe_targets,
            featured_users,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.consumer_packet_updates.is_empty()
            && self.receiver_bwe_targets.is_empty()
            && self.featured_users.is_empty()
    }

    /// Applies transport-visible gates before committing selector state.
    ///
    /// The room write lock is acquired only after transport I/O finishes.
    /// Only updates accepted by the transport are committed back into
    /// `RoomState`; rejected or stale updates are left for the next policy
    /// refresh.
    pub async fn execute(self, room: &Room, media_port: &MediaTransport) {
        Self::apply_receiver_bwe_targets(room, media_port, &self.receiver_bwe_targets).await;
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

    async fn apply_receiver_bwe_targets(
        room: &Room,
        media_port: &MediaTransport,
        targets: &[ReceiverBweTargetPlan],
    ) {
        let updates = {
            let state = room.state.read().await;
            targets
                .iter()
                .map(|target| {
                    ReceiverBweTargetUpdate::new(
                        state.transport_user_key(target.user_id(), target.connection_id()),
                        target.target(),
                    )
                })
                .collect::<Vec<_>>()
        };
        media_port.set_receiver_bwe_targets(&updates).await;
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

    async fn apply_consumer_packet_updates(
        room: &Room,
        media_port: &MediaTransport,
        updates: Vec<ConsumerPacketSelectionUpdate>,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let updates = Self::prepare_packet_updates(room, updates).await;
        let packet_gate_updates = Self::packet_gate_updates(&updates);
        let mut packet_gate_results = media_port
            .set_consumer_packet_gates(&packet_gate_updates)
            .await
            .into_iter();
        let mut applied_updates = Vec::with_capacity(updates.len());
        for prepared in updates {
            if prepared.selection.packet_gate.is_some()
                && !matches!(packet_gate_results.next(), Some(Ok(())))
            {
                Self::warn_rejected_packet_update(&prepared.selection);
                continue;
            }
            if let Some(update) = Self::accepted_packet_update(media_port, prepared).await {
                applied_updates.push(update);
            }
        }
        applied_updates
    }

    async fn prepare_packet_updates(
        room: &Room,
        updates: Vec<ConsumerPacketSelectionUpdate>,
    ) -> Vec<ResolvedConsumerPacketUpdate> {
        let state = room.state.read().await;
        updates
            .into_iter()
            .map(|update| {
                Self::log_prepared_packet_update(&update);
                ResolvedConsumerPacketUpdate {
                    transport_route: state.transport_consumer_route(&update.route),
                    selection: update,
                }
            })
            .collect()
    }

    fn packet_gate_updates(
        updates: &[ResolvedConsumerPacketUpdate],
    ) -> Vec<ConsumerPacketGateUpdate> {
        let mut packet_gate_updates = Vec::with_capacity(updates.len());
        for prepared in updates {
            let Some(packet_gate) = prepared.selection.packet_gate.as_ref() else {
                continue;
            };
            packet_gate_updates.push(ConsumerPacketGateUpdate::new(
                prepared.transport_route.clone(),
                packet_gate.clone(),
            ));
        }
        packet_gate_updates
    }

    async fn accepted_packet_update(
        media_port: &MediaTransport,
        prepared: ResolvedConsumerPacketUpdate,
    ) -> Option<ConsumerPacketSelectionUpdate> {
        Self::log_accepted_packet_update(&prepared.selection);
        if !Self::apply_route_activity_update(media_port, &prepared).await {
            warn!(
                route = ?prepared.selection.route,
                route_active = prepared.selection.route_active(),
                "media transport failed to apply source policy route activity"
            );
            return None;
        }
        if !Self::request_adaptation_keyframe(media_port, &prepared).await {
            warn!(
                route = ?prepared.selection.route,
                "media transport failed to request an adaptation keyframe refresh"
            );
        }
        Some(prepared.selection)
    }

    async fn apply_route_activity_update(
        media_port: &MediaTransport,
        prepared: &ResolvedConsumerPacketUpdate,
    ) -> bool {
        if !prepared.selection.route_activity_update {
            return true;
        }
        media_port
            .set_consumer_active(
                &prepared.transport_route,
                ConsumerActivity::from_active(prepared.selection.route_active()),
            )
            .await
            .is_ok()
    }

    async fn request_adaptation_keyframe(
        media_port: &MediaTransport,
        prepared: &ResolvedConsumerPacketUpdate,
    ) -> bool {
        if !prepared.selection.request_keyframe {
            debug!(
                route = ?prepared.selection.route,
                "receiver-driven packet selection did not request a keyframe refresh"
            );
            return true;
        }
        debug!(route = ?prepared.selection.route, "requesting adaptation keyframe refresh");
        let accepted = media_port
            .request_consumer_keyframe(&prepared.transport_route)
            .await
            .is_ok();
        if accepted {
            debug!(
                route = ?prepared.selection.route,
                "media transport accepted adaptation keyframe refresh"
            );
        }
        accepted
    }

    fn log_prepared_packet_update(update: &ConsumerPacketSelectionUpdate) {
        debug!(
            route = ?update.route,
            selector = ?update.selector,
            policy_pause_reason = ?update.policy_pause_reason,
            packet_gate = ?update.packet_gate,
            request_keyframe = update.request_keyframe,
            "prepared receiver-driven packet selection update"
        );
    }

    fn log_accepted_packet_update(update: &ConsumerPacketSelectionUpdate) {
        debug!(
            route = ?update.route,
            selector = ?update.selector,
            policy_pause_reason = ?update.policy_pause_reason,
            packet_gate = ?update.packet_gate,
            request_keyframe = update.request_keyframe,
            "media transport accepted receiver-driven packet selection update"
        );
    }

    fn warn_rejected_packet_update(update: &ConsumerPacketSelectionUpdate) {
        warn!(
            route = ?update.route,
            "media transport rejected the receiver-driven packet selection update"
        );
    }
}
