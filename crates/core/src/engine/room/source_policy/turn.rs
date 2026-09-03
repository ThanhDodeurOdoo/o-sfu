//! source-policy apply ownership without transport awaits under the room lock

use std::{borrow::Cow, mem};

use o_sfu_router::MediaKind;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::info;

use super::{
    action::{
        ConsumerPacketSelectionUpdate, FeaturedUserUpdate, ReceiverVideoBudgetPlan,
        VideoRouteTransition,
    },
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
    source_model::{
        PolicyPauseReason, ReceiverVideoBudgetDiagnostics, SourceAdaptationPolicy,
        SourceEncodingId, SourceSelector,
    },
};

/// Deferred request to recompute one room's source policy.
///
/// `RoomEffects` decides whether the turn runs before or after its transport
/// work. [`Self::execute`] serializes it with publication activity.
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
        self.execute_observed(room, media_transport, active_speaker_sources, None)
            .await;
    }

    async fn execute_observed(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
        active_speaker_sources: Option<&[ActiveSpeakerSource]>,
        bandwidth: Option<&ReceiverBandwidthSnapshot>,
    ) -> bool {
        if !self.requested {
            return false;
        }
        let Some(media_transport) = media_transport else {
            return false;
        };
        let transaction = if let Some(sources) = active_speaker_sources {
            run_packet_selection(room, sources, media_transport, bandwidth).await
        } else {
            let sources = media_transport.active_speaker_source_snapshot().await;
            run_packet_selection(room, &sources, media_transport, bandwidth).await
        };
        let Some(transaction) = transaction else {
            return false;
        };
        transaction.commit(room, media_transport).await;
        true
    }
}

#[cfg(feature = "internal-benchmarks")]
pub async fn run_source_policy_turn_for_benchmark(
    room: &Room,
    media_transport: &MediaTransport,
    bandwidth: &ReceiverBandwidthSnapshot,
) -> bool {
    let _guard = room.source_policy_turn.lock().await;
    SourcePolicyTurn::packet_selection()
        .execute_observed(room, Some(media_transport), None, Some(bandwidth))
        .await
}

async fn run_packet_selection(
    room: &Room,
    active_speakers: &[ActiveSpeakerSource],
    media_transport: &MediaTransport,
    bandwidth_override: Option<&ReceiverBandwidthSnapshot>,
) -> Option<SourcePolicyTransaction> {
    let sessions = {
        let state = room.state.read().await;
        state
            .transport_user_entries()
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let receiver_bandwidth = bandwidth_override.map_or_else(
        || Cow::Owned(media_transport.receiver_bandwidth_snapshot(&sessions)),
        Cow::Borrowed,
    );
    let source_bitrate = media_transport.transport_bitrate_snapshot(&sessions);
    let state = room.state.read().await;
    SourcePolicyTransaction::plan(
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
    receiver_video_budget_plans: Vec<ReceiverVideoBudgetPlan>,
    featured_users: Vec<FeaturedUserUpdate>,
}

impl SourcePolicyTransaction {
    pub(in crate::engine::room) fn plan(
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

    pub(super) fn push_receiver_video_budget_plan(&mut self, plan: ReceiverVideoBudgetPlan) {
        self.receiver_video_budget_plans.push(plan);
    }

    pub(super) fn set_receiver_bwe_targets(&mut self, targets: Vec<ReceiverBweTargetUpdate>) {
        self.route_effects.set_receiver_bwe_targets(targets);
    }

    async fn commit(self, room: &Room, media_transport: &MediaTransport) {
        let Self {
            route_effects,
            mut state_updates,
            receiver_video_budget_plans,
            featured_users,
        } = self;
        if !route_effects.is_empty() {
            // Only accepted transport controls join state-only updates. Room
            // state must not claim a selection that its worker rejected.
            state_updates.extend(route_effects.execute(room.uuid(), media_transport).await);
        }
        // Only committed nonzero counters schedule another observation. Rejected
        // or topology-stale work must not advance adaptation hysteresis.
        if commit_accepted_updates(
            room,
            state_updates,
            &receiver_video_budget_plans,
            &featured_users,
        )
        .await
        {
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
            && self.receiver_video_budget_plans.is_empty()
            && self.featured_users.is_empty()
    }
}

async fn commit_accepted_updates(
    room: &Room,
    state_updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_video_budget_plans: &[ReceiverVideoBudgetPlan],
    featured_users: &[FeaturedUserUpdate],
) -> bool {
    if state_updates.is_empty()
        && receiver_video_budget_plans.is_empty()
        && featured_users.is_empty()
    {
        return false;
    }
    let (committed_updates, info_fanout, requires_follow_up) = {
        let mut state = room.state.write().await;
        let (committed_updates, requires_follow_up) =
            commit_packet_updates(&mut state, state_updates, receiver_video_budget_plans);
        let info_fanout = commit_featured_user_updates(&mut state, featured_users);
        drop(state);
        (committed_updates, info_fanout, requires_follow_up)
    };
    record_committed_selection_updates(room, &committed_updates);
    if let Some(info_fanout) = info_fanout {
        info_fanout.emit();
    }
    requires_follow_up
}

fn record_committed_selection_updates(room: &Room, updates: &[ConsumerPacketSelectionUpdate]) {
    for update in updates {
        if update.packet_gate.is_some() {
            room.metrics
                .record_source_selection_update(metrics::source_selection_kind(update.selector));
        }
        let Some(transition) = update.transition else {
            continue;
        };
        let (metric_outcome, outcome, reason) = match transition {
            VideoRouteTransition::Degraded => (BudgetSolverOutcome::Degraded, "degraded", None),
            VideoRouteTransition::Paused { reason } => {
                (BudgetSolverOutcome::Paused, "paused", Some(reason))
            }
            VideoRouteTransition::Resumed { cleared_reason } => (
                BudgetSolverOutcome::Resumed,
                "resumed",
                Some(cleared_reason),
            ),
        };
        room.metrics.record_budget_solver_outcome(metric_outcome);
        let consumer = update.route.consumer_session_key();
        let source = update.route.source_session_key();
        info!(
            event = telemetry_event::SOURCE_POLICY_ROUTE_CHANGED,
            room_id = room.uuid(),
            user_id = %consumer.user_id().path_segment(),
            connection_id = consumer.connection_id().as_u64(),
            media_worker_id = consumer.media_worker_id().as_usize(),
            transport_media_id = update.route.consumer_transport_media_id().as_u64(),
            producer_user_id = %source.user_id().path_segment(),
            source_transport_media_id = update.route.source_transport_media_id().as_u64(),
            stream_id = %update.key.stream,
            outcome,
            reason = reason.map(policy_pause_reason_name),
            latest_receiver_bandwidth_estimate_bps = update
                .planned_budget
                .latest_receiver_bandwidth()
                .map(crate::Bitrate::as_bps),
            selected_video_budget_bps = update
                .planned_budget
                .selected_video_budget()
                .map(crate::Bitrate::as_bps),
            planned_active_video_route_count = update.planned_budget.active_video_route_count(),
            planned_selected_video_bitrate_bps = update
                .planned_budget
                .selected_video_bitrate()
                .as_bps(),
            selector = source_selector_name(update.selector),
            selected_encoding_id = update
                .selector
                .selected_encoding()
                .map(SourceEncodingId::as_u64),
            selected_estimated_bitrate_bps = update
                .selected_estimated_bitrate
                .map(crate::Bitrate::as_bps),
            "source policy route changed"
        );
    }
}

const fn source_selector_name(selector: SourceSelector) -> &'static str {
    match selector {
        SourceSelector::Open => "open",
        SourceSelector::Encoding(_) => "encoding",
    }
}

const fn policy_pause_reason_name(reason: PolicyPauseReason) -> &'static str {
    match reason {
        PolicyPauseReason::BudgetPressure => "budget_pressure",
        PolicyPauseReason::HiddenTile => "hidden_tile",
        PolicyPauseReason::OverflowTile => "overflow_tile",
        PolicyPauseReason::MissingUsableLayer => "missing_usable_layer",
        PolicyPauseReason::AudioSpeakerLimit => "audio_speaker_limit",
        PolicyPauseReason::ReceiverDeafened => "receiver_deafened",
        PolicyPauseReason::VideoDownloadLimit => "video_download_limit",
        PolicyPauseReason::SourceBitrateLimit => "source_bitrate_limit",
    }
}

fn commit_packet_updates(
    state: &mut RoomState,
    mut updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_video_budget_plans: &[ReceiverVideoBudgetPlan],
) -> (Vec<ConsumerPacketSelectionUpdate>, bool) {
    let mut requires_follow_up = false;
    updates.retain_mut(|update| {
        let committed = state.topology.update_consumer_source_selection(
            &update.key,
            update.source_id,
            &update.route,
            |selection| {
                selection.set_selector(update.selector);
                selection.set_policy_pause_reason(update.policy_pause_reason);
                selection.set_adaptation_observations(
                    update.pressure_observations,
                    update.upgrade_observations,
                );
            },
        );
        if committed
            && update.transition.is_some()
            && !route_transition_remains_observable(state, update)
        {
            update.transition = None;
        }
        requires_follow_up |= committed && update.requires_follow_up();
        committed
    });
    for plan in receiver_video_budget_plans {
        reconcile_receiver_video_budget(state, plan);
    }
    (updates, requires_follow_up)
}

fn route_transition_remains_observable(
    state: &RoomState,
    update: &ConsumerPacketSelectionUpdate,
) -> bool {
    state.user_connection_id(&update.key.receiver)
        == Some(update.route.consumer_session_key().connection_id())
        && state
            .topology
            .committed_consumer_route_for_key(&update.key)
            .is_some_and(|route| {
                route.source.descriptor.source_id() == update.source_id
                    && route.route == &update.route
                    && route.source.active
                    && route.selection.active()
            })
}

fn reconcile_receiver_video_budget(state: &mut RoomState, plan: &ReceiverVideoBudgetPlan) {
    let receiver = &plan.receiver;
    let receiver_connection_id = state.user_connection_id(receiver);
    // Async route controls may accept only part of a receiver plan. Rebuild the
    // shared diagnostics from captured route bitrates because ridless source
    // observations are unavailable after the transport await.
    let mut active_route_count = 0;
    let mut selected_video_bitrate = crate::Bitrate::zero();
    let mut update_targets = Vec::with_capacity(plan.routes.len());
    let mut plan_index = 0;
    for route in state
        .topology
        .committed_consumer_routes_for_user(receiver)
        .filter(|route| {
            receiver_connection_id == Some(route.route.consumer_session_key().connection_id())
        })
    {
        let policy = route.source.descriptor.policy();
        if route.source.descriptor.media_kind() != MediaKind::Video
            || (policy.adaptation() == SourceAdaptationPolicy::None
                && policy.video_bitrate_cap().is_none())
        {
            continue;
        }
        while plan
            .routes
            .get(plan_index)
            .is_some_and(|planned| &planned.key < route.key)
        {
            plan_index += 1;
        }
        let participating = route.source.active && route.selection.active();
        let Some(planned) = plan
            .routes
            .get(plan_index)
            .filter(|planned| &planned.key == route.key)
        else {
            if participating {
                return;
            }
            continue;
        };
        plan_index += 1;
        if planned.source_id != route.source.descriptor.source_id() || planned.route != *route.route
        {
            if participating {
                return;
            }
            continue;
        }
        let Some(planned_state) = planned.planned else {
            continue;
        };
        let selected_bitrate = if planned_state.matches_selection(route.selection) {
            planned_state.selected_bitrate
        } else if planned.captured.matches_selection(route.selection) {
            planned.captured.selected_bitrate
        } else {
            if participating {
                return;
            }
            continue;
        };
        update_targets.push(planned);
        if participating && route.selection.policy_pause_reason().is_none() {
            active_route_count += 1;
            selected_video_bitrate = selected_video_bitrate.saturating_add(selected_bitrate);
        }
    }
    let budget = ReceiverVideoBudgetDiagnostics::new(
        plan.planned_budget.latest_receiver_bandwidth(),
        plan.planned_budget.selected_video_budget(),
        active_route_count,
        selected_video_bitrate,
    );
    for target in update_targets {
        let updated = state.topology.update_consumer_source_selection(
            &target.key,
            target.source_id,
            &target.route,
            |selection| selection.set_budget(budget),
        );
        debug_assert!(
            updated,
            "validated route should remain committed under the lock"
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
