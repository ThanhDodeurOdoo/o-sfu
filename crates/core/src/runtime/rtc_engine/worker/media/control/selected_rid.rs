//! Selected-RID readiness and retry scheduling for route packet gates.

use std::{
    mem::take,
    time::{Duration, Instant},
};

use str0m::media::{KeyframeRequestKind, Rid};
use tracing::{debug, warn};

use super::{
    super::keyframe::request_keyframe_for_source,
    routes::{owned_local_producer_mid, packet_gate_rid, refresh_source_packet_gate},
};
use crate::runtime::{
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{RtcRouteControlMetrics, RtcRouteControlOutcome},
    rtc_engine::{
        demux::{MediaRouteDestination, MediaRouteEntry},
        media_registry::RegisteredMediaHandle,
        route_control::{KeyframeRequestDecision, PacketLayerGate},
        state::{PacketLoopState, RidReadinessScratch},
    },
};

#[derive(Default)]
struct RidReadinessRouteUpdate {
    suspended_stale_gate: bool,
    selected_gate: RidReadinessSelectedGateUpdate,
}

impl RidReadinessRouteUpdate {
    fn mark_pending_selected_gate(&mut self) {
        if matches!(self.selected_gate, RidReadinessSelectedGateUpdate::None) {
            self.selected_gate = RidReadinessSelectedGateUpdate::Pending;
        }
    }

    fn mark_activated_pending_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::Activated;
    }

    fn mark_activated_bootstrap_fallback_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::BootstrapFallback;
    }

    fn activated_pending_gate(&self) -> bool {
        matches!(
            self.selected_gate,
            RidReadinessSelectedGateUpdate::Activated
        )
    }

    fn activated_bootstrap_fallback_gate(&self) -> bool {
        matches!(
            self.selected_gate,
            RidReadinessSelectedGateUpdate::BootstrapFallback
        )
    }

    fn has_pending_selected_gate(&self) -> bool {
        matches!(self.selected_gate, RidReadinessSelectedGateUpdate::Pending)
    }

    fn changed_gate(&self) -> bool {
        self.activated_pending_gate()
            || self.activated_bootstrap_fallback_gate()
            || self.suspended_stale_gate
    }
}

#[derive(Default)]
enum RidReadinessSelectedGateUpdate {
    #[default]
    None,
    Pending,
    Activated,
    BootstrapFallback,
}

const SELECTED_RID_READY_MAX_AGE: Duration = Duration::from_secs(2);
const SELECTED_RID_KEYFRAME_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_millis(1_100),
    Duration::from_millis(2_500),
    Duration::from_secs(5),
    Duration::from_secs(8),
    Duration::from_secs(13),
];

/// Update packet-path readiness for one incoming producer RID.
///
/// This is the bridge between RTP observation and route control. It may move
/// pending consumer gates to their selected RID, install a single fallback RID
/// while the selected layer waits for a keyframe, or suspend a stale strict gate
/// that no longer has fresh packets. It only mutates transport state and queues
/// keyframe refreshes. Room policy remains the owner of which RID is selected.
#[cfg(test)]
pub fn observe_source_rid_readiness(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let first_observed = state.observe_producer_rid_packet(source_transport_media_id, rid, now);
    if first_observed {
        debug!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            is_keyframe,
            "observed first live RTP for producer RID"
        );
    }
    apply_source_rid_readiness(
        state,
        metrics,
        source_session_key,
        source_transport_media_id,
        rid,
        is_keyframe,
        now,
    )
}

/// Apply source/RID readiness work after packet-level liveness was recorded.
///
/// Packet-loop batches can call this once per unique source/RID observed in a
/// turn. That keeps route scans proportional to unique readiness changes rather
/// than packet count while preserving per-packet liveness updates.
pub(in crate::runtime::rtc_engine) fn apply_source_rid_readiness(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let mut scratch = take(&mut state.rid_readiness_scratch);
    let route_update = update_rid_readiness_routes(
        state,
        source_transport_media_id,
        rid,
        is_keyframe,
        now,
        &mut scratch,
    );
    for stale_rid in scratch.stale.iter().copied() {
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            stale_rid,
            now,
        );
    }
    if route_update.activated_pending_gate() {
        refresh_source_packet_gate(state, source_transport_media_id);
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
        schedule_live_rid_keyframe_retries(state, source_transport_media_id, rid, now);
    } else if route_update.activated_bootstrap_fallback_gate() {
        refresh_source_packet_gate(state, source_transport_media_id);
        for pending_rid in scratch.pending_selected.iter().copied() {
            request_live_rid_keyframe(
                state,
                metrics,
                source_session_key,
                source_transport_media_id,
                pending_rid,
                now,
            );
        }
    } else if route_update.has_pending_selected_gate() {
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    } else if route_update.suspended_stale_gate {
        refresh_source_packet_gate(state, source_transport_media_id);
    }
    drain_live_rid_keyframe_retries(
        state,
        metrics,
        source_session_key,
        source_transport_media_id,
        rid,
        now,
    );
    scratch.clear();
    state.rid_readiness_scratch = scratch;
    route_update.changed_gate()
}

/// Drain selected-RID keyframe retries whose packet-loop deadlines have passed.
///
/// Retries live in `PacketLoopState` so they can fire even if the selected
/// RID does not keep sending packets. Missing source ownership is expected
/// after teardown and is handled as a dropped best-effort refresh.
pub fn drain_due_rid_keyframe_refreshes(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    now: Instant,
) {
    for (source_transport_media_id, rid) in state.drain_due_rid_keyframe_refreshes_for_all(now) {
        let Some(source_session_key) =
            keyframe_refresh_source_session(state, source_transport_media_id)
        else {
            warn!(
                ?source_transport_media_id,
                ?rid,
                "dropped selected RID keyframe refresh because source ownership is unavailable"
            );
            continue;
        };
        debug!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "draining scheduled selected RID keyframe refresh"
        );
        request_live_rid_keyframe(
            state,
            metrics,
            &source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    }
}

/// Split a selected-RID gate into effective and pending transport state.
///
/// The effective gate is what the packet loop enforces now. The pending gate is
/// the target selected by room policy. Keeping both lets bootstrap forwarding
/// stay decodable without losing the receiver's intended layer.
pub(super) fn guarded_packet_gate(
    state: &PacketLoopState,
    source_transport_media_id: TransportMediaId,
    packet_gate: PacketLayerGate,
    now: Instant,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    let Some(rid) = packet_gate_rid(&packet_gate) else {
        return (packet_gate, None);
    };
    if state.producer_rid_is_ready(
        source_transport_media_id,
        rid,
        now,
        SELECTED_RID_READY_MAX_AGE,
    ) {
        return (packet_gate, None);
    }
    debug!(
        ?source_transport_media_id,
        ?rid,
        requested_packet_gate = ?packet_gate,
        "blocked selected RID route until selected producer RID has live RTP"
    );
    (PacketLayerGate::Block, Some(packet_gate))
}

/// Update RID-gated routes with one scan over the source destinations.
///
/// Packet observation can activate a selected RID, open a temporary bootstrap
/// fallback or suspend a stale selected RID. Keeping those decisions in one
/// route pass makes the packet-loop cost proportional to the source fanout once
/// per observed RID packet instead of once per sub-decision.
fn update_rid_readiness_routes(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    incoming_rid: Rid,
    is_keyframe: bool,
    now: Instant,
    scratch: &mut RidReadinessScratch,
) -> RidReadinessRouteUpdate {
    scratch.clear();
    state.collect_ready_producer_rids(
        source_transport_media_id,
        now,
        SELECTED_RID_READY_MAX_AGE,
        &mut scratch.ready,
    );
    state
        .media_route_index
        .get_mut(&source_transport_media_id)
        .map_or_else(RidReadinessRouteUpdate::default, |route_entry| {
            update_selected_rid_destinations(
                route_entry,
                source_transport_media_id,
                incoming_rid,
                is_keyframe,
                scratch,
            )
        })
}

fn update_selected_rid_destinations(
    route_entry: &mut MediaRouteEntry,
    source_transport_media_id: TransportMediaId,
    incoming_rid: Rid,
    is_keyframe: bool,
    scratch: &mut RidReadinessScratch,
) -> RidReadinessRouteUpdate {
    let mut update = RidReadinessRouteUpdate::default();
    for destination in &mut route_entry.destinations {
        suspend_stale_destination_gate(
            destination,
            source_transport_media_id,
            incoming_rid,
            &scratch.ready,
            &mut scratch.stale,
            &mut update,
        );
        let Some(selected_rid) = destination
            .pending_packet_gate
            .as_ref()
            .and_then(packet_gate_rid)
        else {
            continue;
        };
        add_unique_rid(&mut scratch.pending_selected, selected_rid);
        if selected_rid != incoming_rid {
            continue;
        }
        update.mark_pending_selected_gate();
        if is_keyframe && let Some(packet_gate) = destination.pending_packet_gate.take() {
            debug!(
                ?source_transport_media_id,
                consumer_session_key = ?destination.dest_session,
                consumer_transport_media_id = ?destination.dest_transport_media_id,
                ?incoming_rid,
                activated_packet_gate = ?packet_gate,
                "activated deferred strict RID packet gate after producer RID became live"
            );
            destination.packet_gate = packet_gate;
            update.mark_activated_pending_gate();
        }
    }
    if is_keyframe && !update.activated_pending_gate() {
        activate_bootstrap_fallback_destinations(
            route_entry,
            source_transport_media_id,
            incoming_rid,
            &mut update,
        );
    }
    update
}

fn suspend_stale_destination_gate(
    destination: &mut MediaRouteDestination,
    source_transport_media_id: TransportMediaId,
    incoming_rid: Rid,
    ready_rids: &[Rid],
    stale_rids: &mut Vec<Rid>,
    update: &mut RidReadinessRouteUpdate,
) {
    if destination.pending_packet_gate.is_some() {
        return;
    }
    let Some(selected_rid) = packet_gate_rid(&destination.packet_gate) else {
        return;
    };
    if selected_rid == incoming_rid || ready_rids.contains(&selected_rid) {
        return;
    }
    let selected_packet_gate = destination.packet_gate.clone();
    debug!(
        ?source_transport_media_id,
        consumer_session_key = ?destination.dest_session,
        consumer_transport_media_id = ?destination.dest_transport_media_id,
        ?incoming_rid,
        stale_rid = ?selected_rid,
        pending_packet_gate = ?selected_packet_gate,
        "blocked stale selected RID route until selected producer RID resumes"
    );
    destination.packet_gate = PacketLayerGate::Block;
    destination.pending_packet_gate = Some(selected_packet_gate);
    add_unique_rid(stale_rids, selected_rid);
    update.suspended_stale_gate = true;
}

fn activate_bootstrap_fallback_destinations(
    route_entry: &mut MediaRouteEntry,
    source_transport_media_id: TransportMediaId,
    incoming_rid: Rid,
    update: &mut RidReadinessRouteUpdate,
) {
    for destination in &mut route_entry.destinations {
        let Some(selected_rid) = destination
            .pending_packet_gate
            .as_ref()
            .and_then(packet_gate_rid)
        else {
            continue;
        };
        if selected_rid == incoming_rid
            || !matches!(destination.packet_gate, PacketLayerGate::Block)
        {
            continue;
        }
        debug!(
            ?source_transport_media_id,
            consumer_session_key = ?destination.dest_session,
            consumer_transport_media_id = ?destination.dest_transport_media_id,
            fallback_rid = ?incoming_rid,
            pending_selected_rid = ?selected_rid,
            "activated bootstrap fallback RID packet gate while selected producer RID is pending"
        );
        destination.packet_gate = PacketLayerGate::Rid(incoming_rid);
        update.mark_activated_bootstrap_fallback_gate();
    }
}

fn add_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn request_live_rid_keyframe(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    debug!(
        user_id = ?source_session_key.user_id(),
        media_worker_id = source_session_key.media_worker_id(),
        ?source_transport_media_id,
        ?rid,
        "requesting selected RID producer keyframe"
    );
    if owned_local_producer_mid(state, source_session_key, source_transport_media_id).is_some() {
        request_keyframe_for_source(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            Some(rid),
            KeyframeRequestKind::Pli,
            now,
        );
        return;
    }
    let Some((registered_source_session_key, source_control)) = state
        .remote_source_registration(source_transport_media_id)
        .map(|registration| {
            (
                registration.source_session_key().clone(),
                registration.source_control().clone(),
            )
        })
    else {
        warn!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "could not request selected RID keyframe because source ownership is unavailable"
        );
        return;
    };
    if registered_source_session_key != *source_session_key {
        warn!(
            observed_source_user_id = ?source_session_key.user_id(),
            observed_media_worker_id = source_session_key.media_worker_id(),
            registered_source_user_id = ?registered_source_session_key.user_id(),
            registered_media_worker_id = registered_source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "could not request selected RID keyframe because source ownership changed"
        );
        return;
    }
    match state.route_control.decide_keyframe_request_for_rid(
        source_transport_media_id,
        Some(rid),
        now,
    ) {
        KeyframeRequestDecision::Forward => {
            source_control.request_keyframe(
                registered_source_session_key,
                source_transport_media_id,
                Some(rid),
                KeyframeRequestKind::Pli,
            );
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
        }
        KeyframeRequestDecision::Absorb => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        }
    }
}

fn schedule_live_rid_keyframe_retries(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    for delay in SELECTED_RID_KEYFRAME_RETRY_DELAYS {
        state.schedule_rid_keyframe_refresh(source_transport_media_id, rid, now + delay);
    }
    debug!(
        ?source_transport_media_id,
        ?rid,
        retry_count = SELECTED_RID_KEYFRAME_RETRY_DELAYS.len(),
        "scheduled follow-up selected RID keyframe refreshes"
    );
}

fn drain_live_rid_keyframe_retries(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    let due_count = state.drain_due_rid_keyframe_refreshes(source_transport_media_id, rid, now);
    for _ in 0..due_count {
        debug!(
            user_id = ?source_session_key.user_id(),
            media_worker_id = source_session_key.media_worker_id(),
            ?source_transport_media_id,
            ?rid,
            "draining follow-up selected RID keyframe refresh"
        );
        request_live_rid_keyframe(
            state,
            metrics,
            source_session_key,
            source_transport_media_id,
            rid,
            now,
        );
    }
}

fn keyframe_refresh_source_session(
    state: &PacketLoopState,
    source_transport_media_id: TransportMediaId,
) -> Option<TransportSessionKey> {
    match state.media_handle(source_transport_media_id) {
        Some(RegisteredMediaHandle::Producer { session_key, .. }) => Some(session_key.clone()),
        Some(RegisteredMediaHandle::Consumer { .. }) | None => state
            .remote_source_registration(source_transport_media_id)
            .map(|registration| registration.source_session_key().clone()),
    }
}
