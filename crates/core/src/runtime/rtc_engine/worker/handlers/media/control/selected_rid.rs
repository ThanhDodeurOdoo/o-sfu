//! selected-rid readiness and retry scheduling for route packet gates
//!
//! selected-rid gates are stricter than ordinary packet gates because the
//! receiver must not be switched to a simulcast layer until that layer is fresh
//! enough to decode
//! this module keeps the room-selected gate in `pending_packet_gate` while the
//! packet loop waits for recent rtp and a keyframe on that rid
//!
//! the effective gate is allowed to differ from the selected gate during
//! bootstrap:
//! - `Block` means the selected rid has not produced a fresh packet yet
//! - a fallback `Rid` means another live rid has produced a keyframe and can
//!   keep the receiver decodable while the selected rid is requested again
//! - the pending gate becomes effective only when the selected rid produces a
//!   keyframe
//!
//! all state here is worker-local transport state
//! room policy still owns which rid should be selected, while this file decides
//! when that selected rid is safe to enforce on the packet path

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

/// summary of route mutations caused by one source/rid readiness observation
///
/// this keeps the route scan separate from follow-up work
/// the scan mutates destination gates and records what kind of side effects the
/// caller must perform after the mutable route borrow ends
#[derive(Default)]
struct RidReadinessRouteUpdate {
    /// a strict selected rid became stale and was moved back to pending state
    suspended_stale_gate: bool,
    /// selected-gate transition observed while scanning destinations
    selected_gate: RidReadinessSelectedGateUpdate,
}

impl RidReadinessRouteUpdate {
    /// records that at least one destination is waiting on the incoming rid
    ///
    /// this must not overwrite a stronger transition because activation and
    /// fallback require different follow-up work
    fn mark_pending_selected_gate(&mut self) {
        if matches!(self.selected_gate, RidReadinessSelectedGateUpdate::None) {
            self.selected_gate = RidReadinessSelectedGateUpdate::Pending;
        }
    }

    /// records that a pending selected gate became the effective gate
    fn mark_activated_pending_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::Activated;
    }

    /// records that a fallback rid was opened while the selected rid stays pending
    fn mark_activated_bootstrap_fallback_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::BootstrapFallback;
    }

    /// reports whether the selected rid became strict for at least one destination
    fn activated_pending_gate(&self) -> bool {
        matches!(
            self.selected_gate,
            RidReadinessSelectedGateUpdate::Activated
        )
    }

    /// reports whether a temporary fallback rid became effective
    fn activated_bootstrap_fallback_gate(&self) -> bool {
        matches!(
            self.selected_gate,
            RidReadinessSelectedGateUpdate::BootstrapFallback
        )
    }

    /// reports whether the incoming rid is selected but still waiting for a keyframe
    fn has_pending_selected_gate(&self) -> bool {
        matches!(self.selected_gate, RidReadinessSelectedGateUpdate::Pending)
    }

    /// reports whether route-control aggregation must be refreshed
    ///
    /// keyframe requests alone do not count as gate changes
    fn changed_gate(&self) -> bool {
        self.activated_pending_gate()
            || self.activated_bootstrap_fallback_gate()
            || self.suspended_stale_gate
    }
}

/// selected-gate transition seen while processing one source/rid observation
///
/// the variants are ordered by how much follow-up work they imply
/// `Pending` can be promoted to either activation variant during the same scan
#[derive(Default)]
enum RidReadinessSelectedGateUpdate {
    /// no selected-rid destination was affected
    #[default]
    None,
    /// at least one destination wants the incoming rid but still needs a keyframe
    Pending,
    /// a selected rid produced a keyframe and became effective
    Activated,
    /// a different rid produced a keyframe and was opened as a temporary fallback
    BootstrapFallback,
}

/// maximum age for treating a producer rid as live enough for strict gating
///
/// browser encoders may stop sending a rid after adaptation
/// readiness is therefore freshness-based instead of a permanent once-seen bit
const SELECTED_RID_READY_MAX_AGE: Duration = Duration::from_secs(2);

/// bounded follow-up refresh schedule after a selected rid becomes effective
///
/// the first keyframe can still be followed by decoder loss or reordered packet
/// delivery, so a few delayed requests help receivers settle without keeping an
/// unbounded retry loop alive
const SELECTED_RID_KEYFRAME_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_millis(1_100),
    Duration::from_millis(2_500),
    Duration::from_secs(5),
    Duration::from_secs(8),
    Duration::from_secs(13),
];

/// updates packet-path readiness for one incoming producer rid
///
/// this test helper mirrors the packet-loop sequence by recording liveness
/// before applying readiness work
/// production packet-loop batches record liveness per packet and then call
/// [`apply_source_rid_readiness`] once per unique source/rid
///
/// returns `true` when an effective packet gate changed
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

/// applies source/rid readiness work after packet-level liveness was recorded
///
/// packet-loop batches can call this once per unique source/rid observed in a
/// turn
/// that keeps route scans proportional to unique readiness changes rather than
/// packet count while preserving per-packet liveness updates
///
/// the caller must pass the source session associated with the observed packet
/// remote-source keyframe requests revalidate that ownership before crossing
/// back to the producer worker
///
/// returns `true` when an effective gate changed and downstream planning should
/// treat the source as route-control dirty
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

/// drains selected-rid keyframe retries whose packet-loop deadlines have passed
///
/// retries live in `PacketLoopState` so they can fire even if the selected rid
/// does not keep sending packets
/// missing source ownership is expected after teardown and is handled as a
/// dropped best-effort refresh
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

/// splits a selected-rid gate into effective and pending transport state
///
/// the effective gate is what the packet loop enforces now
/// the pending gate is the target selected by room policy
/// keeping both lets bootstrap forwarding stay decodable without losing the
/// receiver's intended layer
///
/// non-rid gates pass through unchanged
/// rid gates become effective immediately only when the producer rid has recent
/// packet liveness
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

/// updates rid-gated routes with one scan over the source destinations
///
/// packet observation can activate a selected rid, open a temporary bootstrap
/// fallback or suspend a stale selected rid
/// keeping those decisions in one route pass makes the packet-loop cost
/// proportional to the source fanout once per observed rid packet instead of
/// once per sub-decision
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

/// applies readiness transitions to every destination of a source route
///
/// selected gates are activated only by a keyframe on their selected rid
/// a keyframe from another rid may become a bootstrap fallback, but only when no
/// selected gate was activated by this observation
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

/// moves a strict selected-rid gate back to pending when its rid goes stale
///
/// a selected rid is considered safe if it is the incoming rid or appears in
/// the ready-rid scratch set
/// otherwise the destination is blocked until the selected rid resumes and a
/// keyframe can activate it again
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
    let selected_packet_gate = destination.packet_gate;
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

/// opens the incoming rid as a temporary bootstrap fallback
///
/// fallback is used only for destinations that are blocked while waiting for a
/// different selected rid
/// this avoids black video during selected-rid bootstrap while preserving the
/// pending selected gate for the eventual strict switch
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

/// appends a rid to a small scratch vector when it is not already present
///
/// the vector is intentionally used instead of a set because the number of rids
/// per source is tiny and this runs on the packet-loop path
fn add_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

/// requests a keyframe for a live rid on either a local or remote source
///
/// local sources can be refreshed directly through their registered producer
/// remote sources are refreshed through the relay source control after the
/// observed ownership is checked against the current registration
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
                &registered_source_session_key,
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

/// schedules bounded follow-up keyframe refreshes after selected-rid activation
///
/// the retry schedule is tied to the source/rid pair rather than a destination
/// so multiple consumers selecting the same rid share the same refresh stream
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

/// drains due follow-up refreshes for the source/rid currently being observed
///
/// this catches retry deadlines naturally when the rid remains active and keeps
/// the packet-loop timer path as a fallback for quiet rids
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

/// resolves the current owner session used for timer-driven rid refreshes
///
/// a source may be local to this worker or represented as a remote consumer
/// registration
/// the returned session key is only a best-effort snapshot and request-time
/// ownership checks still apply before a remote keyframe request is sent
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
