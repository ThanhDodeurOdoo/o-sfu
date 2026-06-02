//! selected-rid readiness and retry scheduling for route packet gates
//!
//! selected-rid gates are stricter than ordinary packet gates because the
//! receiver must not be switched to a simulcast layer until that layer is fresh
//! enough to decode
//! this module keeps the room-selected gate in `pending_gate` while the
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
    super::keyframe::{KeyframeRequestMode, KeyframeRequestTarget, request_kf_for_target},
    routes::ensure_local_producer_mid,
};
use crate::engine::{
    media_transport::{
        TransportMediaId, TransportSessionKey,
        rtc::{
            media_registry::RegisteredMediaHandle,
            route_control::PacketLayerGate,
            route_table::{RidReadinessRouteUpdate, RidReadinessSelectedGateUpdate},
            state::{PacketLoopState, RidReadinessScratch},
        },
    },
    metrics::RtcRouteControlMetrics,
};

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
/// [`apply_src_rid_ready`] once per unique source/rid
///
/// returns `true` when an effective packet gate changed
#[cfg(test)]
pub fn observe_src_rid_ready(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let first_observed = state
        .routes
        .observe_producer_packet(src_media, Some(rid), false, now);
    if first_observed {
        debug!(
            user_id = ?src_key.user_id(),
            media_worker_id = src_key.media_worker_id().as_usize(),
            source_transport_media_id = ?src_media,
            ?rid,
            is_keyframe,
            "observed first live RTP for producer RID"
        );
    }
    apply_src_rid_ready(state, metrics, src_key, src_media, rid, is_keyframe, now)
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
pub(in crate::engine::media_transport::rtc) fn apply_src_rid_ready(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    is_keyframe: bool,
    now: Instant,
) -> bool {
    let mut scratch = take(&mut state.rid_readiness_scratch);
    let route_update =
        update_rid_readiness_routes(state, src_media, rid, is_keyframe, now, &mut scratch);
    for stale_rid in scratch.stale.iter().copied() {
        request_live_rid_kf(
            state,
            metrics,
            src_key,
            src_media,
            stale_rid,
            KeyframeRequestMode::Track(now),
        );
    }
    match route_update.selected_gate {
        RidReadinessSelectedGateUpdate::Activated => {
            request_live_rid_kf(
                state,
                metrics,
                src_key,
                src_media,
                rid,
                KeyframeRequestMode::Track(now),
            );
            schedule_live_rid_kf_retries(state, src_media, rid, now);
        }
        RidReadinessSelectedGateUpdate::BootstrapFallback => {
            for pending_rid in scratch.pending_selected.iter().copied() {
                request_live_rid_kf(
                    state,
                    metrics,
                    src_key,
                    src_media,
                    pending_rid,
                    KeyframeRequestMode::Track(now),
                );
            }
        }
        RidReadinessSelectedGateUpdate::Pending => request_live_rid_kf(
            state,
            metrics,
            src_key,
            src_media,
            rid,
            KeyframeRequestMode::Track(now),
        ),
        RidReadinessSelectedGateUpdate::None => {}
    }
    drain_live_rid_kf_retries(state, metrics, src_key, src_media, rid, now);
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
pub fn drain_due_rid_kf_refreshes(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    now: Instant,
) {
    for (src_media, rid) in state.routes.drain_all_due_rid_refreshes(now) {
        let Some(src_key) = kf_refresh_src_key(state, src_media) else {
            warn!(
                source_transport_media_id = ?src_media,
                ?rid,
                "dropped selected RID keyframe refresh because source ownership is unavailable"
            );
            continue;
        };
        debug!(
            user_id = ?src_key.user_id(),
            media_worker_id = src_key.media_worker_id().as_usize(),
            source_transport_media_id = ?src_media,
            ?rid,
            "draining scheduled selected RID keyframe refresh"
        );
        request_live_rid_kf(
            state,
            metrics,
            &src_key,
            src_media,
            rid,
            KeyframeRequestMode::Retry,
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
pub(super) fn guarded_pkt_gate(
    state: &PacketLoopState,
    src_media: TransportMediaId,
    packet_gate: PacketLayerGate,
    now: Instant,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    let Some(rid) = packet_gate.selected_rid() else {
        return (packet_gate, None);
    };
    if state
        .routes
        .producer_rid_is_ready(src_media, rid, now, SELECTED_RID_READY_MAX_AGE)
    {
        return (packet_gate, None);
    }
    debug!(
        source_transport_media_id = ?src_media,
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
    src_media: TransportMediaId,
    incoming_rid: Rid,
    is_keyframe: bool,
    now: Instant,
    scratch: &mut RidReadinessScratch,
) -> RidReadinessRouteUpdate {
    scratch.clear();
    state.routes.collect_ready_producer_rids(
        src_media,
        now,
        SELECTED_RID_READY_MAX_AGE,
        &mut scratch.ready,
    );
    state.routes.update_rid_readiness(
        src_media,
        incoming_rid,
        is_keyframe,
        &scratch.ready,
        &mut scratch.stale,
        &mut scratch.pending_selected,
    )
}

/// requests a keyframe for a live rid on either a local or remote source
///
/// local sources can be refreshed directly through their registered producer
/// remote sources are refreshed through the relay source control after the
/// observed ownership is checked against the current registration
fn request_live_rid_kf(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    mode: KeyframeRequestMode,
) {
    debug!(
        user_id = ?src_key.user_id(),
        media_worker_id = src_key.media_worker_id().as_usize(),
        source_transport_media_id = ?src_media,
        ?rid,
        "requesting selected RID producer keyframe"
    );
    if ensure_local_producer_mid(state, src_key, src_media).is_ok() {
        request_kf_for_target(
            state,
            metrics,
            KeyframeRequestTarget::Local(src_key, src_media),
            Some(rid),
            KeyframeRequestKind::Pli,
            mode,
        );
        return;
    }
    let Some((registered_src, src_control)) =
        state.routes.remote_source(src_media).map(|registration| {
            (
                registration.source().clone(),
                registration.source_control().clone(),
            )
        })
    else {
        warn!(
            user_id = ?src_key.user_id(),
            media_worker_id = src_key.media_worker_id().as_usize(),
            source_transport_media_id = ?src_media,
            ?rid,
            "could not request selected RID keyframe because source ownership is unavailable"
        );
        return;
    };
    if registered_src.session_key() != src_key {
        warn!(
            observed_source_user_id = ?src_key.user_id(),
            observed_media_worker_id = src_key.media_worker_id().as_usize(),
            registered_source_user_id = ?registered_src.session_key().user_id(),
            registered_media_worker_id = registered_src.session_key().media_worker_id().as_usize(),
            source_transport_media_id = ?src_media,
            ?rid,
            "could not request selected RID keyframe because source ownership changed"
        );
        return;
    }
    request_kf_for_target(
        state,
        metrics,
        KeyframeRequestTarget::Remote(&registered_src, &src_control),
        Some(rid),
        KeyframeRequestKind::Pli,
        mode,
    );
}

/// schedules bounded follow-up keyframe refreshes after selected-rid activation
///
/// the retry schedule is tied to the source/rid pair rather than a destination
/// so multiple consumers selecting the same rid share the same refresh stream
fn schedule_live_rid_kf_retries(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    for delay in SELECTED_RID_KEYFRAME_RETRY_DELAYS {
        state
            .routes
            .schedule_rid_refresh(src_media, rid, now + delay);
    }
    debug!(
        source_transport_media_id = ?src_media,
        ?rid,
        retry_count = SELECTED_RID_KEYFRAME_RETRY_DELAYS.len(),
        "scheduled follow-up selected RID keyframe refreshes"
    );
}

/// drains due follow-up refreshes for the source/rid currently being observed
///
/// this catches retry deadlines naturally when the rid remains active and keeps
/// the packet-loop timer path as a fallback for quiet rids
fn drain_live_rid_kf_retries(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    now: Instant,
) {
    let due_count = state.routes.drain_due_rid_refreshes(src_media, rid, now);
    for _ in 0..due_count {
        debug!(
            user_id = ?src_key.user_id(),
            media_worker_id = src_key.media_worker_id().as_usize(),
            source_transport_media_id = ?src_media,
            ?rid,
            "draining follow-up selected RID keyframe refresh"
        );
        request_live_rid_kf(
            state,
            metrics,
            src_key,
            src_media,
            rid,
            KeyframeRequestMode::Retry,
        );
    }
}

/// resolves the current owner session used for timer-driven rid refreshes
///
/// a source may be local to this worker or represented as a remote consumer
/// registration
/// the returned session key is only a best-effort snapshot and request-time
/// ownership checks still apply before a remote keyframe request is sent
fn kf_refresh_src_key(
    state: &PacketLoopState,
    src_media: TransportMediaId,
) -> Option<TransportSessionKey> {
    match state.media_handle(src_media) {
        Some(RegisteredMediaHandle::Producer { session_key, .. }) => Some(session_key.clone()),
        Some(RegisteredMediaHandle::Consumer { .. }) | None => state
            .routes
            .remote_source(src_media)
            .map(|registration| registration.src_key().clone()),
    }
}
