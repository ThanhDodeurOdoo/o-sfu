//! selected-rid readiness for route packet gates
//!
//! selected-rid gates are stricter than ordinary packet gates because the
//! receiver must not be switched to a simulcast layer until that layer is fresh
//! enough to decode
//! this module keeps the room-selected gate in `pending_gate` while the
//! packet loop waits for a complete decoder refresh on that rid
//!
//! the effective gate is allowed to differ from the selected gate during
//! bootstrap:
//! - `Block` means the selected rid has not produced a fresh packet yet
//! - a fallback `Rid` means another live rid has produced a decoder refresh and can
//!   keep the receiver decodable while the selected rid is requested again
//! - the pending gate becomes effective only after the selected rid decoder
//!   refresh is complete
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
            route_control::PacketLayerGate,
            route_table::{RidReadinessRouteUpdate, RidReadinessSelectedGateUpdate},
            source_route::RemoteSourceRegistration,
            state::{PacketLoopState, RidReadinessScratch},
        },
    },
    metrics::RtcMetricsRecorder,
};

/// maximum age for treating a producer rid as live enough for strict gating
///
/// browser encoders may stop sending a rid after adaptation
/// readiness is therefore freshness-based instead of a permanent once-seen bit
const SELECTED_RID_READY_MAX_AGE: Duration = Duration::from_secs(2);

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
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    complete_refresh: bool,
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
            complete_refresh,
            "observed first live RTP for producer RID"
        );
    }
    apply_src_rid_ready(
        state,
        metrics,
        src_key,
        src_media,
        rid,
        complete_refresh,
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
#[cfg(any(test, feature = "internal-benchmarks"))]
pub fn apply_src_rid_ready(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    complete_refresh: bool,
    now: Instant,
) -> bool {
    apply_src_decoder_ready(
        state,
        metrics,
        src_key,
        src_media,
        Some(rid),
        complete_refresh,
        now,
    )
}

/// applies decoder readiness for a source packet with or without a RID
///
/// RID-less video uses the same complete-refresh barrier as simulcast routes
/// without scheduling RID-specific producer refresh retries
pub fn apply_src_decoder_ready(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    complete_refresh: bool,
    now: Instant,
) -> bool {
    let mut scratch = take(&mut state.rid_readiness_scratch);
    let route_update =
        update_rid_readiness_routes(state, src_media, rid, complete_refresh, now, &mut scratch);
    let gate_changed = route_update.changed_gate();
    for stale_rid in scratch.stale.iter().copied() {
        request_live_rid_kf(
            state,
            metrics,
            src_key,
            src_media,
            stale_rid,
            KeyframeRequestMode::DecoderTransition(now),
        );
    }
    match (rid, route_update.selected_gate) {
        (Some(rid), RidReadinessSelectedGateUpdate::Activated) => {
            for pending_rid in scratch
                .pending_selected
                .iter()
                .copied()
                .filter(|pending_rid| *pending_rid != rid)
            {
                request_live_rid_kf(
                    state,
                    metrics,
                    src_key,
                    src_media,
                    pending_rid,
                    KeyframeRequestMode::DecoderTransition(now),
                );
            }
        }
        (Some(_), RidReadinessSelectedGateUpdate::BootstrapFallback) => {
            for pending_rid in scratch.pending_selected.iter().copied() {
                request_live_rid_kf(
                    state,
                    metrics,
                    src_key,
                    src_media,
                    pending_rid,
                    KeyframeRequestMode::DecoderTransition(now),
                );
            }
        }
        (Some(rid), RidReadinessSelectedGateUpdate::Pending) => request_live_rid_kf(
            state,
            metrics,
            src_key,
            src_media,
            rid,
            KeyframeRequestMode::DecoderTransition(now),
        ),
        (None, _) | (_, RidReadinessSelectedGateUpdate::None) => {}
    }
    scratch.clear();
    state.rid_readiness_scratch = scratch;
    gate_changed
}

/// splits a selected-rid gate into effective and pending transport state
///
/// the effective gate is what the packet loop enforces now
/// the pending gate is the target selected by room policy
/// keeping both lets bootstrap forwarding stay decodable without losing the
/// receiver's intended layer
///
/// non-rid gates pass through unchanged
/// rid gates remain pending until the packet loop observes a complete decoder
/// refresh for the destination target
pub(super) fn guarded_pkt_gate(
    src_media: TransportMediaId,
    packet_gate: PacketLayerGate,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    let Some(rid) = packet_gate.selected_rid() else {
        return (packet_gate, None);
    };
    debug!(
        source_transport_media_id = ?src_media,
        ?rid,
        requested_packet_gate = ?packet_gate,
        "deferred selected RID route until its decoder refresh is complete"
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
    incoming_rid: Option<Rid>,
    complete_refresh: bool,
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
        complete_refresh,
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
pub fn request_src_decoder_refresh(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    now: Instant,
) {
    request_live_source_kf(
        state,
        metrics,
        src_key,
        src_media,
        rid,
        KeyframeRequestMode::DecoderTransition(now),
    );
}

fn request_live_rid_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    mode: KeyframeRequestMode,
) {
    request_live_source_kf(state, metrics, src_key, src_media, Some(rid), mode);
}

fn request_live_source_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
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
            rid,
            KeyframeRequestKind::Pli,
            mode,
        );
        return;
    }
    let Some((registered_src, src_control)) = state
        .routes
        .remote_source(src_media)
        .map(RemoteSourceRegistration::cloned_control_path)
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
        rid,
        KeyframeRequestKind::Pli,
        mode,
    );
}
