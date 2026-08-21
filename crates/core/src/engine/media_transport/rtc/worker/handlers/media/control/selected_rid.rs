//! Decoder-safe packet-gate transitions.
//!
//! Room policy chooses a requested gate. Worker-local readiness state decides
//! when that gate can become effective without stranding the decoder. Until a
//! keyframe arrives, the route retains the request in `pending_gate` and
//! enforces `Block`. A RID gate may use another recently decodable RID as a
//! temporary fallback while refreshing the selected RID.
//!
//! Packet liveness and decoder readiness are distinct. Delta packets refresh
//! RID liveness but do not activate a pending gate. A RID-less keyframe may
//! activate a pending `Open` gate.

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
            route_table::{
                RidReadinessRouteUpdate, RidReadinessScratch, RidReadinessSelectedGateUpdate,
            },
            source_route::RemoteSourceRegistration,
            state::PacketLoopState,
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
///
/// returns `true` when an effective packet gate changed
#[cfg(test)]
pub fn observe_src_rid_ready(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
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
    // Readiness uses the freshness set to detect stale RIDs. Include the packet
    // that triggered this transition before querying that set.
    apply_src_decoder_ready(
        state,
        metrics,
        src_key,
        src_media,
        Some(rid),
        is_keyframe,
        now,
    )
}

/// Applies one producer packet's decoder readiness to its consumer routes.
pub fn apply_src_decoder_ready(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
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
            KeyframeRequestMode::for_recovery(now, true),
        );
    }
    match route_update.selected_gate {
        RidReadinessSelectedGateUpdate::BootstrapFallback if rid.is_some() => {
            for pending_rid in scratch.pending_selected.iter().copied() {
                request_live_rid_kf(
                    state,
                    metrics,
                    src_key,
                    src_media,
                    pending_rid,
                    KeyframeRequestMode::for_recovery(now, true),
                );
            }
        }
        RidReadinessSelectedGateUpdate::Pending if let Some(rid) = rid => {
            request_live_rid_kf(
                state,
                metrics,
                src_key,
                src_media,
                rid,
                KeyframeRequestMode::for_recovery(now, true),
            );
        }
        RidReadinessSelectedGateUpdate::Activated
        | RidReadinessSelectedGateUpdate::BootstrapFallback
        | RidReadinessSelectedGateUpdate::Pending
        | RidReadinessSelectedGateUpdate::None => {}
    }
    scratch.clear();
    state.rid_readiness_scratch = scratch;
    route_update.changed_gate()
}

/// Defers a decoder-sensitive gate until a keyframe makes the destination decodable.
///
/// When no decoder refresh is required the requested gate takes effect directly.
/// Otherwise `Block` remains effective while the requested gate stays pending.
/// Recent packet liveness alone cannot restore the decoder reference chain.
pub(in crate::engine::media_transport::rtc) fn guarded_pkt_gate(
    requires_decoder_refresh: bool,
    src_media: TransportMediaId,
    packet_gate: PacketLayerGate,
) -> (PacketLayerGate, Option<PacketLayerGate>) {
    if !requires_decoder_refresh {
        return (packet_gate, None);
    }
    debug!(
        source_transport_media_id = ?src_media,
        requested_packet_gate = ?packet_gate,
        "blocked video route until its decoder refresh arrives"
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
    is_keyframe: bool,
    now: Instant,
    scratch: &mut RidReadinessScratch,
) -> RidReadinessRouteUpdate {
    state.routes.collect_ready_producer_rids(
        src_media,
        now,
        SELECTED_RID_READY_MAX_AGE,
        &mut scratch.ready,
    );
    let (routes, users) = (&mut state.routes, &mut state.users);
    routes.update_decoder_readiness(
        src_media,
        incoming_rid,
        is_keyframe,
        scratch,
        |destination| {
            if let Some(session_state) = users.get_mut(&destination.dest_session) {
                session_state.invalidate_rtx_stream(destination.dest_stream);
            }
        },
    )
}

/// requests a keyframe for a live rid on either a local or remote source
///
/// local sources can be refreshed directly through their registered producer
/// remote sources are refreshed through the relay source control after the
/// observed ownership is checked against the current registration
fn request_live_rid_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
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
        Some(rid),
        KeyframeRequestKind::Pli,
        mode,
    );
}
