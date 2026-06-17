//! keyframe-request routing for worker-local and relayed sources
//!
//! all callers dispatch through a local or remote target so pending request
//! tracking and retry accounting stay in one place

use std::time::Instant;

use str0m::media::{KeyframeRequestKind, Mid, Rid};
use tracing::debug;

use super::{
    super::super::super::{
        commands::RemoteSourceControl,
        keyframe_tracker::KeyframeRequestDecision,
        media_registry::RegisteredMediaHandle,
        route_control::PacketLayerGate,
        source_route::{MediaRouteDestination, RemoteSourceRegistration},
        state::{PacketLoopState, RtcSessionState},
    },
    control::{ensure_existing_route_src, ensure_local_producer_mid},
    types::RouteSourceKind,
};
use crate::engine::{
    media_transport::{
        TransportAdapterError, TransportConsumerRoute, TransportMediaId, TransportSessionKey,
        TransportSourceKey, rtc::relay_registry::RelayTargetId,
    },
    metrics::{
        RtcKeyframeRequestOutcome, RtcRouteControlMetrics, RtcRouteControlOutcome, RuntimeMetrics,
    },
};

pub fn worker_request_remote_kf(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    src: &TransportSourceKey,
    target_id: RelayTargetId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) {
    if !state
        .routes
        .is_relay_target_active(src.transport_media_id(), target_id)
    {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        return;
    }
    request_kf_for_target(
        state,
        metrics,
        KeyframeRequestTarget::Local(src.session_key(), src.transport_media_id()),
        rid,
        kind,
        KeyframeRequestMode::Track(Instant::now()),
    );
}

#[derive(Clone, Copy)]
pub enum KeyframeRequestTarget<'a> {
    Local(&'a TransportSessionKey, TransportMediaId),
    Remote(&'a TransportSourceKey, &'a RemoteSourceControl),
}

#[derive(Debug, Clone, Copy)]
pub enum KeyframeRequestMode {
    Track(Instant),
    Retry,
}

impl KeyframeRequestMode {
    fn track_at(self) -> Option<Instant> {
        match self {
            Self::Track(now) => Some(now),
            Self::Retry => None,
        }
    }

    fn outcome(self) -> RtcKeyframeRequestOutcome {
        match self {
            Self::Track(_) => RtcKeyframeRequestOutcome::Forwarded,
            Self::Retry => RtcKeyframeRequestOutcome::Retry,
        }
    }
}

pub fn request_kf_for_target(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    target: KeyframeRequestTarget<'_>,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) {
    match target {
        KeyframeRequestTarget::Local(src_key, src_media) => {
            request_local_kf(state, metrics, src_key, src_media, rid, kind, mode);
        }
        KeyframeRequestTarget::Remote(source, source_control) => {
            request_remote_kf(state, metrics, source, source_control, rid, kind, mode);
        }
    }
}

fn request_local_kf(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) {
    let Some(mid) = local_kf_req_mid(state, src_key, src_media, rid, kind) else {
        return;
    };
    let target_rids = producer_kf_target_rids(state, src_key, src_media, mid, rid, kind);
    if target_rids.is_empty() {
        return;
    }
    if !track_kf_req(state, metrics, src_media, rid, kind, mode) {
        return;
    }
    if request_kf_from_producer(state, metrics, src_key, src_media, mid, &target_rids, kind) {
        metrics.record_rtc_keyframe_request(mode.outcome());
    } else {
        state.routes.forget_kf_req(src_media, rid);
    }
}

fn request_remote_kf(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src: &TransportSourceKey,
    src_control: &RemoteSourceControl,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) {
    if !track_kf_req(state, metrics, src.transport_media_id(), rid, kind, mode) {
        return;
    }
    if src_control.request_kf(src, rid, kind) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
        metrics.record_rtc_keyframe_request(mode.outcome());
    } else {
        state.routes.forget_kf_req(src.transport_media_id(), rid);
    }
}

fn track_kf_req(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) -> bool {
    let Some(now) = mode.track_at() else {
        return true;
    };
    match state.routes.track_kf_req(src_media, rid, kind, now) {
        KeyframeRequestDecision::Forward => true,
        KeyframeRequestDecision::Absorb => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
            metrics.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Absorbed);
            false
        }
    }
}

/// request a refresh frame for an already-declared consumer route
pub fn worker_request_consumer_kf(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    route: &TransportConsumerRoute,
) -> Result<(), TransportAdapterError> {
    let consumer_key = route.consumer_session_key();
    let consumer_media = route.consumer_transport_media_id();
    let src_key = route.source_session_key();
    let src_media = route.source_transport_media_id();
    let route_source = ensure_existing_route_src(state, consumer_key, route.source())?;
    match state.media_handle(consumer_media) {
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            src_media: consumer_src_media,
            ..
        }) if session_key == consumer_key && *consumer_src_media == src_media => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let (dst_active, dst_rid) = state
        .routes
        .local_route(src_media)
        .and_then(|route_entry| {
            route_entry.destinations.iter().find(|dst| {
                dst.dest_session == *consumer_key && dst.dest_transport_media_id == consumer_media
            })
        })
        .map(|dst| (dst.active, kf_req_rid(dst)))
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if !dst_active {
        return Ok(());
    }
    let now = Instant::now();
    match route_source {
        RouteSourceKind::Local => {
            request_kf_for_target(
                state,
                metrics,
                KeyframeRequestTarget::Local(src_key, src_media),
                dst_rid,
                KeyframeRequestKind::Pli,
                KeyframeRequestMode::Track(now),
            );
        }
        RouteSourceKind::Remote => {
            let Some((src, src_control)) = state
                .routes
                .remote_source(src_media)
                .map(RemoteSourceRegistration::cloned_control_path)
            else {
                return Err(TransportAdapterError::TransportUnavailable);
            };
            request_kf_for_target(
                state,
                metrics,
                KeyframeRequestTarget::Remote(&src, &src_control),
                dst_rid,
                KeyframeRequestKind::Pli,
                KeyframeRequestMode::Track(now),
            );
        }
    }
    Ok(())
}

/// returns the RID that a route-level keyframe command should refresh
///
/// route commands can bootstrap a pending selected RID, so they look at
/// `pending_gate` before the effective fallback gate
fn kf_req_rid(dst: &MediaRouteDestination) -> Option<Rid> {
    dst.pending_gate
        .as_ref()
        .and_then(PacketLayerGate::selected_rid)
        .or_else(|| dst.packet_gate.selected_rid())
}

fn local_kf_req_mid(
    state: &PacketLoopState,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) -> Option<Mid> {
    let mid = ensure_local_producer_mid(state, src_key, src_media).ok();
    if mid.is_none() {
        log_ignored_kf_req(
            src_key,
            src_media,
            None,
            rid,
            kind,
            "ignored keyframe request for unknown local producer",
        );
    }
    mid
}

fn producer_kf_target_rids(
    state: &mut PacketLoopState,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    mid: Mid,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) -> Vec<Option<Rid>> {
    let Some(session_state) = state.users.get_mut(src_key) else {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            rid,
            kind,
            "ignored keyframe request for missing source session",
        );
        return Vec::new();
    };
    let candidate_rids = if rid.is_none() {
        producer_kf_candidate_rids(session_state, mid)
    } else {
        Vec::new()
    };
    let mut direct_api = session_state.rtc.direct_api();
    let mut target_rids = Vec::new();
    if let Some(rid) = rid {
        if direct_api.stream_rx_by_mid(mid, Some(rid)).is_some() {
            target_rids.push(Some(rid));
        }
    } else {
        for candidate_rid in candidate_rids {
            if direct_api
                .stream_rx_by_mid(mid, Some(candidate_rid))
                .is_some()
            {
                target_rids.push(Some(candidate_rid));
            }
        }
        if target_rids.is_empty() && direct_api.stream_rx_by_mid(mid, None).is_some() {
            target_rids.push(None);
        }
    }
    if target_rids.is_empty() {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            rid,
            kind,
            "ignored keyframe request for missing producer stream",
        );
    }
    target_rids
}

fn producer_kf_candidate_rids(session_state: &RtcSessionState, mid: Mid) -> Vec<Rid> {
    let mut rids = Vec::new();
    if let Some(parameters) = session_state
        .sdp_negotiation
        .negotiated_producer_parameters
        .get(&mid)
    {
        for rid in parameters
            .bindings()
            .filter_map(|binding| binding.rid().map(Rid::from))
        {
            push_unique_rid(&mut rids, rid);
        }
    }
    if let Some(pending_streams) = session_state.sdp_negotiation.pending_recv_streams.get(&mid) {
        for rid in pending_streams.iter().filter_map(|stream| stream.rid) {
            push_unique_rid(&mut rids, rid);
        }
    }
    rids
}

fn push_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn request_kf_from_producer(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    mid: Mid,
    target_rids: &[Option<Rid>],
    kind: KeyframeRequestKind,
) -> bool {
    let Some(session_state) = state.users.get_mut(src_key) else {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing source session",
        );
        return false;
    };
    let mut direct_api = session_state.rtc.direct_api();
    let mut requested_rids = Vec::with_capacity(target_rids.len());
    for target_rid in target_rids {
        if let Some(stream_rx) = direct_api.stream_rx_by_mid(mid, *target_rid) {
            stream_rx.request_keyframe(kind);
            requested_rids.push(*target_rid);
        }
    }
    if requested_rids.is_empty() {
        log_ignored_kf_req(
            src_key,
            src_media,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing producer stream",
        );
        return false;
    }
    state.mark_session_dirty(src_key);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    debug!(
        source_session_key = ?src_key,
        source_transport_media_id = ?src_media,
        ?mid,
        ?requested_rids,
        ?kind,
        "requested local producer keyframe"
    );
    true
}

fn log_ignored_kf_req(
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    mid: Option<Mid>,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    message: &'static str,
) {
    debug!(
        source_session_key = ?src_key,
        source_transport_media_id = ?src_media,
        ?mid,
        ?rid,
        ?kind,
        message
    );
}
