//! Keyframe-request routing for worker-local and relayed sources.
//!
//! The packet loop and relay-control paths both need the same source-ownership
//! checks and route-control throttling. This module keeps those rules in one
//! place so local and cross-worker feedback stay consistent.

use std::time::Instant;

use str0m::media::{KeyframeRequestKind, Mid, Rid};
use tracing::debug;

use super::{
    super::super::{
        demux::MediaRouteDestination, media_registry::RegisteredMediaHandle,
        route_control::KeyframeRequestDecision, state::RtcBootstrapState,
    },
    control::{ensure_existing_route_source, owned_local_producer_mid, packet_gate_rid},
    types::{RemoteKeyframeRequest, RouteSourceKind},
};
use crate::runtime::{
    media_transport::{TransportAdapterError, TransportMediaId, TransportSessionKey},
    metrics::{RtcRouteControlOutcome, RuntimeMetrics},
};

pub fn respond_request_remote_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    request: &RemoteKeyframeRequest<'_>,
) {
    if !state.is_relay_target_active(request.source_transport_media_id, request.target_id) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        return;
    }
    request_keyframe_for_source(
        state,
        metrics,
        request.source_session_key,
        request.source_transport_media_id,
        request.rid,
        request.kind,
        Instant::now(),
    );
}

/// Forward a keyframe request to a locally owned producer when route-control
/// policy says the request should escape the shard.
///
/// This is reused by the packet loop after it resolves feedback back to a local
/// source. The helper stays worker-local so the packet path does not need to
/// know how producer ownership or per-source throttling are represented.
pub fn request_keyframe_for_source(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    now: Instant,
) {
    let Some(mid) = local_keyframe_request_mid(
        state,
        source_session_key,
        source_transport_media_id,
        rid,
        kind,
    ) else {
        return;
    };
    let target_rids = producer_keyframe_target_rids(
        state,
        source_session_key,
        source_transport_media_id,
        mid,
        rid,
        kind,
    );
    if target_rids.is_empty() {
        return;
    }
    if should_absorb_keyframe_request(
        state,
        source_session_key,
        source_transport_media_id,
        mid,
        rid,
        kind,
        now,
    ) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        return;
    }
    request_keyframe_from_producer(
        state,
        metrics,
        source_session_key,
        source_transport_media_id,
        mid,
        &target_rids,
        kind,
    );
}

/// Request a refresh frame for an already-declared consumer route.
///
/// The worker revalidates consumer/source ownership and skips paused routes.
/// RID-gated destinations are mapped back into the keyframe target before the
/// local source is marked dirty or the remote keyframe request is forwarded with
/// the normal coalescing rules.
pub(in crate::runtime::rtc_engine::worker::media) fn worker_request_consumer_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let route_source = ensure_existing_route_source(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
    )?;
    match state
        .mid_registry
        .get(&consumer_transport_media_id.as_u64())
    {
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            source_transport_media_id: consumer_source_transport_media_id,
            ..
        }) if session_key == consumer_session_key
            && *consumer_source_transport_media_id == source_transport_media_id => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let (destination_active, destination_rid) = state
        .media_route_index
        .get(&source_transport_media_id)
        .and_then(|route_entry| {
            route_entry.destinations.iter().find(|destination| {
                destination.dest_session == *consumer_session_key
                    && destination.dest_transport_media_id == consumer_transport_media_id
            })
        })
        .map(|destination| (destination.active, keyframe_request_rid(destination)))
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if !destination_active {
        return Ok(());
    }
    let now = Instant::now();
    match route_source {
        RouteSourceKind::Local => {
            request_keyframe_for_source(
                state,
                metrics,
                source_session_key,
                source_transport_media_id,
                destination_rid,
                KeyframeRequestKind::Pli,
                now,
            );
        }
        RouteSourceKind::Remote => {
            let Some((source_session_key, source_control)) = state
                .remote_source_registration(source_transport_media_id)
                .map(|registration| {
                    (
                        registration.source_session_key().clone(),
                        registration.source_control().clone(),
                    )
                })
            else {
                return Err(TransportAdapterError::TransportUnavailable);
            };
            match state.route_control.decide_keyframe_request_for_rid(
                source_transport_media_id,
                destination_rid,
                now,
            ) {
                KeyframeRequestDecision::Forward => {
                    source_control.request_keyframe(
                        source_session_key,
                        source_transport_media_id,
                        destination_rid,
                        KeyframeRequestKind::Pli,
                    );
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
                }
                KeyframeRequestDecision::Absorb => {
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
                }
            }
        }
    }
    Ok(())
}

fn keyframe_request_rid(destination: &MediaRouteDestination) -> Option<Rid> {
    destination
        .pending_packet_gate
        .as_ref()
        .and_then(packet_gate_rid)
        .or_else(|| packet_gate_rid(&destination.packet_gate))
}

fn local_keyframe_request_mid(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) -> Option<Mid> {
    let mid = owned_local_producer_mid(state, source_session_key, source_transport_media_id);
    if mid.is_none() {
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
            None,
            rid,
            kind,
            "ignored keyframe request for unknown local producer",
        );
    }
    mid
}

fn producer_keyframe_target_rids(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    mid: Mid,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) -> Vec<Option<Rid>> {
    let candidate_rids = producer_keyframe_candidate_rids(state, source_session_key, mid, rid);
    let Some(session_state) = state.users.get_mut(source_session_key) else {
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
            Some(mid),
            rid,
            kind,
            "ignored keyframe request for missing source session",
        );
        return Vec::new();
    };
    let mut direct_api = session_state.rtc.direct_api();
    let mut target_rids = candidate_rids
        .into_iter()
        .filter(|candidate_rid| direct_api.stream_rx_by_mid(mid, *candidate_rid).is_some())
        .collect::<Vec<_>>();
    if target_rids.is_empty() && rid.is_none() && direct_api.stream_rx_by_mid(mid, None).is_some() {
        target_rids.push(None);
    }
    if target_rids.is_empty() {
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
            Some(mid),
            rid,
            kind,
            "ignored keyframe request for missing producer stream",
        );
    }
    target_rids
}

fn producer_keyframe_candidate_rids(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    mid: Mid,
    rid: Option<Rid>,
) -> Vec<Option<Rid>> {
    if rid.is_some() {
        return vec![rid];
    }
    let Some(session_state) = state.users.get(source_session_key) else {
        return vec![None];
    };
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
    if rids.is_empty() {
        vec![None]
    } else {
        rids.into_iter().map(Some).collect()
    }
}

fn push_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn should_absorb_keyframe_request(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    mid: Mid,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    now: Instant,
) -> bool {
    if matches!(
        state
            .route_control
            .decide_keyframe_request_for_rid(source_transport_media_id, rid, now),
        KeyframeRequestDecision::Absorb
    ) {
        debug!(
            ?source_session_key,
            ?source_transport_media_id,
            ?mid,
            ?rid,
            ?kind,
            "absorbed duplicate local keyframe request"
        );
        return true;
    }
    false
}

fn request_keyframe_from_producer(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    mid: Mid,
    target_rids: &[Option<Rid>],
    kind: KeyframeRequestKind,
) {
    let Some(session_state) = state.users.get_mut(source_session_key) else {
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing source session",
        );
        return;
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
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing producer stream",
        );
        return;
    }
    state.mark_session_dirty(source_session_key);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    debug!(
        ?source_session_key,
        ?source_transport_media_id,
        ?mid,
        ?requested_rids,
        ?kind,
        "requested local producer keyframe"
    );
}

fn log_ignored_keyframe_request(
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    mid: Option<Mid>,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    message: &'static str,
) {
    debug!(
        ?source_session_key,
        ?source_transport_media_id,
        ?mid,
        ?rid,
        ?kind,
        message
    );
}
