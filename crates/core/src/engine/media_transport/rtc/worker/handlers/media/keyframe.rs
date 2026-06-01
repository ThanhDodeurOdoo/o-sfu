//! keyframe-request routing for worker-local and relayed sources
//!
//! all callers dispatch through a local or remote target so pending request
//! tracking and retry accounting stay in one place

use std::time::Instant;

use str0m::media::{KeyframeRequestKind, Mid, Rid};
use tracing::debug;

use super::{
    super::super::super::{
        commands::RemoteSourceControl, demux::MediaRouteDestination,
        keyframe_tracker::KeyframeRequestDecision, media_registry::RegisteredMediaHandle,
        route_control::PacketLayerGate, state::PacketLoopState,
    },
    control::{ensure_existing_route_source, owned_local_producer_mid},
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

pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn worker_request_remote_keyframe(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) {
    if !state.is_relay_target_active(source.transport_media_id(), target_id) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        return;
    }
    request_keyframe_for_target(
        state,
        metrics,
        KeyframeRequestTarget::Local(source.session_key(), source.transport_media_id()),
        rid,
        kind,
        KeyframeRequestMode::Track(Instant::now()),
    );
}

#[derive(Clone, Copy)]
pub(in crate::engine::media_transport::rtc) enum KeyframeRequestTarget<'a> {
    Local(&'a TransportSessionKey, TransportMediaId),
    Remote(&'a TransportSourceKey, &'a RemoteSourceControl),
}

#[derive(Debug, Clone, Copy)]
pub(in crate::engine::media_transport::rtc) enum KeyframeRequestMode {
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

pub(in crate::engine::media_transport::rtc) fn request_keyframe_for_target(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    target: KeyframeRequestTarget<'_>,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) {
    match target {
        KeyframeRequestTarget::Local(source_session_key, source_transport_media_id) => {
            request_local_keyframe(
                state,
                metrics,
                source_session_key,
                source_transport_media_id,
                rid,
                kind,
                mode,
            );
        }
        KeyframeRequestTarget::Remote(source, source_control) => {
            request_remote_keyframe(state, metrics, source, source_control, rid, kind, mode);
        }
    }
}

fn request_local_keyframe(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
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
    if !track_keyframe_request(state, metrics, source_transport_media_id, rid, kind, mode) {
        return;
    }
    if request_keyframe_from_producer(
        state,
        metrics,
        source_session_key,
        source_transport_media_id,
        mid,
        &target_rids,
        kind,
    ) {
        metrics.record_rtc_keyframe_request(mode.outcome());
    } else {
        state
            .keyframe_requests
            .forget(source_transport_media_id, rid);
    }
}

fn request_remote_keyframe(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source: &TransportSourceKey,
    source_control: &RemoteSourceControl,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) {
    if !track_keyframe_request(state, metrics, source.transport_media_id(), rid, kind, mode) {
        return;
    }
    if source_control.request_keyframe(source, rid, kind) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
        metrics.record_rtc_keyframe_request(mode.outcome());
    } else {
        state
            .keyframe_requests
            .forget(source.transport_media_id(), rid);
    }
}

fn track_keyframe_request(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    mode: KeyframeRequestMode,
) -> bool {
    let Some(now) = mode.track_at() else {
        return true;
    };
    match state
        .keyframe_requests
        .track(source_transport_media_id, rid, kind, now)
    {
        KeyframeRequestDecision::Forward => true,
        KeyframeRequestDecision::Absorb => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
            metrics.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Absorbed);
            false
        }
    }
}

/// request a refresh frame for an already-declared consumer route
pub(in crate::engine::media_transport::rtc::worker::handlers::media) fn worker_request_consumer_keyframe(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    route: &TransportConsumerRoute,
) -> Result<(), TransportAdapterError> {
    let consumer_session_key = route.consumer_session_key();
    let consumer_transport_media_id = route.consumer_transport_media_id();
    let source_session_key = route.source_session_key();
    let source_transport_media_id = route.source_transport_media_id();
    let route_source = ensure_existing_route_source(state, consumer_session_key, route.source())?;
    match state.media_handle(consumer_transport_media_id) {
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
            request_keyframe_for_target(
                state,
                metrics,
                KeyframeRequestTarget::Local(source_session_key, source_transport_media_id),
                destination_rid,
                KeyframeRequestKind::Pli,
                KeyframeRequestMode::Track(now),
            );
        }
        RouteSourceKind::Remote => {
            let Some((source, source_control)) = state
                .remote_source_registration(source_transport_media_id)
                .map(|registration| {
                    (
                        registration.source().clone(),
                        registration.source_control().clone(),
                    )
                })
            else {
                return Err(TransportAdapterError::TransportUnavailable);
            };
            request_keyframe_for_target(
                state,
                metrics,
                KeyframeRequestTarget::Remote(&source, &source_control),
                destination_rid,
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
/// `pending_packet_gate` before the effective fallback gate
fn keyframe_request_rid(destination: &MediaRouteDestination) -> Option<Rid> {
    destination
        .pending_packet_gate
        .as_ref()
        .and_then(PacketLayerGate::selected_rid)
        .or_else(|| destination.packet_gate.selected_rid())
}

fn local_keyframe_request_mid(
    state: &PacketLoopState,
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
    state: &mut PacketLoopState,
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
    state: &PacketLoopState,
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

fn request_keyframe_from_producer(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    mid: Mid,
    target_rids: &[Option<Rid>],
    kind: KeyframeRequestKind,
) -> bool {
    let Some(session_state) = state.users.get_mut(source_session_key) else {
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
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
        log_ignored_keyframe_request(
            source_session_key,
            source_transport_media_id,
            Some(mid),
            None,
            kind,
            "ignored keyframe request for missing producer stream",
        );
        return false;
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
    true
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
