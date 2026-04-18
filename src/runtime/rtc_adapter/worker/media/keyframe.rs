use std::time::Instant;

use str0m::media::{KeyframeRequestKind, Rid};

use crate::runtime::metrics::{RtcRouteControlOutcome, RuntimeMetrics};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::super::super::{
    relay_registry::RelayRegistry, route_control::KeyframeRequestDecision, state::RtcBootstrapState,
};
use super::control::owned_local_producer_mid;
use super::types::RemoteKeyframeRequest;

pub(crate) fn respond_request_remote_keyframe(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    relay_registry: &RelayRegistry,
    request: &RemoteKeyframeRequest<'_>,
) {
    if !relay_registry.is_source_target_active(request.source_transport_media_id, request.target_id)
    {
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

pub(crate) fn request_keyframe_for_source(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    now: Instant,
) {
    let Some(mid) = owned_local_producer_mid(state, source_session_key, source_transport_media_id)
    else {
        return;
    };
    let Some(session_state) = state.sessions.get_mut(source_session_key) else {
        return;
    };
    if session_state
        .rtc
        .direct_api()
        .stream_rx_by_mid(mid, rid)
        .is_none()
    {
        return;
    }
    if matches!(
        state
            .route_control
            .decide_keyframe_request(source_transport_media_id, now),
        KeyframeRequestDecision::Absorb
    ) {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        return;
    }
    let Some(session_state) = state.sessions.get_mut(source_session_key) else {
        return;
    };
    let mut direct_api = session_state.rtc.direct_api();
    let Some(stream_rx) = direct_api.stream_rx_by_mid(mid, rid) else {
        return;
    };
    stream_rx.request_keyframe(kind);
    state.mark_session_dirty(source_session_key);
    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
}
