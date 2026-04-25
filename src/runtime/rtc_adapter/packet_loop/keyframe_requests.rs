use std::time::Instant;

use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};

use super::{
    super::{
        commands::RemoteSourceControl,
        media_registry::RegisteredMediaHandle,
        route_control::{KeyframeRequestDecision, coalesce_keyframe_kind},
        state::RtcBootstrapState,
        worker::request_keyframe_for_source,
    },
    buffers::PacketLoopBuffers,
};
use crate::runtime::{
    metrics::{RtcRouteControlOutcome, RuntimeMetrics},
    transport_adapter::{TransportMediaId, TransportSessionKey},
};

#[derive(Debug, Clone, Copy)]
pub(super) struct PendingKeyframeRequest {
    pub(super) consumer_mid: Mid,
    pub(super) consumer_rid: Option<Rid>,
    pub(super) kind: KeyframeRequestKind,
}

impl PendingKeyframeRequest {
    pub(super) fn new(request: KeyframeRequest) -> Self {
        Self {
            consumer_mid: request.mid,
            consumer_rid: request.rid,
            kind: request.kind,
        }
    }
}

#[derive(Clone)]
pub(super) enum ResolvedKeyframeRoute {
    Local {
        source_session_key: TransportSessionKey,
    },
    Remote {
        source_session_key: TransportSessionKey,
        source_control: RemoteSourceControl,
    },
}

#[derive(Clone)]
pub(super) struct CoalescedKeyframeRequest {
    source_transport_media_id: TransportMediaId,
    route: ResolvedKeyframeRoute,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
}

impl CoalescedKeyframeRequest {
    fn new(
        source_transport_media_id: TransportMediaId,
        route: ResolvedKeyframeRoute,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    ) -> Self {
        Self {
            source_transport_media_id,
            route,
            rid,
            kind,
        }
    }

    fn coalesce(&mut self, rid: Option<Rid>, kind: KeyframeRequestKind) {
        self.rid = self.rid.or(rid);
        self.kind = coalesce_keyframe_kind(self.kind, kind);
    }
}

pub(super) fn flush_pending_keyframe_requests(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    let pending_keyframe_requests = &mut buffers.pending_keyframe_requests;
    let coalesced_requests = &mut buffers.coalesced_keyframe_requests;
    coalesced_requests.clear();
    for (consumer_session_key, request) in pending_keyframe_requests.drain(..) {
        let Some(source_transport_media_id) = state.consumer_source_transport_media_id_for_mid(
            &consumer_session_key,
            request.consumer_mid,
        ) else {
            continue;
        };
        let Some(route) = resolve_keyframe_route(state, source_transport_media_id) else {
            continue;
        };
        coalesced_requests.push(CoalescedKeyframeRequest::new(
            source_transport_media_id,
            route,
            request.consumer_rid,
            request.kind,
        ));
    }
    coalesced_requests.sort_by_key(|request| request.source_transport_media_id);
    let now = Instant::now();
    let mut current_request: Option<CoalescedKeyframeRequest> = None;
    for coalesced_request in coalesced_requests.drain(..) {
        match &mut current_request {
            Some(current)
                if current.source_transport_media_id
                    == coalesced_request.source_transport_media_id =>
            {
                current.coalesce(coalesced_request.rid, coalesced_request.kind);
            }
            Some(_) => {
                if let Some(request) = current_request.take() {
                    flush_coalesced_keyframe_request(state, metrics, request, now);
                }
                current_request = Some(coalesced_request);
            }
            None => {
                current_request = Some(coalesced_request);
            }
        }
    }
    if let Some(request) = current_request {
        flush_coalesced_keyframe_request(state, metrics, request, now);
    }
}

fn flush_coalesced_keyframe_request(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    coalesced_request: CoalescedKeyframeRequest,
    now: Instant,
) {
    match coalesced_request.route {
        ResolvedKeyframeRoute::Local { source_session_key } => request_keyframe_for_source(
            state,
            metrics,
            &source_session_key,
            coalesced_request.source_transport_media_id,
            coalesced_request.rid,
            coalesced_request.kind,
            now,
        ),
        ResolvedKeyframeRoute::Remote {
            source_session_key,
            source_control,
        } => {
            match state
                .route_control
                .decide_keyframe_request(coalesced_request.source_transport_media_id, now)
            {
                KeyframeRequestDecision::Forward => {
                    source_control.request_keyframe(
                        source_session_key,
                        coalesced_request.source_transport_media_id,
                        coalesced_request.rid,
                        coalesced_request.kind,
                    );
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
                }
                KeyframeRequestDecision::Absorb => {
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
                }
            }
        }
    }
}

fn resolve_keyframe_route(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<ResolvedKeyframeRoute> {
    if let Some(RegisteredMediaHandle::Producer { session_key, .. }) =
        state.mid_registry.get(&source_transport_media_id.as_u64())
    {
        return Some(ResolvedKeyframeRoute::Local {
            source_session_key: session_key.clone(),
        });
    }
    state
        .remote_source_registration(source_transport_media_id)
        .cloned()
        .map(|remote_source| ResolvedKeyframeRoute::Remote {
            source_session_key: remote_source.source_session_key().clone(),
            source_control: remote_source.source_control().clone(),
        })
}
