//! route-level keyframe feedback handling
//!
//! consumer feedback arrives as MID/RID terms local to the receiving session
//! this module resolves it to the current producer source/RID target before
//! dispatching through the shared keyframe tracker

use std::{cmp::Ordering, time::Instant};

use str0m::media::{KeyframeRequest as RtcKeyframeRequest, KeyframeRequestKind, Mid, Rid};

use super::{
    super::{
        commands::RemoteSourceControl,
        keyframe_tracker::{SourceKeyframeRequest, coalesce_keyframe_kind},
        media_registry::RegisteredMediaHandle,
        state::PacketLoopState,
        worker::{KeyframeRequestMode, KeyframeRequestTarget, request_keyframe_for_target},
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey},
    metrics::RtcRouteControlMetrics,
};

/// keyframe feedback emitted by one consumer session before producer lookup
#[derive(Debug, Clone, Copy)]
pub(in crate::engine::media_transport::rtc) struct PendingKeyframeRequest {
    pub(super) consumer_mid: Mid,
    pub(super) consumer_rid: Option<Rid>,
    pub(super) kind: KeyframeRequestKind,
}

impl PendingKeyframeRequest {
    pub(super) fn new(request: RtcKeyframeRequest) -> Self {
        Self {
            consumer_mid: request.mid,
            consumer_rid: request.rid,
            kind: request.kind,
        }
    }

    #[cfg(feature = "internal-benchmarks")]
    pub const fn benchmark_request(mid: Mid, rid: Option<Rid>, kind: KeyframeRequestKind) -> Self {
        Self {
            consumer_mid: mid,
            consumer_rid: rid,
            kind,
        }
    }
}

pub(super) enum ResolvedKeyframeRoute {
    Local {
        source_session_key: TransportSessionKey,
    },
    Remote {
        source: TransportSourceKey,
        source_control: RemoteSourceControl,
    },
}

impl ResolvedKeyframeRoute {
    fn target(&self, source_transport_media_id: TransportMediaId) -> KeyframeRequestTarget<'_> {
        match self {
            Self::Local { source_session_key } => {
                KeyframeRequestTarget::Local(source_session_key, source_transport_media_id)
            }
            Self::Remote {
                source,
                source_control,
            } => KeyframeRequestTarget::Remote(source, source_control),
        }
    }
}

/// resolve and flush every keyframe request staged during the current turn
pub(super) fn flush_pending_keyframe_requests(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    flush_pending_keyframe_requests_at(state, metrics, buffers, Instant::now());
}

/// resolves staged keyframe requests at a supplied time for tests and benchmarks
pub(in crate::engine::media_transport::rtc) fn flush_pending_keyframe_requests_at(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) {
    let pending_keyframe_requests = &mut buffers.pending_keyframe_requests;
    let coalesced_requests = &mut buffers.coalesced_keyframe_requests;
    coalesced_requests.clear();
    let mut has_rid = false;
    let mut same_request: Option<SourceKeyframeRequest> = None;
    for (consumer_session_key, request) in pending_keyframe_requests.drain(..) {
        let Some(target) = state.active_consumer_keyframe_target_for_mid(
            &consumer_session_key,
            request.consumer_mid,
            request.consumer_rid,
        ) else {
            continue;
        };
        let resolved_request = SourceKeyframeRequest {
            source_transport_media_id: target.source_transport_media_id,
            rid: target.rid,
            kind: request.kind,
        };
        has_rid |= resolved_request.rid.is_some();
        // most turns carry repeated feedback for one target
        // keep that path out of sorting
        if coalesced_requests.is_empty() {
            match &mut same_request {
                Some(current)
                    if current.source_transport_media_id
                        == resolved_request.source_transport_media_id
                        && current.rid == resolved_request.rid =>
                {
                    current.kind = coalesce_keyframe_kind(current.kind, resolved_request.kind);
                }
                Some(_) => {
                    if let Some(current) = same_request.take() {
                        coalesced_requests.push(current);
                    }
                    coalesced_requests.push(resolved_request);
                }
                None => {
                    same_request = Some(resolved_request);
                }
            }
        } else {
            coalesced_requests.push(resolved_request);
        }
    }
    if coalesced_requests.is_empty() {
        if let Some(request) = same_request {
            flush_coalesced_keyframe_request(state, metrics, request, now);
        }
        return;
    }
    if has_rid {
        // rid-scoped batches need the full source/RID key to avoid widening
        // simulcast feedback
        coalesced_requests.sort_unstable_by(|left, right| {
            left.source_transport_media_id
                .cmp(&right.source_transport_media_id)
                .then_with(|| compare_keyframe_rids(left.rid, right.rid))
        });
    } else {
        coalesced_requests.sort_unstable_by_key(|request| request.source_transport_media_id);
    }
    let mut current_request: Option<SourceKeyframeRequest> = None;
    for coalesced_request in coalesced_requests.drain(..) {
        match &mut current_request {
            Some(current)
                if current.source_transport_media_id
                    == coalesced_request.source_transport_media_id
                    && current.rid == coalesced_request.rid =>
            {
                current.kind = coalesce_keyframe_kind(current.kind, coalesced_request.kind);
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

/// drain retry deadlines after new feedback has had a chance to arm them
pub(in crate::engine::media_transport::rtc) fn drain_due_keyframe_retries(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) {
    state
        .routes
        .drain_due_keyframe_requests(now, &mut buffers.keyframe_retries);
    for retry in buffers.keyframe_retries.drain(..) {
        flush_keyframe_retry(state, metrics, retry);
    }
}

fn compare_keyframe_rids(left: Option<Rid>, right: Option<Rid>) -> Ordering {
    left.as_deref().cmp(&right.as_deref())
}

fn flush_coalesced_keyframe_request(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    coalesced_request: SourceKeyframeRequest,
    now: Instant,
) {
    let Some(route) = resolve_keyframe_route(state, coalesced_request.source_transport_media_id)
    else {
        return;
    };
    request_keyframe_for_target(
        state,
        metrics,
        route.target(coalesced_request.source_transport_media_id),
        coalesced_request.rid,
        coalesced_request.kind,
        KeyframeRequestMode::Track(now),
    );
}

fn flush_keyframe_retry(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    retry: SourceKeyframeRequest,
) {
    let source_transport_media_id = retry.source_transport_media_id;
    let rid = retry.rid;
    let kind = retry.kind;
    if !state
        .routes
        .has_keyframe_demand(source_transport_media_id, rid)
    {
        state
            .routes
            .forget_keyframe_request(source_transport_media_id, rid);
        return;
    }
    let Some(route) = resolve_keyframe_route(state, source_transport_media_id) else {
        state
            .routes
            .forget_keyframe_request(source_transport_media_id, rid);
        return;
    };
    request_keyframe_for_target(
        state,
        metrics,
        route.target(source_transport_media_id),
        rid,
        kind,
        KeyframeRequestMode::Retry,
    );
}

/// resolve a producer media id to its local or relayed control path
fn resolve_keyframe_route(
    state: &PacketLoopState,
    source_transport_media_id: TransportMediaId,
) -> Option<ResolvedKeyframeRoute> {
    if let Some(RegisteredMediaHandle::Producer { session_key, .. }) =
        state.media_handle(source_transport_media_id)
    {
        return Some(ResolvedKeyframeRoute::Local {
            source_session_key: session_key.clone(),
        });
    }
    state
        .routes
        .remote_source(source_transport_media_id)
        .map(|remote_source| ResolvedKeyframeRoute::Remote {
            source: remote_source.source().clone(),
            source_control: remote_source.source_control().clone(),
        })
}
