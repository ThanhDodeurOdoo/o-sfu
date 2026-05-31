//! Route-level keyframe feedback handling.
//!
//! Consumer `str0m::Rtc` instances emit keyframe requests in consumer-local
//! terms, usually by MID and optional RID. The packet loop must translate that
//! feedback back to the producer source that can satisfy it. This module contains
//! that translation, duplicate coalescing and final dispatch to either a local
//! producer session or a remote source-control handle.
//!
//! Keyframe requests are route-control feedback, not room policy. The room
//! decides which sources and layers a user should receive. This module only
//! makes sure the currently routed producer gets a bounded, source/RID keyed
//! request stream.

use std::{cmp::Ordering, time::Instant};

use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};
use tracing::debug;

use super::{
    super::{
        commands::RemoteSourceControl,
        media_registry::RegisteredMediaHandle,
        route_control::{KeyframeRequestDecision, coalesce_keyframe_kind},
        state::PacketLoopState,
        worker::request_keyframe_for_source,
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey},
    metrics::{RtcRouteControlMetrics, RtcRouteControlOutcome},
};

/// Keyframe feedback emitted by one consumer session before producer lookup.
///
/// The MID is the consumer-local media identity. It must be resolved through
/// worker route state before the packet loop knows which producer source owns
/// the requested media.
#[derive(Debug, Clone, Copy)]
pub(in crate::engine::media_transport::rtc) struct PendingKeyframeRequest {
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

    #[cfg(feature = "internal-benchmarks")]
    pub const fn benchmark_request(mid: Mid, rid: Option<Rid>, kind: KeyframeRequestKind) -> Self {
        Self {
            consumer_mid: mid,
            consumer_rid: rid,
            kind,
        }
    }
}

/// Producer-side destination for a resolved keyframe request.
///
/// Local sources are driven by the same worker state. Remote sources are owned
/// by another worker or node and must be reached through source-control
/// messaging.
pub(super) enum ResolvedKeyframeRoute {
    Local {
        source_session_key: TransportSessionKey,
    },
    Remote {
        source: TransportSourceKey,
        source_control: RemoteSourceControl,
    },
}

/// Source and RID keyed keyframe request after route resolution.
///
/// Multiple consumers can ask for the same source during one packet-loop turn.
/// Coalescing keeps duplicate feedback bounded without merging distinct
/// simulcast layers into one source-wide request.
#[derive(Clone, Copy)]
pub(super) struct CoalescedKeyframeRequest {
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
}

/// Resolve and flush every keyframe request staged during the current turn.
///
/// Requests are drained from packet-loop buffers, resolved from consumer-local
/// MID to source transport media id and selected producer RID before any
/// request is dispatched. Missing route state means the route changed before
/// the feedback was flushed and is treated as a benign stale request.
pub(super) fn flush_pending_keyframe_requests(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    flush_pending_keyframe_requests_at(state, metrics, buffers, Instant::now());
}

/// resolves and flushes staged keyframe requests at a supplied time
///
/// tests and benchmarks pass `now` explicitly so route-control throttling stays
/// deterministic
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
    let mut same_request: Option<CoalescedKeyframeRequest> = None;
    for (consumer_session_key, request) in pending_keyframe_requests.drain(..) {
        let Some(target) = state.active_consumer_keyframe_target_for_mid(
            &consumer_session_key,
            request.consumer_mid,
            request.consumer_rid,
        ) else {
            continue;
        };
        let resolved_request = CoalescedKeyframeRequest {
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
    let mut current_request: Option<CoalescedKeyframeRequest> = None;
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

fn compare_keyframe_rids(left: Option<Rid>, right: Option<Rid>) -> Ordering {
    left.as_deref().cmp(&right.as_deref())
}

/// Dispatch one source/RID keyed keyframe request.
///
/// Local producers are marked through the worker's normal keyframe request path.
/// Remote producers pass through route-control de-duplication before the request
/// leaves the worker so repeated feedback does not flood another relay target.
fn flush_coalesced_keyframe_request(
    state: &mut PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    coalesced_request: CoalescedKeyframeRequest,
    now: Instant,
) {
    let Some(route) = resolve_keyframe_route(state, coalesced_request.source_transport_media_id)
    else {
        return;
    };
    match route {
        ResolvedKeyframeRoute::Local { source_session_key } => {
            debug!(
                ?source_session_key,
                source_transport_media_id = ?coalesced_request.source_transport_media_id,
                rid = ?coalesced_request.rid,
                kind = ?coalesced_request.kind,
                "forwarding local keyframe request to source"
            );
            request_keyframe_for_source(
                state,
                metrics,
                &source_session_key,
                coalesced_request.source_transport_media_id,
                coalesced_request.rid,
                coalesced_request.kind,
                now,
            );
        }
        ResolvedKeyframeRoute::Remote {
            source,
            source_control,
        } => {
            match state.route_control.decide_keyframe_request_for_rid(
                coalesced_request.source_transport_media_id,
                coalesced_request.rid,
                now,
            ) {
                KeyframeRequestDecision::Forward => {
                    debug!(
                        source_session_key = ?source.session_key(),
                        source_transport_media_id = ?coalesced_request.source_transport_media_id,
                        rid = ?coalesced_request.rid,
                        kind = ?coalesced_request.kind,
                        "forwarding remote keyframe request to source control"
                    );
                    source_control.request_keyframe(
                        &source,
                        coalesced_request.rid,
                        coalesced_request.kind,
                    );
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
                }
                KeyframeRequestDecision::Absorb => {
                    debug!(
                        source_session_key = ?source.session_key(),
                        source_transport_media_id = ?coalesced_request.source_transport_media_id,
                        rid = ?coalesced_request.rid,
                        kind = ?coalesced_request.kind,
                        "absorbed duplicate keyframe request"
                    );
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
                }
            }
        }
    }
}

/// Resolve a producer transport media id to the control path that owns it.
///
/// The source can be local to this worker through `mid_registry`, or remote
/// through the remote source registry populated by relay setup. Missing entries
/// mean the route changed before the feedback was flushed.
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
        .remote_source_registration(source_transport_media_id)
        .cloned()
        .map(|remote_source| ResolvedKeyframeRoute::Remote {
            source: remote_source.source().clone(),
            source_control: remote_source.source_control().clone(),
        })
}
