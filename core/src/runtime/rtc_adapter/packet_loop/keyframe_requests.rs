//! Route-level keyframe feedback handling.
//!
//! # Boundary role
//!
//! Consumer `str0m::Rtc` instances emit keyframe requests in consumer-local
//! terms, usually by MID and optional RID. The packet loop must translate that
//! feedback back to the producer source that can satisfy it. This module owns
//! that translation, duplicate coalescing and final dispatch to either a local
//! producer session or a remote source-control handle.
//!
//! Keyframe requests are route-control feedback, not room policy. The room
//! decides which sources and layers a user should receive. This module only
//! makes sure the currently routed producer gets a bounded, source-keyed request
//! stream.

use std::time::Instant;

use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};
use tracing::debug;

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

/// Keyframe feedback emitted by one consumer session before producer lookup.
///
/// The MID is the consumer-local media identity. It must be resolved through
/// worker route state before the packet loop knows which producer source owns
/// the requested media.
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

/// Producer-side destination for a resolved keyframe request.
///
/// Local sources are driven by the same worker state. Remote sources are owned
/// by another worker or node and must be reached through source-control
/// messaging.
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

/// Source-keyed keyframe request after route resolution.
///
/// Multiple consumers can ask for the same source during one packet-loop turn.
/// Coalescing keeps the request stream proportional to source media instead of
/// fanout count and upgrades request kind when a stronger request is observed.
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

/// Resolve and flush every keyframe request staged during the current turn.
///
/// Requests are drained from packet-loop buffers, resolved from consumer-local
/// MID to source transport media id, sorted by source and coalesced before any
/// request is dispatched. Missing source state means the route changed before
/// the feedback was flushed and is treated as a benign stale request.
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

/// Dispatch one source-keyed keyframe request.
///
/// Local producers are marked through the worker's normal keyframe request path.
/// Remote producers pass through route-control de-duplication before the request
/// leaves the worker so repeated feedback does not flood another relay target.
fn flush_coalesced_keyframe_request(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    coalesced_request: CoalescedKeyframeRequest,
    now: Instant,
) {
    match coalesced_request.route {
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
            source_session_key,
            source_control,
        } => {
            match state.route_control.decide_keyframe_request_for_rid(
                coalesced_request.source_transport_media_id,
                coalesced_request.rid,
                now,
            ) {
                KeyframeRequestDecision::Forward => {
                    debug!(
                        ?source_session_key,
                        source_transport_media_id = ?coalesced_request.source_transport_media_id,
                        rid = ?coalesced_request.rid,
                        kind = ?coalesced_request.kind,
                        "forwarding remote keyframe request to source control"
                    );
                    source_control.request_keyframe(
                        source_session_key,
                        coalesced_request.source_transport_media_id,
                        coalesced_request.rid,
                        coalesced_request.kind,
                    );
                    metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
                }
                KeyframeRequestDecision::Absorb => {
                    debug!(
                        ?source_session_key,
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
