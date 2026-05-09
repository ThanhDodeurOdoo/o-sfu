//! Route-level keyframe feedback handling.
//!
//! Consumer host sessions emit keyframe requests in consumer-local
//! terms, usually by MID and optional RID. The packet loop must translate that
//! feedback back to the producer source that can satisfy it. This module contain
//! that translation, duplicate coalescing and final dispatch to either a local
//! producer session or a remote source-control handle.
//!
//! Keyframe requests are route-control feedback, not room policy. The room
//! decides which sources and layers a user should receive. This module only
//! makes sure the currently routed producer gets a bounded, source-keyed request
//! stream.

use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};
use tracing::debug;

use super::{
    super::{
        commands::RemoteSourceControl,
        media_registry::RegisteredMediaHandle,
        route_control::{KeyframeRequestDecision, coalesce_keyframe_kind},
    },
    machine::{
        effect::{PacketLoopEffect, PacketLoopEffects, PacketLoopMetricEffect},
        scratch::PacketLoopScratch,
        state::PacketLoopState,
    },
    time::PacketLoopTime,
};
use crate::runtime::{
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::RtcRouteControlOutcome,
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
pub(in crate::runtime::rtc_engine) enum ResolvedKeyframeRoute {
    Local {
        source_session_key: TransportSessionKey,
    },
    Remote {
        source_session_key: TransportSessionKey,
        source_control: RemoteSourceControl,
    },
}

pub(in crate::runtime::rtc_engine) enum ResolvedKeyframeDecision {
    Local {
        source_session_key: TransportSessionKey,
    },
    RemoteForward {
        source_session_key: TransportSessionKey,
        source_control: RemoteSourceControl,
    },
    Absorb {
        source_session_key: TransportSessionKey,
    },
}

impl ResolvedKeyframeRoute {
    pub(in crate::runtime::rtc_engine) fn source_session_key(&self) -> &TransportSessionKey {
        match self {
            Self::Local { source_session_key }
            | Self::Remote {
                source_session_key, ..
            } => source_session_key,
        }
    }
}

/// Source-keyed keyframe request after route resolution.
///
/// Multiple consumers can ask for the same source during one packet-loop turn.
/// Coalescing keeps the request stream proportional to source media instead of
/// fanout count and upgrades request kind when a stronger request is observed.
#[derive(Clone)]
pub(super) struct CoalescedKeyframeRequest {
    pub(super) source_transport_media_id: TransportMediaId,
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
/// Requests are drained from packet-loop scratch, resolved from consumer-local
/// MID to source transport media id, sorted by source and coalesced before any
/// request is dispatched. Missing source state means the route changed before
/// the feedback was flushed and is treated as a benign stale request.
pub(super) fn flush_pending_keyframe_requests(
    state: &mut PacketLoopState,
    effects: &mut PacketLoopEffects,
    scratch: &mut PacketLoopScratch,
    now: PacketLoopTime,
) {
    scratch.rebuild_coalesced_keyframe_requests(|consumer_session_key, request| {
        let source_transport_media_id = state.consumer_source_transport_media_id_for_mid(
            &consumer_session_key,
            request.consumer_mid,
        )?;
        let route = resolve_keyframe_route(state, source_transport_media_id)?;
        Some(CoalescedKeyframeRequest::new(
            source_transport_media_id,
            route,
            request.consumer_rid,
            request.kind,
        ))
    });
    let mut current_request: Option<CoalescedKeyframeRequest> = None;
    for coalesced_request in scratch.drain_coalesced_keyframe_requests() {
        match &mut current_request {
            Some(current)
                if current.source_transport_media_id
                    == coalesced_request.source_transport_media_id =>
            {
                current.coalesce(coalesced_request.rid, coalesced_request.kind);
            }
            Some(_) => {
                if let Some(request) = current_request.take() {
                    request_resolved_keyframe(
                        state,
                        effects,
                        request.source_transport_media_id,
                        request.route,
                        request.rid,
                        request.kind,
                        now,
                    );
                }
                current_request = Some(coalesced_request);
            }
            None => {
                current_request = Some(coalesced_request);
            }
        }
    }
    if let Some(request) = current_request {
        request_resolved_keyframe(
            state,
            effects,
            request.source_transport_media_id,
            request.route,
            request.rid,
            request.kind,
            now,
        );
    }
}

pub(in crate::runtime::rtc_engine) fn request_resolved_keyframe(
    state: &mut PacketLoopState,
    effects: &mut PacketLoopEffects,
    source_transport_media_id: TransportMediaId,
    route: ResolvedKeyframeRoute,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    now: PacketLoopTime,
) {
    match decide_resolved_keyframe_route(state, source_transport_media_id, route, rid, now) {
        ResolvedKeyframeDecision::Local { source_session_key } => {
            debug!(
                ?source_session_key,
                ?source_transport_media_id,
                ?rid,
                ?kind,
                "forwarding local keyframe request to source"
            );
            effects.push(PacketLoopEffect::RequestLocalKeyframe {
                source_session_key,
                source_transport_media_id,
                rid,
                kind,
                now,
            });
        }
        ResolvedKeyframeDecision::RemoteForward {
            source_session_key,
            source_control,
        } => {
            debug!(
                ?source_session_key,
                ?source_transport_media_id,
                ?rid,
                ?kind,
                "forwarding remote keyframe request to source control"
            );
            effects.push(PacketLoopEffect::RequestRemoteKeyframe {
                source_session_key,
                source_transport_media_id,
                source_control,
                rid,
                kind,
            });
            effects.record_metric(PacketLoopMetricEffect::RtcRouteControl(
                RtcRouteControlOutcome::Forwarded,
            ));
        }
        ResolvedKeyframeDecision::Absorb { source_session_key } => {
            debug!(
                ?source_session_key,
                ?source_transport_media_id,
                ?rid,
                ?kind,
                "absorbed duplicate keyframe request"
            );
            effects.record_metric(PacketLoopMetricEffect::RtcRouteControl(
                RtcRouteControlOutcome::Absorbed,
            ));
        }
    }
}

/// Resolve a producer transport media id to the control path that owns it.
///
/// The source can be local to this worker through `mid_registry`, or remote
/// through the remote source registry populated by relay setup. Missing entries
/// mean the route changed before the feedback was flushed.
pub(in crate::runtime::rtc_engine) fn resolve_keyframe_route(
    state: &PacketLoopState,
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

pub(in crate::runtime::rtc_engine) fn decide_keyframe_route(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    now: PacketLoopTime,
) -> Option<ResolvedKeyframeDecision> {
    let route = resolve_keyframe_route(state, source_transport_media_id)?;
    Some(decide_resolved_keyframe_route(
        state,
        source_transport_media_id,
        route,
        rid,
        now,
    ))
}

fn decide_resolved_keyframe_route(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    route: ResolvedKeyframeRoute,
    rid: Option<Rid>,
    now: PacketLoopTime,
) -> ResolvedKeyframeDecision {
    match route {
        ResolvedKeyframeRoute::Local { source_session_key } => {
            ResolvedKeyframeDecision::Local { source_session_key }
        }
        ResolvedKeyframeRoute::Remote {
            source_session_key,
            source_control,
        } => match state.route_control.decide_keyframe_request_for_rid(
            source_transport_media_id,
            rid,
            now,
        ) {
            KeyframeRequestDecision::Forward => ResolvedKeyframeDecision::RemoteForward {
                source_session_key,
                source_control,
            },
            KeyframeRequestDecision::Absorb => {
                ResolvedKeyframeDecision::Absorb { source_session_key }
            }
        },
    }
}
