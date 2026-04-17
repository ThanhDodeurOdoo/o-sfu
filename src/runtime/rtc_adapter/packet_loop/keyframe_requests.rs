use std::{collections::BTreeMap, time::Instant};

use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};

use super::super::{
    commands::RemoteSourceControl,
    media_registry::RegisteredMediaHandle,
    route_control::{KeyframeRequestDecision, coalesce_keyframe_kind},
    state::RtcBootstrapState,
    worker::request_keyframe_for_source,
};
use super::buffers::PacketLoopBuffers;
use crate::runtime::metrics::{RtcRouteControlOutcome, RuntimeMetrics};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

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
enum ResolvedKeyframeRoute {
    Local {
        source_session_key: TransportSessionKey,
    },
    Remote {
        source_session_key: TransportSessionKey,
        source_control: RemoteSourceControl,
    },
}

#[derive(Clone)]
struct CoalescedKeyframeRequest {
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
    let mut coalesced_requests = BTreeMap::new();
    for (consumer_session_key, request) in buffers.pending_keyframe_requests.drain(..) {
        let Some(source_transport_media_id) = state.consumer_source_transport_media_id_for_mid(
            &consumer_session_key,
            request.consumer_mid,
        ) else {
            continue;
        };
        let Some(route) = resolve_keyframe_route(state, source_transport_media_id) else {
            continue;
        };
        coalesced_requests
            .entry(source_transport_media_id)
            .and_modify(|coalesced: &mut CoalescedKeyframeRequest| {
                coalesced.coalesce(request.consumer_rid, request.kind);
            })
            .or_insert_with(|| {
                CoalescedKeyframeRequest::new(
                    source_transport_media_id,
                    route,
                    request.consumer_rid,
                    request.kind,
                )
            });
    }
    let now = Instant::now();
    for coalesced_request in coalesced_requests.into_values() {
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
