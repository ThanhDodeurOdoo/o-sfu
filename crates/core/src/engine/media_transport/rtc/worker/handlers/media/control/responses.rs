//! Command response adapters for worker-local media route control.

use std::time::Instant;

use tokio::sync::oneshot;

use super::{
    super::keyframe::{worker_request_consumer_kf, worker_request_remote_kf},
    routes,
};
use crate::engine::{
    media_transport::{
        TransportAdapterError, TransportResult, TransportSourceKey,
        rtc::{
            commands::{ConsumerPacketGateCommand, RouteControlRequest},
            state::PacketLoopState,
        },
    },
    metrics::RuntimeMetrics,
};

pub fn apply_route_control_request(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    request: RouteControlRequest,
    now: Instant,
    response: Option<oneshot::Sender<Result<(), TransportAdapterError>>>,
) {
    let result = match request {
        RouteControlRequest::SetProducerActive { source, active } => {
            routes::worker_set_producer_active(state, &source, active)
        }
        RouteControlRequest::SetConsumerActive { route, active } => {
            routes::worker_set_consumer_active(state, &route, active)
        }
        RouteControlRequest::SetConsumerPacketGate { route, packet_gate } => {
            routes::worker_set_consumer_pkt_gate(state, &route, packet_gate, now)
        }
        RouteControlRequest::RequestConsumerKeyframe { route } => {
            worker_request_consumer_kf(state, metrics, &route)
        }
        RouteControlRequest::AddRelayTarget {
            source,
            target_id,
            target,
        } => routes::worker_add_relay_target(state, &source, target_id, target),
        RouteControlRequest::RemoveRelayTarget {
            src_media,
            target_id,
        } => {
            state.routes.remove_relay_target(src_media, target_id);
            Ok(())
        }
        RouteControlRequest::SetRelayTargetActive {
            source,
            target_id,
            active,
        } => routes::worker_set_relay_target_active(state, &source, target_id, active),
        RouteControlRequest::RequestRemoteKeyframe {
            source,
            target_id,
            rid,
            kind,
        } => {
            worker_request_remote_kf(state, metrics, &source, target_id, rid, kind);
            Ok(())
        }
        RouteControlRequest::SetRemoteSourcePacketGate {
            source,
            target_id,
            packet_gate,
        } => {
            routes::set_remote_src_pkt_gate(state, &source, target_id, packet_gate);
            Ok(())
        }
    };
    if let Some(response) = response {
        let _ = response.send(result);
    }
}

pub fn respond_set_consumer_pkt_gates(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    updates: Vec<ConsumerPacketGateCommand>,
    now: Instant,
    response: oneshot::Sender<TransportResult<Vec<TransportResult<()>>>>,
) {
    let _ = response.send(Ok(routes::worker_set_consumer_pkt_gates(
        state, source, updates, now,
    )));
}
