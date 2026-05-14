//! Command response adapters for worker-local media route control.

use std::time::Instant;

use tokio::sync::oneshot;

use super::{
    super::{keyframe::worker_request_consumer_keyframe, types::ConsumerPacketGateRequest},
    remote_source, routes,
};
use crate::runtime::{
    media_transport::{
        TransportAdapterError, TransportMediaId, TransportResult, TransportSessionKey,
    },
    metrics::RuntimeMetrics,
    rtc_engine::{
        commands::ConsumerPacketGateCommand,
        relay_registry::{RelayTargetId, RelayTargetTransport},
        route_control::PacketLayerGate,
        state::PacketLoopState,
    },
};

pub fn respond_set_producer_active(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(routes::worker_set_producer_active(
        state,
        session_key,
        transport_media_id,
        active,
    ));
}

pub fn respond_set_consumer_active(
    state: &mut PacketLoopState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(routes::worker_set_consumer_active(
        state,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        active,
    ));
}

/// Command adapter for receiver-driven layer updates.
///
/// The dispatcher already owns the mutable worker state. This wrapper keeps the
/// oneshot response boundary at the command edge while the worker function
/// revalidates the route before mutating packet-gate state.
pub fn respond_set_consumer_packet_gate(
    state: &mut PacketLoopState,
    request: ConsumerPacketGateRequest<'_>,
    now: Instant,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(routes::worker_set_consumer_packet_gate(state, request, now));
}

pub fn respond_set_consumer_packet_gates(
    state: &mut PacketLoopState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    updates: Vec<ConsumerPacketGateCommand>,
    now: Instant,
    response: oneshot::Sender<TransportResult<Vec<TransportResult<()>>>>,
) {
    let _ = response.send(Ok(routes::worker_set_consumer_packet_gates(
        state,
        source_session_key,
        source_transport_media_id,
        updates,
        now,
    )));
}

pub fn respond_request_consumer_keyframe(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_request_consumer_keyframe(
        state,
        metrics,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
    ));
}

pub fn respond_add_relay_target(
    state: &mut PacketLoopState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    target: RelayTargetTransport,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(remote_source::worker_add_relay_target(
        state,
        source_session_key,
        source_transport_media_id,
        target_id,
        target,
    ));
}

pub fn respond_remove_relay_target(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    remote_source::remove_relay_target(state, source_transport_media_id, target_id);
    let _ = response.send(Ok(()));
}

pub fn respond_set_relay_target_active(
    state: &mut PacketLoopState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(remote_source::worker_set_relay_target_active(
        state,
        source_session_key,
        source_transport_media_id,
        target_id,
        active,
    ));
}

pub fn respond_set_remote_source_packet_gate(
    state: &mut PacketLoopState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    remote_source::set_remote_source_packet_gate(
        state,
        source_session_key,
        source_transport_media_id,
        target_id,
        packet_gate,
    );
}
