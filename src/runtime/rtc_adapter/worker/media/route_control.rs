use tokio::sync::oneshot;

use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::super::super::{
    demux::MediaRouteEntry,
    media_registry::RegisteredMediaHandle,
    route_control::{PacketLayerGate, aggregate_packet_gates},
    state::RtcBootstrapState,
};
use super::route_source::ensure_route_source_exists;

pub(crate) fn respond_set_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: Option<PacketLayerGate>,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_source_packet_gate(
        state,
        source_session_key,
        source_transport_media_id,
        packet_gate,
    ));
}

pub(crate) fn respond_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_producer_active(
        state,
        session_key,
        transport_media_id,
        active,
    ));
}

pub(crate) fn respond_set_consumer_active(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_set_consumer_active(
        state,
        consumer_session_key,
        consumer_transport_media_id,
        source_session_key,
        source_transport_media_id,
        active,
    ));
}

// TODO: needs documentation:
pub(crate) fn refresh_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) {
    let local_packet_gate = state
        .media_route_index
        .get(&source_transport_media_id)
        .and_then(local_source_packet_gate);
    state
        .route_control
        .set_local_packet_gate(source_transport_media_id, local_packet_gate.clone());
    if let Some(remote_source_registration) =
        state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration.source_control().set_packet_gate(
            remote_source_registration.source_session_key().clone(),
            source_transport_media_id,
            local_packet_gate.unwrap_or(PacketLayerGate::Block),
        );
    }
}

pub(super) fn local_source_packet_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}

fn worker_set_producer_active(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    match state.mid_registry.get(&transport_media_id.as_u64()) {
        Some(RegisteredMediaHandle::Producer {
            session_key: owner_session_key,
            ..
        }) if owner_session_key == session_key => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    let route_entry = state
        .media_route_index
        .get_mut(&transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    route_entry.source_active = active;
    Ok(())
}

fn worker_set_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: Option<PacketLayerGate>,
) -> Result<(), TransportAdapterError> {
    match state.mid_registry.get(&source_transport_media_id.as_u64()) {
        Some(RegisteredMediaHandle::Producer {
            session_key: owner_session_key,
            ..
        }) if owner_session_key == source_session_key => {}
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            return Err(TransportAdapterError::InvalidInput);
        }
        None => return Err(TransportAdapterError::TransportUnavailable),
    }
    state
        .route_control
        .set_source_packet_gate(source_transport_media_id, packet_gate);
    Ok(())
}

fn worker_set_consumer_active(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    ensure_route_source_exists(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
    )?;
    match state
        .mid_registry
        .get(&consumer_transport_media_id.as_u64())
    {
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
    let route_entry = state
        .media_route_index
        .get_mut(&source_transport_media_id)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let destination = route_entry
        .destinations
        .iter_mut()
        .find(|destination| {
            destination.dest_session == *consumer_session_key
                && destination.dest_transport_media_id == consumer_transport_media_id
        })
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if destination.active == active {
        return Ok(());
    }
    destination.active = active;
    refresh_source_packet_gate(state, source_transport_media_id);
    if let Some(remote_source_registration) =
        state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration
            .source_control()
            .set_route_active(
                remote_source_registration.source_session_key().clone(),
                source_transport_media_id,
                active,
            );
    }
    Ok(())
}
