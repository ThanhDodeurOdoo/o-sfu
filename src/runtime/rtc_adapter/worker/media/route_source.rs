use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::super::super::{
    media_registry::RegisteredMediaHandle, relay_registry::RelayRegistry,
    route_control::PacketLayerGate, state::RtcBootstrapState,
};
use super::types::RouteSourceKind;

pub(crate) fn respond_set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    active: bool,
) {
    set_remote_source_route_active(
        state,
        relay_registry,
        source_session_key,
        source_transport_media_id,
        target_id,
        active,
    );
}

pub(crate) fn respond_set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    set_remote_source_packet_gate(
        state,
        source_session_key,
        source_transport_media_id,
        target_id,
        packet_gate,
    );
}

pub(super) fn ensure_route_source_registered(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<super::super::super::commands::RemoteSourceControl>,
) -> Result<RouteSourceKind, TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        if let Some(handle) = state.mid_registry.get(&source_transport_media_id.as_u64()) {
            return match handle {
                RegisteredMediaHandle::Producer {
                    session_key: owner_session_key,
                    mid: _mid,
                } if owner_session_key == source_session_key => Ok(RouteSourceKind::Local),
                RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
                    Err(TransportAdapterError::InvalidInput)
                }
            };
        }
        return Err(TransportAdapterError::TransportUnavailable);
    }
    let Some(remote_source_control) = remote_source_control else {
        return Err(TransportAdapterError::InvalidInput);
    };
    state.register_remote_source(
        source_transport_media_id,
        source_session_key,
        remote_source_control,
    )?;
    Ok(RouteSourceKind::Remote)
}

pub(super) fn ensure_route_source_exists(
    state: &RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        if let Some(handle) = state.mid_registry.get(&source_transport_media_id.as_u64()) {
            return match handle {
                RegisteredMediaHandle::Producer {
                    session_key: owner_session_key,
                    ..
                } if owner_session_key == source_session_key => Ok(()),
                RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
                    Err(TransportAdapterError::InvalidInput)
                }
            };
        }
        return Err(TransportAdapterError::TransportUnavailable);
    }
    match state.remote_source_registration(source_transport_media_id) {
        Some(registration) if registration.source_session_key() == source_session_key => Ok(()),
        Some(_registration) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

fn set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    active: bool,
) {
    let Some(handle) = state
        .mid_registry
        .get(&source_transport_media_id.as_u64())
        .cloned()
    else {
        return;
    };
    let RegisteredMediaHandle::Producer { session_key, .. } = handle else {
        return;
    };
    if &session_key != source_session_key {
        return;
    }
    relay_registry.set_source_target_active(source_transport_media_id, target_id, active);
}

fn set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    let Some(handle) = state
        .mid_registry
        .get(&source_transport_media_id.as_u64())
        .cloned()
    else {
        return;
    };
    let RegisteredMediaHandle::Producer { session_key, .. } = handle else {
        return;
    };
    if &session_key != source_session_key {
        return;
    }
    state
        .route_control
        .set_relay_packet_gate(source_transport_media_id, target_id, packet_gate);
}
