use str0m::media::Mid;

use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::super::super::{
    commands::RemoteSourceControl, media_registry::RegisteredMediaHandle, state::RtcBootstrapState,
};
use super::types::RouteSourceKind;

pub(super) fn ensure_route_source_registered(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<RemoteSourceControl>,
) -> Result<RouteSourceKind, TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id)?;
        return Ok(RouteSourceKind::Local);
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
) -> Result<RouteSourceKind, TransportAdapterError> {
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id)?;
        return Ok(RouteSourceKind::Local);
    }
    match state.remote_source_registration(source_transport_media_id) {
        Some(registration) if registration.source_session_key() == source_session_key => {
            Ok(RouteSourceKind::Remote)
        }
        Some(_registration) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

pub(super) fn ensure_owned_local_producer_mid(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<Mid, TransportAdapterError> {
    match state.media_handle(source_transport_media_id) {
        Some(RegisteredMediaHandle::Producer { session_key, mid })
            if session_key == source_session_key =>
        {
            Ok(*mid)
        }
        Some(RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. }) => {
            Err(TransportAdapterError::InvalidInput)
        }
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}

pub(super) fn owned_local_producer_mid(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Option<Mid> {
    ensure_owned_local_producer_mid(state, source_session_key, source_transport_media_id).ok()
}
