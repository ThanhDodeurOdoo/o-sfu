use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::{Direction, MediaKind, Mid, Rid};
use str0m::rtp::Ssrc;
use tokio::sync::oneshot;
use tracing::debug;

use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::super::{
    demux::{MediaRouteDestination, MediaRouteEntry},
    media_registry::RegisteredMediaHandle,
    state::RtcBootstrapState,
};

enum RouteSourceKind {
    Local { source_mid: Mid },
    Remote,
}

pub(super) fn respond_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_remove_media(state, session_key, transport_media_id));
}

pub(super) fn respond_add_recv_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_recv_media(
        state,
        session_key,
        media_kind,
        rtp_parameters,
    ));
}

pub(super) fn respond_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    consumer_rtp_parameters: &RouterRtpParameters,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_send_media(
        state,
        consumer_session_key,
        media_kind,
        source_session_key,
        source_transport_media_id,
        consumer_rtp_parameters,
    ));
}

pub(super) fn respond_set_producer_active(
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

pub(super) fn respond_set_consumer_active(
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

fn worker_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let Some(handle) = state.remove_media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    match handle {
        RegisteredMediaHandle::Producer { session_key, mid } => {
            let should_remove_media = !state.session_has_mid(&session_key, mid);
            if let Some(session_state) = state.sessions.get_mut(&session_key) {
                session_state
                    .sdp_negotiation
                    .negotiated_producer_parameters
                    .remove(&mid);
            }
            if let Some(session_state) = state.sessions.get_mut(&session_key)
                && should_remove_media
            {
                if session_state.sdp_negotiation.initial_offer_applied {
                    worker_stage_native_media_removal(session_state, mid)?;
                } else {
                    session_state.rtc.direct_api().remove_media(mid);
                }
            }
            state.media_route_index.remove(&transport_media_id);
            state.mark_session_dirty(&session_key);
        }
        RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } => {
            let should_remove_media = !state.session_has_mid(&session_key, mid);
            if let Some(session_state) = state.sessions.get_mut(&session_key)
                && should_remove_media
            {
                if session_state.sdp_negotiation.initial_offer_applied {
                    worker_stage_native_media_removal(session_state, mid)?;
                } else {
                    session_state.rtc.direct_api().remove_media(mid);
                }
            }
            if let Some(route_entry) = state.media_route_index.get_mut(&source_transport_media_id) {
                if let Some(position) = route_entry.destinations.iter().position(|destination| {
                    destination.dest_session == session_key
                        && destination.dest_transport_media_id == transport_media_id
                }) {
                    route_entry.destinations.remove(position);
                }
                if route_entry.destinations.is_empty() {
                    state.media_route_index.remove(&source_transport_media_id);
                }
            }
            state.prune_remote_source_if_unrouted(source_transport_media_id);
            state.mark_session_dirty(&session_key);
        }
    }
    Ok(())
}

fn worker_stage_native_media_removal(
    session_state: &mut super::super::state::RtcSessionState,
    mid: Mid,
) -> Result<(), TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        session_state
            .sdp_negotiation
            .queued_removal_mids
            .insert(mid);
        return Ok(());
    }
    if session_state.rtc.media(mid).is_none() {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    sdp_api.set_direction(mid, Direction::Inactive);
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    session_state
        .sdp_negotiation
        .queued_removal_mids
        .remove(&mid);
    Ok(())
}

fn worker_add_recv_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_recv_media(session_state, media_kind, rtp_parameters)?
    } else {
        let mid = transport_mid(rtp_parameters).unwrap_or_default();
        let has_media = session_state.rtc.media(mid).is_some();
        {
            let mut api = session_state.rtc.direct_api();
            if !has_media {
                api.declare_media(mid, media_kind);
            }
            if let Some((ssrc, rid)) = primary_encoding_identity(rtp_parameters) {
                api.expect_stream_rx(ssrc, None, mid, rid);
            }
        }
        state.mark_session_dirty(session_key);
        mid
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid,
    });
    debug!(
        session_id = ?session_key.session_id(),
        channel_runtime_id = session_key.channel_runtime_id(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc session for incoming producer RTP"
    );
    Ok(transport_media_id)
}

fn worker_stage_native_recv_media(
    session_state: &mut super::super::state::RtcSessionState,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Result<Mid, TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    let mid = sdp_api.add_media(media_kind, Direction::RecvOnly, None, None, None);
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    if let Some((ssrc, rid)) = primary_encoding_identity(rtp_parameters) {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .insert(mid, super::super::state::PendingRecvStream { ssrc, rid });
    } else {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .remove(&mid);
    }
    Ok(mid)
}

fn worker_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let route_source = ensure_route_source_registered(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
    )?;
    let Some(session_state) = state.sessions.get_mut(consumer_session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_send_media(session_state, media_kind)?
    } else {
        let mid = transport_mid(consumer_rtp_parameters).unwrap_or_default();
        declare_direct_send_media(session_state, mid, media_kind, consumer_rtp_parameters);
        state.mark_session_dirty(consumer_session_key);
        mid
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session_key.clone(),
        mid,
        source_transport_media_id,
    });
    state
        .media_route_index
        .entry(source_transport_media_id)
        .or_insert_with(|| MediaRouteEntry {
            source_active: true,
            destinations: Vec::new(),
        })
        .destinations
        .push(MediaRouteDestination {
            dest_session: consumer_session_key.clone(),
            dest_transport_media_id: transport_media_id,
            dest_mid: mid,
            active: true,
        });
    debug!(
        consumer_session_id = ?consumer_session_key.session_id(),
        consumer_channel_runtime_id = consumer_session_key.channel_runtime_id(),
        source_session_id = ?source_session_key.session_id(),
        source_channel_runtime_id = source_session_key.channel_runtime_id(),
        ?source_transport_media_id,
        source_mid = ?route_source.source_mid(),
        source_route_kind = route_source.label(),
        ?transport_media_id,
        ?media_kind,
        "declared send-only media and registered media route for consumer"
    );
    Ok(transport_media_id)
}

fn worker_stage_native_send_media(
    session_state: &mut super::super::state::RtcSessionState,
    media_kind: MediaKind,
) -> Result<Mid, TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    let mid = sdp_api.add_media(media_kind, Direction::SendOnly, None, None, None);
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    Ok(mid)
}

fn declare_direct_send_media(
    session_state: &mut super::super::state::RtcSessionState,
    mid: Mid,
    media_kind: MediaKind,
    consumer_rtp_parameters: &RouterRtpParameters,
) {
    let has_media = session_state.rtc.media(mid).is_some();
    let mut api = session_state.rtc.direct_api();
    if !has_media {
        api.declare_media(mid, media_kind);
    }
    let (ssrc, rid) = primary_encoding_identity(consumer_rtp_parameters)
        .unwrap_or_else(|| (api.new_ssrc(), None));
    api.declare_stream_tx(ssrc, None, mid, rid);
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
    destination.active = active;
    Ok(())
}

fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
}

fn primary_encoding_identity(rtp_parameters: &RouterRtpParameters) -> Option<(Ssrc, Option<Rid>)> {
    let encoding = rtp_parameters
        .encodings()
        .find(|encoding| encoding.ssrc().is_some() || encoding.rid().is_some())?;
    let ssrc = encoding.ssrc().map(Ssrc::from)?;
    let rid = encoding.rid().map(Into::into);
    Some((ssrc, rid))
}

impl RouteSourceKind {
    fn source_mid(&self) -> Option<Mid> {
        match self {
            Self::Local { source_mid } => Some(*source_mid),
            Self::Remote => None,
        }
    }

    const fn label(&self) -> &'static str {
        match self {
            Self::Local { .. } => "local",
            Self::Remote => "remote",
        }
    }
}

fn ensure_route_source_registered(
    state: &mut RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<RouteSourceKind, TransportAdapterError> {
    if let Some(handle) = state.mid_registry.get(&source_transport_media_id.as_u64()) {
        return match handle {
            RegisteredMediaHandle::Producer {
                session_key: owner_session_key,
                mid,
            } if owner_session_key == source_session_key => Ok(RouteSourceKind::Local {
                source_mid: *mid,
            }),
            RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
                Err(TransportAdapterError::InvalidInput)
            }
        };
    }
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    state.register_remote_source(source_transport_media_id, source_session_key)?;
    Ok(RouteSourceKind::Remote)
}

fn ensure_route_source_exists(
    state: &RtcBootstrapState,
    route_owner_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
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
    if source_session_key.media_worker_id() == route_owner_session_key.media_worker_id() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    match state.remote_source_registration(source_transport_media_id) {
        Some(registration) if registration.source_session_key() == source_session_key => Ok(()),
        Some(_registration) => Err(TransportAdapterError::InvalidInput),
        None => Err(TransportAdapterError::TransportUnavailable),
    }
}
