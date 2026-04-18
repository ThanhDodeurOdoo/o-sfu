use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::{Direction, MediaKind, Mid};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportMediaId, TransportSessionKey,
};

use super::super::super::{
    commands::{RemoteSourceControl, RemoveMediaOutcome},
    demux::{MediaRouteDestination, MediaRouteEntry},
    media_registry::RegisteredMediaHandle,
    state::{PendingRecvStream, RtcBootstrapState, RtcSessionState},
};
use super::{
    helpers::{
        consumer_packet_gate, primary_encoding_identity, relay_cleanup_for_source, transport_mid,
    },
    ownership::ensure_route_source_registered,
    route_control::refresh_source_packet_gate,
    types::{AddSendMediaRequest, RouteSourceKind},
};

pub(crate) fn respond_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<RemoveMediaOutcome, TransportAdapterError>>,
) {
    let _ = response.send(worker_remove_media(state, session_key, transport_media_id));
}

pub(crate) fn respond_add_recv_media(
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

pub(crate) fn respond_add_send_media(
    state: &mut RtcBootstrapState,
    request: AddSendMediaRequest<'_>,
    response: oneshot::Sender<Result<TransportMediaId, TransportAdapterError>>,
) {
    let _ = response.send(worker_add_send_media(
        state,
        request.consumer_session_key,
        request.media_kind,
        request.source_session_key,
        request.source_transport_media_id,
        request.remote_source_control,
        request.consumer_rtp_parameters,
    ));
}

pub(crate) fn respond_resolve_media_mid(
    state: &RtcBootstrapState,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<Option<String>, TransportAdapterError>>,
) {
    let resolved_mid = state
        .resolve_mid(transport_media_id)
        .map(|mid| mid.to_string());
    let _ = response.send(Ok(resolved_mid));
}

/// Remove one registered transport media handle and reconcile every dependent
/// SDP, route, and remote-source side effect that still points at it.
fn worker_remove_media(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<RemoveMediaOutcome, TransportAdapterError> {
    let Some(handle) = state.media_handle(transport_media_id).cloned() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    let Some(handle) = state.remove_media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
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
            Ok(RemoveMediaOutcome::without_relay_cleanup())
        }
        RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            source_transport_media_id,
        } => {
            let relay_cleanup = relay_cleanup_for_source(state, source_transport_media_id);
            let remote_source_registration = state
                .remote_source_registration(source_transport_media_id)
                .cloned();
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
                    let destination_was_active = route_entry
                        .destinations
                        .get(position)
                        .is_some_and(|destination| destination.active);
                    if destination_was_active
                        && let Some(remote_source_registration) =
                            remote_source_registration.as_ref()
                    {
                        remote_source_registration
                            .source_control()
                            .set_route_active(
                                remote_source_registration.source_session_key().clone(),
                                source_transport_media_id,
                                false,
                            );
                    }
                    route_entry.destinations.remove(position);
                }
                if route_entry.destinations.is_empty() {
                    state.media_route_index.remove(&source_transport_media_id);
                }
            }
            refresh_source_packet_gate(state, source_transport_media_id);
            state.prune_remote_source_if_unrouted(source_transport_media_id);
            state.mark_session_dirty(&session_key);
            relay_cleanup.map_or_else(
                || Ok(RemoveMediaOutcome::without_relay_cleanup()),
                |cleanup| {
                    Ok(RemoveMediaOutcome::with_relay_cleanup(
                        cleanup.source_session_key().clone(),
                        source_transport_media_id,
                    ))
                },
            )
        }
    }
}

fn worker_stage_native_media_removal(
    session_state: &mut RtcSessionState,
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

/// Declare one recv-only media line owned by the publishing session.
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
        media_worker_id = session_key.media_worker_id(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc session for incoming producer RTP"
    );
    Ok(transport_media_id)
}

fn worker_stage_native_recv_media(
    session_state: &mut RtcSessionState,
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
            .insert(mid, PendingRecvStream { ssrc, rid });
    } else {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .remove(&mid);
    }
    Ok(mid)
}

/// Declare one send-only media line for a consumer route and register the
/// corresponding route-source ownership in the worker bootstrap state.
fn worker_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<RemoteSourceControl>,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> Result<TransportMediaId, TransportAdapterError> {
    let route_source = match ensure_route_source_registered(
        state,
        consumer_session_key,
        source_session_key,
        source_transport_media_id,
        remote_source_control,
    ) {
        Ok(route_source) => route_source,
        Err(error) => {
            warn!(
                consumer_session_id = ?consumer_session_key.session_id(),
                consumer_media_worker_id = consumer_session_key.media_worker_id(),
                source_session_id = ?source_session_key.session_id(),
                source_media_worker_id = source_session_key.media_worker_id(),
                ?source_transport_media_id,
                error = ?error,
                "failed to register route source for consumer media"
            );
            return Err(error);
        }
    };
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
            packet_gate: consumer_packet_gate(consumer_rtp_parameters),
        });
    refresh_source_packet_gate(state, source_transport_media_id);
    debug!(
        consumer_session_id = ?consumer_session_key.session_id(),
        consumer_media_worker_id = consumer_session_key.media_worker_id(),
        source_session_id = ?source_session_key.session_id(),
        source_media_worker_id = source_session_key.media_worker_id(),
        ?source_transport_media_id,
        source_route_kind = route_source.label(),
        ?transport_media_id,
        ?media_kind,
        "declared send-only media and registered media route for consumer"
    );
    if matches!(route_source, RouteSourceKind::Remote)
        && let Some(remote_source_registration) =
            state.remote_source_registration(source_transport_media_id)
    {
        remote_source_registration
            .source_control()
            .set_route_active(
                remote_source_registration.source_session_key().clone(),
                source_transport_media_id,
                true,
            );
    }
    Ok(transport_media_id)
}

fn worker_stage_native_send_media(
    session_state: &mut RtcSessionState,
    media_kind: MediaKind,
) -> Result<Mid, TransportAdapterError> {
    if session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
    {
        warn!(
            ?media_kind,
            initial_offer_applied = session_state.sdp_negotiation.initial_offer_applied,
            "cannot stage consumer media while a previous offer is still awaiting answer"
        );
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
    session_state: &mut RtcSessionState,
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
