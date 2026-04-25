//! Worker-local media lifecycle for one RTC shard.
//!
//! This module owns producer and consumer media declaration plus transport-handle
//! teardown inside `RtcBootstrapState`. Route ownership and relay traking
//! stay in `control.rs`, while offer/answer transitions remain in
//! `worker/negotiation.rs`.
//!
//! The helpers here rely on two invariants:
//! - worker commands are serialized through one mutable `RtcBootstrapState`
//! - the signaling edge obeys the one-outstanding-offer rule and does not try
//!   to hand out a second local offer while the previous one still awaits an
//!   answer

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::{
    bwe::Bitrate,
    media::{Direction, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tracing::{debug, warn};

use super::{
    super::super::{
        bitrate::RtcBitrateState,
        commands::{RemoteSourceControl, RemoveMediaOutcome, RtcWorkerResponse},
        local_send_rewrite::forget_transport_media_rewrites,
        media_registry::RegisteredMediaHandle,
        sdp_simulcast,
        state::{PendingRecvStream, RtcBootstrapState, RtcSessionState},
    },
    control::{ensure_route_source_registered, register_consumer_route, remove_consumer_route},
    types::AddSendMediaRequest,
};
use crate::runtime::transport_adapter::{
    SessionUploadKind, SessionUploadSlot, TransportAdapterError, TransportMediaId, TransportResult,
    TransportSessionKey,
};

pub(crate) fn respond_remove_media(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: RtcWorkerResponse<RemoveMediaOutcome>,
) {
    let _ = response.send(worker_remove_media(
        state,
        bitrate_state,
        session_key,
        transport_media_id,
    ));
}

pub(crate) fn respond_add_recv_media(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    max_bitrate_in_bps: u64,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    response: RtcWorkerResponse<TransportMediaId>,
) {
    let _ = response.send(worker_add_recv_media(
        state,
        bitrate_state,
        max_bitrate_in_bps,
        session_key,
        media_kind,
        rtp_parameters,
    ));
}

pub(crate) fn respond_add_send_media(
    state: &mut RtcBootstrapState,
    request: AddSendMediaRequest<'_>,
    response: RtcWorkerResponse<TransportMediaId>,
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
    response: RtcWorkerResponse<Option<String>>,
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
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<RemoveMediaOutcome, TransportAdapterError> {
    let Some(handle) = state.media_handle(transport_media_id).cloned() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    stage_last_mid_removal_before_unregistering_handle(state, transport_media_id, &handle)?;
    let Some(handle) = state.remove_media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    match handle {
        RegisteredMediaHandle::Producer { session_key, mid } => {
            if let Ok(mut bitrate) = bitrate_state.lock() {
                bitrate.remove_incoming_media(&session_key, transport_media_id);
            }
            if let Some(session_state) = state.sessions.get_mut(&session_key) {
                session_state
                    .sdp_negotiation
                    .negotiated_producer_parameters
                    .remove(&mid);
            }
            state.media_route_index.remove(&transport_media_id);
            state.mark_session_dirty(&session_key);
            Ok(RemoveMediaOutcome::without_relay_cleanup())
        }
        RegisteredMediaHandle::Consumer {
            session_key,
            source_transport_media_id,
            ..
        } => {
            if let Some(session_state) = state.sessions.get_mut(&session_key) {
                forget_transport_media_rewrites(
                    &mut session_state.local_send_rewrites,
                    transport_media_id,
                );
            }
            let relay_cleanup = remove_consumer_route(
                state,
                &session_key,
                transport_media_id,
                source_transport_media_id,
            );
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

/// Stage or apply removal before the public transport-handle registry changes.
///
/// If removal cannot be represented in the session's live or staged SDP, the
/// registry entry must remain intact so ownership and route bookeeping do not
/// drift away from the RTC state
fn stage_last_mid_removal_before_unregistering_handle(
    state: &mut RtcBootstrapState,
    transport_media_id: TransportMediaId,
    handle: &RegisteredMediaHandle,
) -> Result<(), TransportAdapterError> {
    if session_has_other_mid_user(
        state,
        handle.session_key(),
        handle.mid(),
        transport_media_id,
    ) {
        return Ok(());
    }
    let session_state = state
        .sessions
        .get_mut(handle.session_key())
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_media_removal(session_state, handle.mid())
    } else {
        session_state.rtc.direct_api().remove_media(handle.mid());
        Ok(())
    }
}

fn session_has_other_mid_user(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    mid: Mid,
    excluded_transport_media_id: TransportMediaId,
) -> bool {
    state.mid_registry.iter().any(|(raw_id, handle)| {
        *raw_id != excluded_transport_media_id.as_u64()
            && handle.session_key() == session_key
            && handle.mid() == mid
    })
}

/// Returns whether the shard already handed out a local offer and is still
/// waiting for the macthing answer. That state accepts queued removals, but it
/// must reject new additions that would need a second concurrent offer
fn offer_is_awaiting_answer(session_state: &RtcSessionState) -> bool {
    session_state.sdp_negotiation.pending_offer.is_some()
        && session_state.sdp_negotiation.staged_offer_sdp.is_none()
}

/// "Removal" for negotiated media means preserving the existing MID and
/// disabling the m-section with `inactive`, not rejecting it out of the SDP.
///
/// If an earlier offer is already in flight, removal is deferred into the next
/// follow-up offer so the worker keeps the one-outstanding-offer contract.
fn worker_stage_native_media_removal(
    session_state: &mut RtcSessionState,
    mid: Mid,
) -> Result<(), TransportAdapterError> {
    if offer_is_awaiting_answer(session_state) {
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
        .staged_offer_upload_slots
        .clear();
    session_state
        .sdp_negotiation
        .queued_removal_mids
        .remove(&mid);
    Ok(())
}

/// Declare one recv-only media line owned by the publishing session.
///
/// Before the first answer lands, the RTC state can be updated directly because
/// there is no committed negotiated description to keep in sync yet. After that
/// point every addition must stage the next renegotiation offer first.
fn worker_add_recv_media(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    max_bitrate_in_bps: u64,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> TransportResult<TransportMediaId> {
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
            for (ssrc, rid) in recv_encoding_identities(rtp_parameters) {
                api.expect_stream_rx(ssrc, None, mid, rid);
                if let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) {
                    stream_rx.request_remb(Bitrate::bps(max_bitrate_in_bps));
                }
                #[cfg(test)]
                {
                    session_state.max_bitrate_in_bps = Some(max_bitrate_in_bps);
                }
            }
        }
        state.mark_session_dirty(session_key);
        mid
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid,
    });
    if let Ok(mut bitrate) = bitrate_state.lock() {
        let counter =
            bitrate.register_incoming_media(session_key, transport_media_id, Instant::now());
        state.register_incoming_bitrate_counter(transport_media_id, counter);
    }
    debug!(
        session_id = ?session_key.session_id(),
        media_worker_id = session_key.media_worker_id(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc session for incoming producer RTP"
    );
    Ok(transport_media_id)
}

/// Stage a producer-side recv-only media section inside the next local offer.
///
/// The pending receive identity is recorded separately so the worker can bind
/// the concrete SSRC only once the remote answer commits the negotiation step.
fn worker_stage_native_recv_media(
    session_state: &mut RtcSessionState,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> TransportResult<Mid> {
    if offer_is_awaiting_answer(session_state) {
        return Err(TransportAdapterError::InvalidInput);
    }

    let existing_pending_offer = session_state.sdp_negotiation.pending_offer.take();
    let mut sdp_api = session_state.rtc.sdp_api();
    if let Some(pending_offer) = existing_pending_offer {
        sdp_api.merge(pending_offer);
    }
    let mid = sdp_api.add_media(
        media_kind,
        Direction::RecvOnly,
        None,
        None,
        sdp_simulcast::publish_recv_simulcast(media_kind, rtp_parameters),
    );
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    session_state.sdp_negotiation.staged_offer_upload_slots =
        vec![upload_slot(mid, media_kind, rtp_parameters)];
    let pending_streams = recv_encoding_identities(rtp_parameters)
        .into_iter()
        .map(|(ssrc, rid)| PendingRecvStream { ssrc, rid })
        .collect::<Vec<_>>();
    if pending_streams.is_empty() {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .remove(&mid);
    } else {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .insert(mid, pending_streams);
    }
    Ok(mid)
}

/// Declare one send-only media line for a consumer route and register the
/// corresponding route-source ownership in the worker bootstrap state.
///
/// Remote-source registration, consumer-media declaration, and route creation
/// form one logical edge. If the consumer session is gone or media staging
/// fails, any provisional remote-source registration is restored before the
/// error escapes.
fn worker_add_send_media(
    state: &mut RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    media_kind: MediaKind,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    remote_source_control: Option<RemoteSourceControl>,
    consumer_rtp_parameters: &RouterRtpParameters,
) -> TransportResult<TransportMediaId> {
    let previous_remote_source_registration = (source_session_key.media_worker_id()
        != consumer_session_key.media_worker_id())
    .then(|| {
        state
            .remote_source_registration(source_transport_media_id)
            .cloned()
    })
    .flatten();
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
    let rollback_remote_source_registration = |state: &mut RtcBootstrapState| {
        if matches!(route_source, super::types::RouteSourceKind::Remote) {
            state.restore_remote_source_registration(
                source_transport_media_id,
                previous_remote_source_registration.clone(),
            );
        }
    };
    let Some(session_state) = state.sessions.get_mut(consumer_session_key) else {
        rollback_remote_source_registration(state);
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        match worker_stage_native_send_media(session_state, media_kind) {
            Ok(mid) => mid,
            Err(error) => {
                let _ = session_state;
                rollback_remote_source_registration(state);
                return Err(error);
            }
        }
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
    register_consumer_route(
        state,
        consumer_session_key,
        transport_media_id,
        mid,
        source_transport_media_id,
        route_source,
        consumer_rtp_parameters,
    );
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
    Ok(transport_media_id)
}

/// Stage one send-only consumer media section in the next local offer.
///
/// Additions are rejected while another offer already await an answer because
/// a new MID canot be committed speculatively against an unresolved remote
/// description.
fn worker_stage_native_send_media(
    session_state: &mut RtcSessionState,
    media_kind: MediaKind,
) -> TransportResult<Mid> {
    if offer_is_awaiting_answer(session_state) {
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
    session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .clear();
    Ok(mid)
}

/// Direct send declaration needs a concrete SSRC in the live RTC state. When
/// the negotiated parameters do not provide one yet, the worker allocates a
/// shard-local SSRC and treats any RID as metadata on that stream identity.
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

fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
}

/// Return the first encoding identity that can be bound directly in the live
/// RTC state.
///
/// RID-only encodings are intentionally ignored here because they are negotiable SDP
/// metadata, but they do not identify a concrete inbound or outbound RTP stream
/// until an SSRC exists.
fn primary_encoding_identity(rtp_parameters: &RouterRtpParameters) -> Option<(Ssrc, Option<Rid>)> {
    let encoding = rtp_parameters
        .encodings()
        .find(|encoding| encoding.ssrc().is_some() || encoding.rid().is_some())?;
    let ssrc = encoding.ssrc().map(Ssrc::from)?;
    let rid = encoding.rid().map(Into::into);
    Some((ssrc, rid))
}

fn recv_encoding_identities(rtp_parameters: &RouterRtpParameters) -> Vec<(Ssrc, Option<Rid>)> {
    rtp_parameters
        .encodings()
        .filter_map(|encoding| {
            let ssrc = encoding.ssrc().map(Ssrc::from)?;
            Some((ssrc, encoding.rid().map(Into::into)))
        })
        .collect()
}

fn upload_slot(
    mid: Mid,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> SessionUploadSlot {
    SessionUploadSlot {
        mid: mid.to_string(),
        kind: upload_kind(media_kind),
        codecs: upload_codecs(rtp_parameters),
        simulcast_encodings: sdp_simulcast::publish_upload_encodings(media_kind, rtp_parameters),
    }
}

fn upload_kind(media_kind: MediaKind) -> SessionUploadKind {
    if media_kind.is_video() {
        SessionUploadKind::Video
    } else {
        SessionUploadKind::Audio
    }
}

fn upload_codecs(rtp_parameters: &RouterRtpParameters) -> Vec<String> {
    let mut codecs = Vec::new();
    for format in rtp_parameters.formats() {
        let codec = format.codec_name();
        if !codecs
            .iter()
            .any(|existing: &String| existing.as_str() == codec)
        {
            codecs.push(codec.to_owned());
        }
    }
    codecs
}
