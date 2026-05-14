//! Worker-local media lifecycle for one RTC worker.
//!
//! This module contain producer and consumer media declaration plus transport-handle
//! teardown inside `PacketLoopState`. Route ownership and relay tracking
//! stay in `control/`, while offer/answer transitions remain in
//! `worker/negotiation.rs`.
//!
//! The helpers here rely on two invariants:
//! - worker commands are serialized through one mutable `PacketLoopState`
//! - the signaling edge obeys the one-outstanding-offer rule and does not try
//!   to hand out a second local offer while the previous one still awaits an
//!   answer

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::{
    bwe::Bitrate as Str0mBitrate,
    media::{Direction, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tracing::{debug, warn};

use super::{
    super::{
        super::{
            bitrate::BitrateRegistry,
            commands::RtcWorkerResponse,
            local_send_rewrite::forget_transport_media_streams,
            media_registry::{
                DecoderRefreshCodec, RegisteredMediaHandle, RemoteSourceRegistration,
            },
            simulcast,
            state::{PacketLoopState, PendingRecvStream, RtcSessionState},
        },
        negotiation,
    },
    control::{
        ConsumerRouteRegistration, ensure_route_source_registered, register_consumer_route,
        remove_consumer_route,
    },
    types::{AddSendMediaRequest, RouteSourceKind},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, VideoBitrateLimits,
    runtime::media_transport::{
        SessionUploadSlot, TransportAdapterError, TransportMediaId, TransportResult,
        TransportSessionKey,
    },
};

#[derive(Clone, Copy)]
pub struct RecvMediaPolicy {
    pub max_bitrate_in: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub codec_flags: MediaCodecFlags,
    pub codec_preferences: CodecPreferences,
}

#[derive(Clone)]
struct RemoteSourceRollback {
    is_remote_source: bool,
    source_transport_media_id: TransportMediaId,
    previous_registration: Option<RemoteSourceRegistration>,
    previous_decoder_refresh_codec: Option<DecoderRefreshCodec>,
}

impl RemoteSourceRollback {
    fn capture(
        state: &PacketLoopState,
        is_remote_source: bool,
        source_transport_media_id: TransportMediaId,
    ) -> Self {
        let previous_registration = is_remote_source
            .then(|| {
                state
                    .remote_source_registration(source_transport_media_id)
                    .cloned()
            })
            .flatten();
        let previous_decoder_refresh_codec = is_remote_source
            .then(|| state.source_decoder_refresh_codec(source_transport_media_id))
            .flatten();
        Self {
            is_remote_source,
            source_transport_media_id,
            previous_registration,
            previous_decoder_refresh_codec,
        }
    }

    fn rollback(&self, state: &mut PacketLoopState) {
        if !self.is_remote_source {
            return;
        }
        state.restore_remote_source_registration(
            self.source_transport_media_id,
            self.previous_registration.clone(),
        );
        state.restore_source_decoder_refresh_codec(
            self.source_transport_media_id,
            self.previous_decoder_refresh_codec,
        );
    }
}

pub fn respond_remove_media(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: RtcWorkerResponse<()>,
) {
    let _ = response.send(worker_remove_media(
        state,
        bitrate_registry,
        session_key,
        transport_media_id,
    ));
}

pub fn respond_add_recv_media(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    policy: RecvMediaPolicy,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    response: RtcWorkerResponse<TransportMediaId>,
) {
    let _ = response.send(worker_add_recv_media(
        state,
        bitrate_registry,
        policy,
        session_key,
        media_kind,
        rtp_parameters,
    ));
}

pub fn respond_add_send_media(
    state: &mut PacketLoopState,
    request: AddSendMediaRequest<'_>,
    now: Instant,
    response: RtcWorkerResponse<TransportMediaId>,
) {
    let _ = response.send(worker_add_send_media(state, request, now));
}

pub fn respond_resolve_media_mid(
    state: &PacketLoopState,
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
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let Some(handle) = state.media_handle(transport_media_id).cloned() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if handle.session_key() != session_key {
        return Err(TransportAdapterError::InvalidInput);
    }
    if can_unregister_unnegotiated_producer(state, &handle) {
        debug!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id(),
            ?transport_media_id,
            mid = ?handle.mid(),
            "released unnegotiated producer media without staging sdp removal"
        );
        return unregister_media_handle(state, bitrate_registry, transport_media_id);
    }
    if let Err(error) =
        stage_last_mid_removal_before_unregistering_handle(state, transport_media_id, &handle)
    {
        if !matches!(error, TransportAdapterError::InvalidInput)
            || !can_unregister_unnegotiated_producer(state, &handle)
        {
            return Err(error);
        }
        debug!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id(),
            ?transport_media_id,
            mid = ?handle.mid(),
            "released unnegotiated producer media after removal staging failed"
        );
    }
    unregister_media_handle(state, bitrate_registry, transport_media_id)
}

fn unregister_media_handle(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    transport_media_id: TransportMediaId,
) -> Result<(), TransportAdapterError> {
    let Some(handle) = state.remove_media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    match handle {
        RegisteredMediaHandle::Producer { session_key, mid } => {
            if let Ok(mut bitrate) = bitrate_registry.lock() {
                bitrate.remove_incoming_media(&session_key, transport_media_id);
            }
            if let Some(session_state) = state.users.get_mut(&session_key) {
                session_state
                    .sdp_negotiation
                    .negotiated_producer_parameters
                    .remove(&mid);
            }
            state.media_route_index.remove(&transport_media_id);
            state.mark_session_dirty(&session_key);
            Ok(())
        }
        RegisteredMediaHandle::Consumer {
            session_key,
            source_transport_media_id,
            ..
        } => {
            if let Some(session_state) = state.users.get_mut(&session_key) {
                forget_transport_media_streams(
                    &mut session_state.consumer_streams,
                    transport_media_id,
                );
            }
            remove_consumer_route(
                state,
                &session_key,
                transport_media_id,
                source_transport_media_id,
            );
            state.mark_session_dirty(&session_key);
            Ok(())
        }
    }
}

/// Stage or apply removal before the public transport-handle registry changes.
///
/// If removal cannot be represented in the user's live or staged SDP, the
/// caller may only unregister producer media that never gained negotiated RTP
/// parameters.
fn stage_last_mid_removal_before_unregistering_handle(
    state: &mut PacketLoopState,
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
        .users
        .get_mut(handle.session_key())
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_media_removal(session_state, handle.mid())
    } else {
        session_state.rtc.direct_api().remove_media(handle.mid());
        Ok(())
    }
}

fn can_unregister_unnegotiated_producer(
    state: &PacketLoopState,
    handle: &RegisteredMediaHandle,
) -> bool {
    let RegisteredMediaHandle::Producer { session_key, mid } = handle else {
        return false;
    };
    state.users.get(session_key).is_some_and(|session_state| {
        session_state.sdp_negotiation.initial_offer_applied
            && !offer_is_awaiting_answer(session_state)
            && !session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .contains_key(mid)
    })
}

fn session_has_other_mid_user(
    state: &PacketLoopState,
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

/// Returns whether the worker already handed out a local offer and is still
/// waiting for the matching answer. That state accepts queued removals, but it
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

/// Declare one recv-only media line owned by the publishing user.
///
/// Before the first answer lands, the RTC state can be updated directly because
/// there is no committed negotiated description to keep in sync yet. After that
/// point every addition must stage the next renegotiation offer first.
fn worker_add_recv_media(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    policy: RecvMediaPolicy,
    session_key: &TransportSessionKey,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> TransportResult<TransportMediaId> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        worker_stage_native_recv_media(
            session_state,
            media_kind,
            rtp_parameters,
            policy.codec_flags,
            policy.codec_preferences,
            policy.video_bitrate_limits,
        )?
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
                    stream_rx.request_remb(Str0mBitrate::bps(policy.max_bitrate_in.as_bps()));
                }
                #[cfg(test)]
                {
                    session_state.max_bitrate_in = Some(policy.max_bitrate_in);
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
    if let Ok(mut bitrate) = bitrate_registry.lock() {
        let counter =
            bitrate.register_incoming_media(session_key, transport_media_id, Instant::now());
        state.register_incoming_bitrate_counter(transport_media_id, counter);
    }
    debug!(
        user_id = ?session_key.user_id(),
        media_worker_id = session_key.media_worker_id(),
        ?transport_media_id,
        ?media_kind,
        "declared recv-only media on rtc user for incoming producer RTP"
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
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
    video_bitrate_limits: VideoBitrateLimits,
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
        simulcast::publish_recv_simulcast_or_default(
            media_kind,
            rtp_parameters,
            codec_flags,
            video_bitrate_limits,
        ),
    );
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    session_state.sdp_negotiation.staged_offer_upload_slots = vec![upload_slot(
        mid,
        media_kind,
        rtp_parameters,
        codec_flags,
        codec_preferences,
        video_bitrate_limits,
    )];
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
/// corresponding route-source ownership in the worker packet-loop state.
///
/// Remote-source registration, consumer-media declaration, and route creation
/// form one logical edge. If the consumer user is gone or media staging
/// fails, any provisional remote-source registration is restored before the
/// error escapes.
fn worker_add_send_media(
    state: &mut PacketLoopState,
    request: AddSendMediaRequest<'_>,
    now: Instant,
) -> TransportResult<TransportMediaId> {
    let AddSendMediaRequest {
        consumer_session_key,
        media_kind,
        source_session_key,
        source_transport_media_id,
        remote_source_control,
        consumer_rtp_parameters,
    } = request;
    let remote_source_rollback = RemoteSourceRollback::capture(
        state,
        source_session_key.media_worker_id() != consumer_session_key.media_worker_id(),
        source_transport_media_id,
    );
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
                consumer_user_id = ?consumer_session_key.user_id(),
                consumer_media_worker_id = consumer_session_key.media_worker_id(),
                source_user_id = ?source_session_key.user_id(),
                source_media_worker_id = source_session_key.media_worker_id(),
                ?source_transport_media_id,
                error = ?error,
                "failed to register route source for consumer media"
            );
            return Err(error);
        }
    };
    if matches!(route_source, RouteSourceKind::Remote) {
        state.refresh_source_decoder_refresh_codec(
            source_transport_media_id,
            consumer_rtp_parameters,
        );
    }
    let Some(session_state) = state.users.get_mut(consumer_session_key) else {
        remote_source_rollback.rollback(state);
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = if session_state.sdp_negotiation.initial_offer_applied {
        match worker_stage_native_send_media(session_state, media_kind) {
            Ok(mid) => mid,
            Err(error) => {
                let _ = session_state;
                remote_source_rollback.rollback(state);
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
        ConsumerRouteRegistration {
            consumer_session_key,
            consumer_transport_media_id: transport_media_id,
            consumer_mid: mid,
            source_transport_media_id,
            consumer_rtp_parameters,
            now,
        },
    );
    debug!(
        consumer_user_id = ?consumer_session_key.user_id(),
        consumer_media_worker_id = consumer_session_key.media_worker_id(),
        source_user_id = ?source_session_key.user_id(),
        source_media_worker_id = source_session_key.media_worker_id(),
        ?source_transport_media_id,
        source_route_kind = route_source.label(),
        ?transport_media_id,
        ?media_kind,
        consumer_payload_type = ?super::control::consumer_payload_type(consumer_rtp_parameters),
        downstream_rid_policy = "single_ridless_stream",
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
    let source_encoding_count = consumer_rtp_parameters.encodings().count();
    let negotiated_ssrc = consumer_rtp_parameters
        .encodings()
        .find_map(|encoding| encoding.ssrc().map(Ssrc::from));
    let ssrc = negotiated_ssrc.unwrap_or_else(|| api.new_ssrc());
    api.declare_stream_tx(ssrc, None, mid, None);
    debug!(
        ?mid,
        ?media_kind,
        ?ssrc,
        source_encoding_count,
        downstream_rid_policy = "single_ridless_stream",
        "declared browser consumer RTP stream"
    );
}

fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
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
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
    video_bitrate_limits: VideoBitrateLimits,
) -> SessionUploadSlot {
    SessionUploadSlot {
        mid: mid.to_string(),
        kind: negotiation::upload_kind(media_kind),
        codecs: upload_codecs(media_kind, rtp_parameters, codec_flags, codec_preferences),
        simulcast_encodings: simulcast::publish_upload_encodings_or_default(
            media_kind,
            rtp_parameters,
            codec_flags,
            video_bitrate_limits,
        ),
    }
}

fn upload_codecs(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> Vec<String> {
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
    if codecs.is_empty() {
        negotiation::offered_codecs(media_kind, codec_flags, codec_preferences)
    } else {
        codecs
    }
}
