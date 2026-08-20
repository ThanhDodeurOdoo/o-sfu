//! Offer/answer ownership for worker-local RTC users.
//!
//! This module keeps the one-outstanding-offer rule local to the worker that
//! owns the user's `str0m::Rtc`. It is responsible for creating the initial
//! server-authored offer, handing out staged follow-up offers, accepting remote
//! answers, and refreshing any worker-local state that answer application
//! invalidates.

use std::{
    collections::BTreeMap,
    mem,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use o_sfu_rfc::webrtc::MediaKind as ProtocolMediaKind;
use str0m::{
    change::{SdpAnswer, SdpApi},
    media::{Direction, Media, MediaKind, Mid},
};
use tracing::debug;

use super::{
    super::super::{
        RtpProfile,
        bitrate::BitrateRegistry,
        bootstrap, codec,
        commands::{ParsedSessionAnswer, RtcSessionOffer},
        state::PacketLoopState,
    },
    publication::{answer_producer_projection, refresh_negotiated_producer_parameters},
    recv_stream::{StaleSsrcPolicy, apply_recv_stream},
};
use crate::{
    Bitrate, VideoBitrateLimits,
    engine::{
        media_transport::{
            AppliedProducer, AppliedSessionAnswer, SessionUploadEncoding, SessionUploadSlot,
            TransportAdapterError, TransportSessionKey,
        },
        metrics::RuntimeMetrics,
    },
};

const INITIAL_NEGOTIATION_DIRECTION: Direction = Direction::RecvOnly;
const INITIAL_NEGOTIATION_MEDIA_KINDS: [MediaKind; 2] = [MediaKind::Audio, MediaKind::Video];

#[derive(Clone, Copy)]
pub(super) struct OfferBootstrapConfig<'a> {
    pub(super) candidate_addr: SocketAddr,
    pub(super) max_bitrate_out: Bitrate,
    pub(super) video_bitrate_limits: VideoBitrateLimits,
    pub(super) profile: &'a RtpProfile,
    pub(super) media_quality_interval: Option<Duration>,
    pub(super) metrics: &'a RuntimeMetrics,
}

/// Create the first local offer for a user after ensuring the worker has
/// packet-loop state and the user still has no negotiated media.
///
/// The initial offer is reserved for the transport bootstrap and capability
/// probe flow. Once media has been registered or an earlier initial offer is in
/// flight, callers must use the renegotiation path instead.
pub(super) fn worker_create_initial_session_offer(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    config: OfferBootstrapConfig<'_>,
    room_id: Arc<str>,
    session_key: &TransportSessionKey,
) -> Result<RtcSessionOffer, TransportAdapterError> {
    ensure_session_ready_for_offer(state, bitrate_registry, config, room_id, session_key)?;
    if state.session_has_registered_media(session_key) {
        return Err(TransportAdapterError::UnsupportedFeature);
    }
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if session_state.sdp_negotiation.pending_offer.is_some() {
        return Err(TransportAdapterError::InvalidInput);
    }
    if session_state.sdp_negotiation.initial_offer_applied {
        return Err(TransportAdapterError::UnsupportedFeature);
    }

    let bootstrap_mids = &mut session_state.sdp_negotiation.bootstrap_mids;
    let (offer, pending_offer) = {
        let mut sdp_api = session_state.rtc.sdp_api();
        ensure_initial_negotiation_media(
            bootstrap_mids,
            &mut sdp_api,
            config.profile,
            config.video_bitrate_limits,
        );
        sdp_api
            .apply()
            .ok_or(TransportAdapterError::TransportUnavailable)?
    };

    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer = None;
    session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .clear();
    Ok(RtcSessionOffer::new(
        offer,
        initial_upload_slots(bootstrap_mids, config.profile, config.video_bitrate_limits),
    ))
}

/// Returns the one staged follow-up offer for `session_key`.
///
/// Delivery consumes `staged_offer` but retains `pending_offer` until answer
/// application. The same offer cannot be requested twice.
///
/// # Errors
///
/// Returns [`TransportAdapterError::TransportUnavailable`] when the session is
/// absent. Returns [`TransportAdapterError::InvalidInput`] before the initial
/// answer or while another answer is pending. Returns
/// [`TransportAdapterError::UnsupportedFeature`] when no media change staged an
/// offer.
pub(super) fn worker_create_session_renegotiation_offer(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
) -> Result<RtcSessionOffer, TransportAdapterError> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if !session_state.sdp_negotiation.initial_offer_applied {
        return Err(TransportAdapterError::InvalidInput);
    }
    let Some(offer) = session_state.sdp_negotiation.staged_offer.take() else {
        if session_state.sdp_negotiation.pending_offer.is_some() {
            return Err(TransportAdapterError::InvalidInput);
        }
        return Err(TransportAdapterError::UnsupportedFeature);
    };
    let upload_slots = session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .clone();
    Ok(RtcSessionOffer::new(*offer, upload_slots))
}

/// Accept the currently pending local offer and reconcile every worker-local
/// structure that depends on the answer.
///
/// Applying an answer can recreate recv bindings inside `str0m`, so this path
/// must rebuild pending recv expectations, refresh negotiated producer
/// parameters, stage any deferred removals, and index the remote candidate
/// addresses that later packet-loop recovery depends on.
pub(super) fn worker_apply_session_answer(
    state: &mut PacketLoopState,
    max_bitrate_in: Bitrate,
    session_key: &TransportSessionKey,
    parsed_answer: ParsedSessionAnswer,
) -> Result<AppliedSessionAnswer, TransportAdapterError> {
    let producer_media_snapshot = state.producer_media_snapshot(session_key);
    let producer_mids = producer_media_snapshot
        .iter()
        .map(|(_transport_media_id, mid)| *mid)
        .collect::<Vec<_>>();
    let ParsedSessionAnswer { answer, rids } = parsed_answer;
    // Derive every fallible answer view available before `accept_answer`.
    // str0m 0.21 mutates ICE, DTLS and session state in place, so a later RID
    // or router-projection rejection could not restore the pending offer.
    // https://github.com/algesten/str0m/blob/0.21.0/src/change/sdp.rs#L158-L208
    // https://github.com/algesten/str0m/blob/0.21.0/src/change/sdp.rs#L961-L981
    let remote_candidate_addrs = answer_remote_candidate_addrs(&answer);
    let client_capabilities = codec::client_rtp_capabilities_from_sdp_answer(&answer)?;
    let producer_answer_projection = answer_producer_projection(&answer, &producer_mids)?;
    let offer_encodings = offer_encodings_by_mid(state, session_key, &producer_mids)?;
    let rids_by_mid = producer_mids
        .iter()
        .map(|mid| {
            let encodings = offer_encodings
                .get(mid)
                .map(Vec::as_slice)
                .unwrap_or_default();
            rids.negotiate(*mid, encodings)
                .map(|rids| (*mid, rids))
                .map_err(|_error| TransportAdapterError::InvalidInput)
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let staged_upload_slots = {
        let Some(session_state) = state.users.get_mut(session_key) else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let Some(pending_offer) = session_state.sdp_negotiation.pending_offer.take() else {
            return Err(TransportAdapterError::InvalidInput);
        };
        session_state
            .rtc
            .sdp_api()
            .accept_answer(pending_offer, answer)
            .map_err(|_error| TransportAdapterError::InvalidInput)?;
        session_state.sdp_negotiation.initial_offer_applied = true;
        session_state.sdp_negotiation.staged_offer = None;
        let staged_upload_slots =
            mem::take(&mut session_state.sdp_negotiation.staged_offer_upload_slots);
        apply_pending_recv_streams(session_state, max_bitrate_in);
        session_state.dtls_started = true;
        staged_upload_slots
    };
    let refreshed_parameters = refresh_negotiated_producer_parameters(
        state,
        session_key,
        &producer_mids,
        producer_answer_projection,
        &rids_by_mid,
        max_bitrate_in,
    )?;
    let refreshed_by_mid = refreshed_parameters.into_iter().collect::<BTreeMap<_, _>>();
    if let Some(session_state) = state.users.get_mut(session_key) {
        stage_queued_removal_offer(session_state);
    }
    state.mark_session_dirty(session_key);
    state
        .remote_addr_demux
        .replace_remote_candidates(session_key, remote_candidate_addrs.iter().copied());
    Ok(AppliedSessionAnswer::from_negotiated_producer_details(
        producer_media_snapshot
            .into_iter()
            .filter_map(|(transport_media_id, mid)| {
                refreshed_by_mid.get(&mid).cloned().map(|parameters| {
                    let rids = rids_by_mid.get(&mid).map(Vec::as_slice).unwrap_or_default();
                    (
                        transport_media_id,
                        AppliedProducer::new(
                            parameters,
                            upload_encodings_for_mid(&staged_upload_slots, mid, rids),
                        ),
                    )
                })
            }),
    )
    .with_client_capabilities(client_capabilities))
}

fn offer_encodings_by_mid(
    state: &PacketLoopState,
    session_key: &TransportSessionKey,
    producer_mids: &[Mid],
) -> Result<BTreeMap<Mid, Vec<SessionUploadEncoding>>, TransportAdapterError> {
    let session_state = state
        .users
        .get(session_key)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let mut encodings = session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .iter()
        .map(|slot| {
            (
                Mid::from(slot.mid.as_str()),
                slot.simulcast_encodings.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for mid in producer_mids {
        if encodings.contains_key(mid) {
            continue;
        }
        let Some(rtp_parameters) = session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .get(mid)
        else {
            continue;
        };
        let Some(media_kind) = session_state.rtc.media(*mid).map(Media::kind) else {
            continue;
        };
        let simulcast_encodings = codec::publish_upload_encodings(media_kind, rtp_parameters);
        if simulcast_encodings.is_empty() {
            continue;
        }
        encodings.insert(*mid, simulcast_encodings);
    }
    Ok(encodings)
}

fn upload_encodings_for_mid(
    upload_slots: &[SessionUploadSlot],
    mid: Mid,
    accepted_rids: &[codec::NegotiatedRid],
) -> Vec<SessionUploadEncoding> {
    let mid_name: &str = &mid;
    let Some(slot) = upload_slots.iter().find(|slot| slot.mid == mid_name) else {
        return Vec::new();
    };
    accepted_rids
        .iter()
        .filter_map(|rid| {
            let rid_name: &str = &rid.rid;
            let mut encoding = slot
                .simulcast_encodings
                .iter()
                .find(|encoding| encoding.rid == rid_name)?
                .clone();
            if let Some(max_bitrate) = rid.max_bitrate {
                encoding.max_bitrate = Some(max_bitrate);
            }
            Some(encoding)
        })
        .collect()
}

fn answer_remote_candidate_addrs(answer: &SdpAnswer) -> Vec<SocketAddr> {
    let mut addrs = answer
        .session
        .ice_candidates()
        .map(str0m::Candidate::addr)
        .collect::<Vec<_>>();
    for media_line in &answer.media_lines {
        addrs.extend(media_line.ice_candidates().map(str0m::Candidate::addr));
    }
    addrs
}

fn apply_pending_recv_streams(
    session_state: &mut super::super::super::state::RtcSessionState,
    max_bitrate_in: Bitrate,
) {
    // Answer-time recv refresh can recreate `StreamRx` bindings, so REMB must
    // be re-set here to keep the inbound user cap alive across renegotiation.
    if session_state
        .sdp_negotiation
        .pending_recv_streams
        .is_empty()
    {
        return;
    }
    let pending_recv_streams = mem::take(&mut session_state.sdp_negotiation.pending_recv_streams);
    let mut api = session_state.rtc.direct_api();
    for (mid, streams) in pending_recv_streams {
        for stream in streams {
            apply_recv_stream(
                &mut api,
                mid,
                stream.rid,
                stream.ssrc,
                max_bitrate_in,
                StaleSsrcPolicy::ReplaceStale,
            );
        }
    }
    #[cfg(test)]
    {
        session_state.max_bitrate_in = Some(max_bitrate_in);
    }
}

fn stage_queued_removal_offer(session_state: &mut super::super::super::state::RtcSessionState) {
    if session_state.sdp_negotiation.queued_removal_mids.is_empty() {
        return;
    }

    let queued_removal_mids = mem::take(&mut session_state.sdp_negotiation.queued_removal_mids);
    let mut sdp_api = session_state.rtc.sdp_api();
    for mid in &queued_removal_mids {
        sdp_api.set_direction(*mid, Direction::Inactive);
    }
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        return;
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer = Some(Box::new(offer));
    session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .clear();
}

fn ensure_initial_negotiation_media(
    bootstrap_mids: &mut Vec<Mid>,
    sdp_api: &mut SdpApi<'_>,
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) {
    if !bootstrap_mids.is_empty() {
        return;
    }
    *bootstrap_mids = INITIAL_NEGOTIATION_MEDIA_KINDS
        .into_iter()
        .map(|media_kind| {
            sdp_api.add_media(
                media_kind,
                INITIAL_NEGOTIATION_DIRECTION,
                None,
                None,
                codec::bootstrap_recv_simulcast(media_kind, profile, video_bitrate_limits),
            )
        })
        .collect();
}

fn initial_upload_slots(
    bootstrap_mids: &[Mid],
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadSlot> {
    INITIAL_NEGOTIATION_MEDIA_KINDS
        .iter()
        .zip(bootstrap_mids.iter())
        .map(|(media_kind, mid)| SessionUploadSlot {
            mid: mid.to_string(),
            kind: upload_kind(*media_kind),
            codecs: profile.codec_names(*media_kind).to_vec(),
            simulcast_encodings: codec::bootstrap_upload_encodings(
                *media_kind,
                profile,
                video_bitrate_limits,
            ),
        })
        .collect()
}

pub(super) fn upload_kind(media_kind: MediaKind) -> ProtocolMediaKind {
    if media_kind.is_video() {
        ProtocolMediaKind::Video
    } else {
        ProtocolMediaKind::Audio
    }
}

fn ensure_session_ready_for_offer(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    config: OfferBootstrapConfig<'_>,
    room_id: Arc<str>,
    session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    let created_session = bootstrap::ensure_session_rtc_state_with_stats_interval(
        &mut state.users,
        room_id,
        session_key,
        config.candidate_addr,
        config.max_bitrate_out,
        config.profile,
        config.media_quality_interval,
    )?;
    if let Some(session_state) = state.users.get(session_key) {
        if created_session && let Ok(mut bitrate) = bitrate_registry.lock() {
            bitrate.register_session_egress(session_key, Arc::clone(&session_state.egress_bitrate));
        }
        let local_ice_ufrag_changed = state
            .remote_addr_demux
            .remember_local_ice_ufrag(&session_state.local_ice_ufrag, session_key);
        if created_session || local_ice_ufrag_changed {
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                candidate_addr = %config.candidate_addr,
                local_ice_ufrag = %session_state.local_ice_ufrag,
                created_session,
                "prepared rtc user for offer generation"
            );
        }
    }
    if created_session {
        config.metrics.add_active_transport_users(1);
    }
    Ok(())
}
