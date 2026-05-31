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
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc::MediaKind as ProtocolMediaKind;
use str0m::{
    change::{SdpAnswer, SdpApi},
    media::{Direction, MediaKind, Mid},
};
use tracing::debug;

use super::{
    super::super::{
        bitrate::BitrateRegistry,
        bootstrap, simulcast,
        state::{PacketLoopState, RtcSnapshotState},
    },
    publication::refresh_negotiated_producer_parameters,
    recv_stream::{StaleSsrcPolicy, apply_recv_stream},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    engine::{
        media_transport::{
            AppliedProducer, AppliedSessionAnswer, SessionOffer, SessionUploadEncoding,
            SessionUploadSlot, TransportAdapterError, TransportSessionKey,
        },
        metrics::RuntimeMetrics,
    },
};

const INITIAL_NEGOTIATION_DIRECTION: Direction = Direction::RecvOnly;
const INITIAL_NEGOTIATION_MEDIA_KINDS: [MediaKind; 2] = [MediaKind::Audio, MediaKind::Video];

#[derive(Clone, Copy)]
pub(super) struct OfferBootstrapConfig<'a> {
    pub(super) public_ip: IpAddr,
    pub(super) max_bitrate_out: Bitrate,
    pub(super) video_bitrate_limits: VideoBitrateLimits,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) codec_preferences: CodecPreferences,
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
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: OfferBootstrapConfig<'_>,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    ensure_session_ready_for_offer(state, bitrate_registry, snapshot_state, config, session_key)?;
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
            config.codec_flags,
            config.video_bitrate_limits,
        );
        sdp_api
            .apply()
            .ok_or(TransportAdapterError::TransportUnavailable)?
    };

    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = None;
    session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .clear();
    Ok(
        SessionOffer::new(offer.to_sdp_string()).with_upload_slots(initial_upload_slots(
            bootstrap_mids,
            config.codec_flags,
            config.codec_preferences,
            config.video_bitrate_limits,
        )),
    )
}

pub(super) fn worker_create_session_renegotiation_offer(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if !session_state.sdp_negotiation.initial_offer_applied {
        return Err(TransportAdapterError::InvalidInput);
    }
    let Some(offer_sdp) = session_state.sdp_negotiation.staged_offer_sdp.take() else {
        if session_state.sdp_negotiation.pending_offer.is_some() {
            return Err(TransportAdapterError::InvalidInput);
        }
        return Err(TransportAdapterError::UnsupportedFeature);
    };
    let upload_slots = mem::take(&mut session_state.sdp_negotiation.staged_offer_upload_slots);
    Ok(SessionOffer::new(offer_sdp).with_upload_slots(upload_slots))
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
    answer_sdp: &str,
) -> Result<AppliedSessionAnswer, TransportAdapterError> {
    let producer_media_snapshot = state.producer_media_snapshot_for_session(session_key);
    let producer_mids = producer_media_snapshot
        .iter()
        .map(|(_transport_media_id, mid)| *mid)
        .collect::<Vec<_>>();
    let answer = SdpAnswer::from_sdp_string(answer_sdp)
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    let remote_candidate_addrs = answer_remote_candidate_addrs(&answer);
    let staged_upload_slots = {
        let Some(session_state) = state.users.get_mut(session_key) else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        for producer_mid in &producer_mids {
            simulcast::send_rids_for_mid(answer_sdp, *producer_mid)
                .map_err(|_error| TransportAdapterError::InvalidInput)?;
        }
        let Some(pending_offer) = session_state.sdp_negotiation.pending_offer.take() else {
            return Err(TransportAdapterError::InvalidInput);
        };
        session_state
            .rtc
            .sdp_api()
            .accept_answer(pending_offer, answer)
            .map_err(|_error| TransportAdapterError::InvalidInput)?;
        session_state.sdp_negotiation.initial_offer_applied = true;
        session_state.sdp_negotiation.staged_offer_sdp = None;
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
        answer_sdp,
        max_bitrate_in,
    )?;
    let refreshed_by_mid = refreshed_parameters.into_iter().collect::<BTreeMap<_, _>>();
    if let Some(session_state) = state.users.get_mut(session_key) {
        stage_queued_removal_offer(session_state);
    }
    state.mark_session_dirty(session_key);
    state
        .remote_addr_demux
        .replace_session_remote_candidate_addrs(
            session_key,
            remote_candidate_addrs.iter().copied(),
        );
    Ok(AppliedSessionAnswer::from_negotiated_producer_details(
        producer_media_snapshot
            .into_iter()
            .filter_map(|(transport_media_id, mid)| {
                refreshed_by_mid.get(&mid).cloned().map(|parameters| {
                    (
                        transport_media_id,
                        AppliedProducer::new(
                            parameters,
                            upload_encodings_for_mid(&staged_upload_slots, mid),
                        ),
                    )
                })
            }),
    ))
}

fn upload_encodings_for_mid(
    upload_slots: &[SessionUploadSlot],
    mid: Mid,
) -> Vec<SessionUploadEncoding> {
    let mid = mid.to_string();
    upload_slots
        .iter()
        .find(|slot| slot.mid == mid)
        .map_or_else(Vec::new, |slot| slot.simulcast_encodings.clone())
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
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    session_state
        .sdp_negotiation
        .staged_offer_upload_slots
        .clear();
}

fn ensure_initial_negotiation_media(
    bootstrap_mids: &mut Vec<Mid>,
    sdp_api: &mut SdpApi<'_>,
    codec_flags: MediaCodecFlags,
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
                simulcast::bootstrap_recv_simulcast(media_kind, codec_flags, video_bitrate_limits),
            )
        })
        .collect();
}

fn initial_upload_slots(
    bootstrap_mids: &[Mid],
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadSlot> {
    INITIAL_NEGOTIATION_MEDIA_KINDS
        .iter()
        .zip(bootstrap_mids.iter())
        .map(|(media_kind, mid)| SessionUploadSlot {
            mid: mid.to_string(),
            kind: upload_kind(*media_kind),
            codecs: offered_codecs(*media_kind, codec_flags, codec_preferences),
            simulcast_encodings: simulcast::bootstrap_upload_encodings(
                *media_kind,
                codec_flags,
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

pub(super) fn offered_codecs(
    media_kind: MediaKind,
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> Vec<String> {
    if media_kind.is_video() {
        return codec_preferences
            .video_order()
            .into_iter()
            .filter(|codec| codec.enabled_by(codec_flags))
            .map(|codec| codec.wire_name().to_owned())
            .collect();
    }
    codec_preferences
        .audio_order()
        .into_iter()
        .filter(|codec| codec.enabled_by(codec_flags))
        .map(|codec| codec.wire_name().to_owned())
        .collect()
}

fn ensure_session_ready_for_offer(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: OfferBootstrapConfig<'_>,
    session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    let candidate_addr = if let Some(shared_socket) = state.shared_socket.as_ref() {
        shared_socket.candidate_addr
    } else {
        let shared_socket =
            bootstrap::bind_shared_rtc_socket(config.public_ip, config.rtc_port_range)?;
        let candidate_addr = shared_socket.candidate_addr;
        state.shared_socket = Some(shared_socket);
        candidate_addr
    };
    let created_session = bootstrap::ensure_session_rtc_state_with_stats_interval(
        &mut state.users,
        session_key,
        candidate_addr,
        config.max_bitrate_out,
        config.codec_flags,
        config.media_quality_interval,
    )?;
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.add_session(session_key);
    }
    if let Ok(mut bitrate) = bitrate_registry.lock() {
        let counter = bitrate.register_session_egress(session_key, Instant::now());
        state.register_egress_bitrate_counter(session_key.clone(), counter);
    }
    if let Some(session_state) = state.users.get(session_key) {
        let local_ice_ufrag_changed = state
            .remote_addr_demux
            .remember_local_ice_ufrag(&session_state.local_ice_ufrag, session_key);
        if created_session || local_ice_ufrag_changed {
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                %candidate_addr,
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
