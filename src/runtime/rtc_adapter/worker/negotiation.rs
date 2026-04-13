use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
};

use str0m::{
    change::SdpAnswer,
    media::{Direction, MediaKind},
};
use tokio::sync::oneshot;

use crate::{
    config::MediaCodecFlags,
    config::RtcPortRange,
    runtime::transport_adapter::{SessionOffer, TransportAdapterError, TransportSessionKey},
};

use super::super::{
    bootstrap,
    state::{RtcBootstrapState, RtcSnapshotState},
};

const INITIAL_NEGOTIATION_MEDIA_KIND: MediaKind = MediaKind::Audio;
const INITIAL_NEGOTIATION_DIRECTION: Direction = Direction::Inactive;

pub(super) fn respond_create_initial_session_offer(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
) {
    let _ = response.send(worker_create_initial_session_offer(
        state,
        snapshot_state,
        public_ip,
        rtc_port_range,
        codec_flags,
        session_key,
    ));
}

pub(super) fn respond_create_session_renegotiation_offer(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
) {
    let _ = response.send(worker_create_session_renegotiation_offer(
        state,
        session_key,
    ));
}

pub(super) fn respond_apply_session_answer(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    answer_sdp: &str,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let _ = response.send(worker_apply_session_answer(state, session_key, answer_sdp));
}

fn worker_create_initial_session_offer(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    ensure_session_ready_for_offer(
        state,
        snapshot_state,
        public_ip,
        rtc_port_range,
        codec_flags,
        session_key,
    )?;
    if state.session_has_registered_media(session_key) {
        return Err(TransportAdapterError::UnsupportedFeature);
    }
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    if session_state.sdp_negotiation.pending_offer.is_some() {
        return Err(TransportAdapterError::InvalidInput);
    }
    if session_state.sdp_negotiation.initial_offer_applied {
        return Err(TransportAdapterError::UnsupportedFeature);
    }

    let mut bootstrap_mid = session_state.sdp_negotiation.bootstrap_mid;
    let (offer, pending_offer) = {
        let mut sdp_api = session_state.rtc.sdp_api();
        if bootstrap_mid.is_none() {
            bootstrap_mid = Some(sdp_api.add_media(
                INITIAL_NEGOTIATION_MEDIA_KIND,
                INITIAL_NEGOTIATION_DIRECTION,
                None,
                None,
                None,
            ));
        }
        sdp_api
            .apply()
            .ok_or(TransportAdapterError::TransportUnavailable)?
    };

    session_state.sdp_negotiation.bootstrap_mid = bootstrap_mid;
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = None;
    Ok(SessionOffer::new(offer.to_sdp_string()))
}

fn worker_create_session_renegotiation_offer(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
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
    Ok(SessionOffer::new(offer_sdp))
}

fn worker_apply_session_answer(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    answer_sdp: &str,
) -> Result<(), TransportAdapterError> {
    let answer = SdpAnswer::from_sdp_string(answer_sdp)
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    let Some(session_state) = state.sessions.get_mut(session_key) else {
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
    session_state.sdp_negotiation.staged_offer_sdp = None;
    apply_pending_recv_streams(session_state);
    stage_queued_removal_offer(session_state);
    session_state.dtls_started = true;
    state.mark_session_dirty(session_key);
    Ok(())
}

fn apply_pending_recv_streams(session_state: &mut super::super::state::RtcSessionState) {
    if session_state
        .sdp_negotiation
        .pending_recv_streams
        .is_empty()
    {
        return;
    }
    let pending_recv_streams = session_state
        .sdp_negotiation
        .pending_recv_streams
        .iter()
        .map(|(mid, stream)| (*mid, stream.clone()))
        .collect::<Vec<_>>();
    let mut api = session_state.rtc.direct_api();
    for (mid, stream) in &pending_recv_streams {
        if let Some(existing_ssrc) = api
            .stream_rx_by_mid(*mid, stream.rid)
            .map(|stream_rx| str0m::rtp::Ssrc::from(*stream_rx.ssrc()))
            && existing_ssrc != stream.ssrc
        {
            api.remove_stream_rx(existing_ssrc);
        }
        api.expect_stream_rx(stream.ssrc, None, *mid, stream.rid);
    }
    for (mid, _stream) in pending_recv_streams {
        session_state
            .sdp_negotiation
            .pending_recv_streams
            .remove(&mid);
    }
}

fn stage_queued_removal_offer(session_state: &mut super::super::state::RtcSessionState) {
    if session_state.sdp_negotiation.queued_removal_mids.is_empty() {
        return;
    }

    let queued_removal_mids = session_state
        .sdp_negotiation
        .queued_removal_mids
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let mut sdp_api = session_state.rtc.sdp_api();
    for mid in &queued_removal_mids {
        sdp_api.set_direction(*mid, Direction::Inactive);
    }
    let Some((offer, pending_offer)) = sdp_api.apply() else {
        session_state.sdp_negotiation.queued_removal_mids.clear();
        return;
    };
    session_state.sdp_negotiation.pending_offer = Some(pending_offer);
    session_state.sdp_negotiation.staged_offer_sdp = Some(offer.to_sdp_string());
    for mid in queued_removal_mids {
        session_state
            .sdp_negotiation
            .queued_removal_mids
            .remove(&mid);
    }
}

fn ensure_session_ready_for_offer(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    let candidate_addr = if let Some(shared_socket) = state.shared_socket.as_ref() {
        shared_socket.candidate_addr
    } else {
        let shared_socket = bootstrap::bind_shared_rtc_socket(public_ip, rtc_port_range)?;
        let candidate_addr = shared_socket.candidate_addr;
        state.shared_socket = Some(shared_socket);
        candidate_addr
    };
    bootstrap::ensure_session_rtc_state(
        &mut state.sessions,
        session_key,
        candidate_addr,
        codec_flags,
    )?;
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.add_session(session_key);
    }
    Ok(())
}
