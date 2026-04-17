use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

use str0m::{
    change::{SdpAnswer, SdpApi},
    media::{Direction, MediaKind, Mid},
    rtp::Ssrc,
};
use tokio::sync::oneshot;
use tracing::debug;

use crate::{
    config::MediaCodecFlags,
    config::RtcPortRange,
    runtime::metrics::RuntimeMetrics,
    runtime::transport_adapter::{SessionOffer, TransportAdapterError, TransportSessionKey},
};

use super::super::{
    bootstrap,
    state::{RtcBootstrapState, RtcSnapshotState},
};
use super::publication::refresh_negotiated_producer_parameters;

const INITIAL_NEGOTIATION_DIRECTION: Direction = Direction::RecvOnly;
const INITIAL_NEGOTIATION_MEDIA_KINDS: [MediaKind; 2] = [MediaKind::Audio, MediaKind::Video];

#[derive(Clone, Copy)]
pub(super) struct OfferBootstrapConfig<'a> {
    pub(super) public_ip: IpAddr,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) metrics: &'a RuntimeMetrics,
}

pub(super) fn respond_create_initial_session_offer(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: OfferBootstrapConfig<'_>,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<SessionOffer, TransportAdapterError>>,
) {
    let _ = response.send(worker_create_initial_session_offer(
        state,
        snapshot_state,
        config,
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
    config: OfferBootstrapConfig<'_>,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    ensure_session_ready_for_offer(state, snapshot_state, config, session_key)?;
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

    let bootstrap_mids = &mut session_state.sdp_negotiation.bootstrap_mids;
    let (offer, pending_offer) = {
        let mut sdp_api = session_state.rtc.sdp_api();
        ensure_initial_negotiation_media(bootstrap_mids, &mut sdp_api);
        sdp_api
            .apply()
            .ok_or(TransportAdapterError::TransportUnavailable)?
    };

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
    let producer_mids = state
        .mid_registry
        .values()
        .filter_map(|handle| match handle {
            super::super::media_registry::RegisteredMediaHandle::Producer {
                session_key: owner_session_key,
                mid,
            } if owner_session_key == session_key => Some(*mid),
            _ => None,
        })
        .collect::<Vec<_>>();
    let answer = SdpAnswer::from_sdp_string(answer_sdp)
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    let remote_candidate_addrs = answer_remote_candidate_addrs(&answer);
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
    refresh_negotiated_producer_parameters(session_state, &producer_mids, answer_sdp);
    stage_queued_removal_offer(session_state);
    session_state.dtls_started = true;
    let local_ice_ufrag = session_state.local_ice_ufrag.clone();
    state.mark_session_dirty(session_key);
    state
        .remote_addr_demux
        .replace_session_remote_candidate_addrs(
            session_key,
            remote_candidate_addrs.iter().copied(),
        );
    debug!(
        session_id = ?session_key.session_id(),
        media_worker_id = session_key.media_worker_id(),
        %local_ice_ufrag,
        remote_candidate_addr_count = remote_candidate_addrs.len(),
        remote_candidate_addrs = ?remote_candidate_addrs,
        "registered answered remote candidate addresses for rtc session"
    );
    Ok(())
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
            .map(|stream_rx| Ssrc::from(*stream_rx.ssrc()))
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

fn ensure_initial_negotiation_media(bootstrap_mids: &mut Vec<Mid>, sdp_api: &mut SdpApi<'_>) {
    if !bootstrap_mids.is_empty() {
        return;
    }
    *bootstrap_mids = INITIAL_NEGOTIATION_MEDIA_KINDS
        .into_iter()
        .map(|media_kind| {
            sdp_api.add_media(media_kind, INITIAL_NEGOTIATION_DIRECTION, None, None, None)
        })
        .collect();
}

fn ensure_session_ready_for_offer(
    state: &mut RtcBootstrapState,
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
    let created_session = bootstrap::ensure_session_rtc_state(
        &mut state.sessions,
        session_key,
        candidate_addr,
        config.codec_flags,
    )?;
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.add_session(session_key);
    }
    if let Some(session_state) = state.sessions.get(session_key) {
        let registered_local_ice_ufrag = state
            .remote_addr_demux
            .remember_local_ice_ufrag(&session_state.local_ice_ufrag, session_key);
        if created_session || registered_local_ice_ufrag {
            debug!(
                session_id = ?session_key.session_id(),
                media_worker_id = session_key.media_worker_id(),
                %candidate_addr,
                local_ice_ufrag = %session_state.local_ice_ufrag,
                created_session,
                "prepared rtc session for offer generation"
            );
        }
    }
    if created_session {
        config.metrics.add_active_transport_sessions(1);
    }
    Ok(())
}
