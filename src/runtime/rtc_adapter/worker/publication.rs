//! Answer-side producer parameter projection.
//!
//! Producers are declared inside the worker before the remote answer confirms
//! the effective RTP bindings. This module projects the accepted answer back
//! into router-native RTP parameters and keeps the producer SSRC indexes aligned
//! with those negotiated bindings.

use std::collections::BTreeSet;

use o_sfu_router::{
    HeaderExtension as RouterHeaderExtension, MediaFormat as RouterMediaFormat,
    MediaKind as RouterMediaKind, RtcpFeedback, RtcpFeedbackKind,
    RtpParameters as RouterRtpParameters, StreamBinding,
};
use str0m::bwe::Bitrate;
use str0m::{
    change::SdpAnswer,
    format::PayloadParams,
    media::{Direction, MediaKind as Str0mMediaKind, Mid, Rid},
    rtp::Extension,
};
use tokio::sync::oneshot;
use tracing::warn;

use crate::{
    rfc::rtp as rfc_rtp,
    runtime::transport_adapter::{TransportAdapterError, TransportMediaId, TransportSessionKey},
};

use super::super::{
    media_registry::RegisteredMediaHandle,
    state::{RtcBootstrapState, RtcSessionState},
};

pub(super) fn respond_resolve_negotiated_producer_parameters(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Result<RouterRtpParameters, TransportAdapterError>>,
) {
    let _ = response.send(worker_resolve_negotiated_producer_parameters(
        state,
        session_key,
        transport_media_id,
    ));
}

pub(super) fn refresh_negotiated_producer_parameters(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    producer_mids: &[Mid],
    answer_sdp: &str,
    max_bitrate_in_bps: u64,
) {
    let mut refreshed_parameters = Vec::with_capacity(producer_mids.len());
    let producer_mid_set = producer_mids.iter().copied().collect::<BTreeSet<_>>();
    {
        let Some(session_state) = state.sessions.get_mut(session_key) else {
            return;
        };
        session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .retain(|mid, _parameters| !producer_mid_set.contains(mid));
        let Ok(answer) = SdpAnswer::from_sdp_string(answer_sdp) else {
            return;
        };
        for media_line in answer
            .media_lines
            .iter()
            .filter(|media_line| producer_mid_set.contains(&media_line.mid()))
        {
            if !matches!(
                media_line.direction(),
                Direction::SendOnly | Direction::SendRecv
            ) {
                continue;
            }
            let mid = media_line.mid();
            let Some(media_kind) = session_state
                .rtc
                .media(mid)
                .map(|media| to_router_media_kind(media.kind()))
            else {
                continue;
            };
            let payload_params = media_line.rtp_params();
            if payload_params.is_empty() {
                continue;
            }
            let primary_payload_type = payload_params.first().map(|params| *params.pt());
            let mut formats = Vec::with_capacity(payload_params.len().saturating_mul(2));
            for params in &payload_params {
                formats.push(project_media_format(media_kind, params));
                if let Some(resend_payload_type) = params.resend() {
                    formats.push(
                        RouterMediaFormat::new(
                            media_kind,
                            rfc_rtp::codec_name::RTX,
                            *resend_payload_type,
                            params.spec().clock_rate.get(),
                        )
                        .with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, params.pt().to_string()),
                    );
                }
            }
            let header_extensions = media_line
                .extmaps()
                .into_iter()
                .map(project_header_extension)
                .collect::<Vec<_>>();
            let rids = media_line.rids();
            let primary_ssrcs = media_line
                .ssrc_info()
                .into_iter()
                .filter(|info| info.repairs.is_none())
                .map(|info| *info.ssrc)
                .collect::<Vec<_>>();
            let bindings = project_bindings(
                session_state,
                mid,
                primary_payload_type,
                rids,
                primary_ssrcs,
            );
            if bindings.is_empty() {
                continue;
            }
            apply_projected_recv_streams(session_state, mid, &bindings, max_bitrate_in_bps);
            let parameters = RouterRtpParameters::new(formats, header_extensions, bindings)
                .with_mid(mid.to_string());
            session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .insert(mid, parameters.clone());
            refreshed_parameters.push((mid, parameters));
        }
    }
    for producer_mid in producer_mids {
        state.clear_producer_ssrc_bindings_for_mid(session_key, *producer_mid);
    }
    for (mid, parameters) in refreshed_parameters {
        state.refresh_producer_ssrc_bindings(session_key, mid, &parameters);
    }
}

fn apply_projected_recv_streams(
    session_state: &mut RtcSessionState,
    mid: Mid,
    bindings: &[StreamBinding],
    max_bitrate_in_bps: u64,
) {
    let mut api = session_state.rtc.direct_api();
    for binding in bindings {
        let Some(ssrc) = binding.ssrc() else {
            continue;
        };
        let rid = binding.rid().map(Rid::from);
        if api.stream_rx_by_mid(mid, rid).is_some() {
            if let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) {
                stream_rx.request_remb(Bitrate::bps(max_bitrate_in_bps));
            }
            continue;
        }
        api.expect_stream_rx(ssrc.into(), None, mid, rid);
        if let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) {
            stream_rx.request_remb(Bitrate::bps(max_bitrate_in_bps));
        }
    }
    #[cfg(test)]
    {
        session_state.max_bitrate_in_bps = Some(max_bitrate_in_bps);
    }
}

/// Resolve the router-native RTP parameters for one producer after answer-side
/// projection has populated them for the owning session.
fn worker_resolve_negotiated_producer_parameters(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<RouterRtpParameters, TransportAdapterError> {
    let Some(handle) = state.mid_registry.get(&transport_media_id.as_u64()) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = match handle {
        RegisteredMediaHandle::Producer {
            session_key: owner_session_key,
            mid,
        } if owner_session_key == session_key => *mid,
        RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
            return Err(TransportAdapterError::InvalidInput);
        }
    };
    let Some(session_state) = state.sessions.get(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let result = session_state
        .sdp_negotiation
        .negotiated_producer_parameters
        .get(&mid)
        .cloned()
        .ok_or(TransportAdapterError::UnsupportedFeature);
    if let Err(TransportAdapterError::UnsupportedFeature) = &result {
        warn!(
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            ?transport_media_id,
            ?mid,
            initial_offer_applied = session_state.sdp_negotiation.initial_offer_applied,
            pending_offer = session_state.sdp_negotiation.pending_offer.is_some(),
            staged_offer = session_state.sdp_negotiation.staged_offer_sdp.is_some(),
            negotiated_mids = ?session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            "negotiated producer parameters were not projected for the staged producer media"
        );
    }
    result
}

fn project_media_format(
    media_kind: RouterMediaKind,
    payload_params: &PayloadParams,
) -> RouterMediaFormat {
    let spec = payload_params.spec();
    let mut format = RouterMediaFormat::new(
        media_kind,
        spec.codec.to_string(),
        *payload_params.pt(),
        spec.clock_rate.get(),
    );
    if let Some(channels) = spec.channels {
        format = format.with_channels(u16::from(channels));
    }
    format = apply_codec_parameters(format, &spec.format.to_string());
    for feedback in rtcp_feedback(payload_params) {
        format = format.with_rtcp_feedback(feedback);
    }
    format
}

fn apply_codec_parameters(mut format: RouterMediaFormat, format_params: &str) -> RouterMediaFormat {
    for entry in format_params
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((key, value)) = entry.split_once('=') else {
            continue;
        };
        format = format.with_parameter(key.trim(), value.trim());
    }
    format
}

fn rtcp_feedback(payload_params: &PayloadParams) -> Vec<RtcpFeedback> {
    let mut feedback = Vec::with_capacity(5);
    if payload_params.fb_transport_cc() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));
    }
    if payload_params.fb_nack() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::Nack, None));
    }
    if payload_params.fb_pli() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    }
    if payload_params.fb_fir() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None));
    }
    if payload_params.fb_remb() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None));
    }
    feedback
}

fn project_header_extension((id, extension): (u8, &Extension)) -> RouterHeaderExtension {
    RouterHeaderExtension::new(extension.as_uri().to_owned(), id)
}

fn project_bindings(
    session_state: &mut RtcSessionState,
    mid: Mid,
    primary_payload_type: Option<u8>,
    rids: Vec<Rid>,
    primary_ssrcs: Vec<u32>,
) -> Vec<StreamBinding> {
    if !rids.is_empty() {
        let mut bindings = rids
            .into_iter()
            .map(|rid| {
                let mut binding = StreamBinding::new().with_rid(rid.to_string());
                if let Some(payload_type) = primary_payload_type {
                    binding = binding.with_payload_type(payload_type);
                }
                if let Some(ssrc) = stream_rx_ssrc(session_state, mid, Some(rid)) {
                    binding = binding.with_ssrc(ssrc);
                }
                binding
            })
            .collect::<Vec<_>>();
        if !bindings.iter().any(|binding| binding.ssrc().is_some()) {
            let fallback_ssrc = primary_ssrcs
                .first()
                .copied()
                .or_else(|| stream_rx_ssrc(session_state, mid, None));
            if let Some(ssrc) = fallback_ssrc
                && let Some(first_binding) = bindings.first_mut()
            {
                *first_binding = first_binding.clone().with_ssrc(ssrc);
            }
        }
        return bindings;
    }

    let mut bindings = primary_ssrcs
        .into_iter()
        .map(|ssrc| {
            let mut binding = StreamBinding::new().with_ssrc(ssrc);
            if let Some(payload_type) = primary_payload_type {
                binding = binding.with_payload_type(payload_type);
            }
            binding
        })
        .collect::<Vec<_>>();
    if bindings.is_empty()
        && let Some(ssrc) = stream_rx_ssrc(session_state, mid, None)
    {
        let mut binding = StreamBinding::new().with_ssrc(ssrc);
        if let Some(payload_type) = primary_payload_type {
            binding = binding.with_payload_type(payload_type);
        }
        bindings.push(binding);
    }
    bindings
}

fn stream_rx_ssrc(session_state: &mut RtcSessionState, mid: Mid, rid: Option<Rid>) -> Option<u32> {
    session_state
        .rtc
        .direct_api()
        .stream_rx_by_mid(mid, rid)
        .map(|stream_rx| *stream_rx.ssrc())
}

fn to_router_media_kind(media_kind: Str0mMediaKind) -> RouterMediaKind {
    match media_kind {
        Str0mMediaKind::Audio => RouterMediaKind::Audio,
        Str0mMediaKind::Video => RouterMediaKind::Video,
    }
}
