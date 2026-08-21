use std::iter;

use o_sfu_rfc::{rtp as rfc_rtp, webrtc as rfc_webrtc};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{
        CodecSetting, HeaderExtension as RouterHeaderExtension, MediaCapabilities, MediaCodec,
        MediaCodecCapability, MediaFormat as RouterMediaFormat, MediaStream, PayloadType,
        RtcpFeedback, RtcpFeedbackKind,
    },
};
use str0m::{change::SdpAnswer, format::PayloadParams, rtp::Extension};

use crate::engine::media_transport::TransportAdapterError;

pub(super) fn primary_codec(parameters: &MediaStream) -> Option<&MediaCodec> {
    parameters
        .formats()
        .find(|format| !format.codec().is_rtx())
        .map(RouterMediaFormat::codec)
}

pub(in crate::engine::media_transport::rtc) fn router_payload_type(
    value: u8,
) -> Result<PayloadType, TransportAdapterError> {
    PayloadType::try_new(value).ok_or(TransportAdapterError::InvalidInput)
}

pub(super) fn media_kind(payload: &PayloadParams) -> RouterMediaKind {
    if payload.spec().codec.is_audio() {
        RouterMediaKind::Audio
    } else {
        RouterMediaKind::Video
    }
}

pub(in crate::engine::media_transport::rtc) fn header_extension(
    (id, extension): (u8, &Extension),
) -> Result<RouterHeaderExtension, TransportAdapterError> {
    let id = rfc_rtp::HeaderExtensionId::try_new(id).ok_or(TransportAdapterError::InvalidInput)?;
    Ok(RouterHeaderExtension::new(
        rfc_webrtc::RtpHeaderExtensionUri::from(extension.as_uri()),
        id,
    ))
}

pub(super) fn media_capability(
    kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Result<MediaCodecCapability, TransportAdapterError> {
    let spec = payload.spec();
    let pt = router_payload_type(*payload.pt())?;
    Ok(apply_payload_codec_facts(
        payload,
        MediaCodecCapability::new(kind, spec.codec.to_string(), spec.clock_rate.get())
            .with_payload_type(pt),
        MediaCodecCapability::with_channels,
        |codec, key, value| codec.with_parameter(key, value),
        MediaCodecCapability::with_rtcp_feedback,
    ))
}

pub(super) fn rtx_capability(
    kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Result<Option<MediaCodecCapability>, TransportAdapterError> {
    let Some(resend) = payload.resend() else {
        return Ok(None);
    };
    let primary_payload_type = router_payload_type(*payload.pt())?;
    let resend_payload_type = router_payload_type(*resend)?;
    // RTX uses the source clock rate and identifies its primary payload type with `apt`.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    Ok(Some(
        MediaCodecCapability::new(
            kind,
            rfc_rtp::codec_name::RTX,
            payload.spec().clock_rate.get(),
        )
        .with_payload_type(resend_payload_type)
        .with_setting(CodecSetting::RtxAssociation(primary_payload_type)),
    ))
}

pub(in crate::engine::media_transport::rtc) fn media_format(
    kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Result<RouterMediaFormat, TransportAdapterError> {
    let spec = payload.spec();
    let pt = router_payload_type(*payload.pt())?;
    Ok(apply_payload_codec_facts(
        payload,
        RouterMediaFormat::new(kind, spec.codec.to_string(), pt, spec.clock_rate.get()),
        RouterMediaFormat::with_channels,
        |format, key, value| format.with_parameter(key, value),
        RouterMediaFormat::with_rtcp_feedback,
    ))
}

pub(in crate::engine::media_transport::rtc) fn rtx_format(
    kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Result<Option<RouterMediaFormat>, TransportAdapterError> {
    let Some(resend) = payload.resend() else {
        return Ok(None);
    };
    let primary_payload_type = router_payload_type(*payload.pt())?;
    let resend_payload_type = router_payload_type(*resend)?;
    // Keep the router format aligned with the same RTX mapping projected as a capability.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    Ok(Some(
        RouterMediaFormat::new(
            kind,
            rfc_rtp::codec_name::RTX,
            resend_payload_type,
            payload.spec().clock_rate.get(),
        )
        .with_setting(CodecSetting::RtxAssociation(primary_payload_type)),
    ))
}

fn apply_payload_codec_facts<T>(
    payload: &PayloadParams,
    mut target: T,
    mut with_channels: impl FnMut(T, u16) -> T,
    mut with_parameter: impl FnMut(T, &str, &str) -> T,
    mut with_rtcp_feedback: impl FnMut(T, RtcpFeedback) -> T,
) -> T {
    let spec = payload.spec();
    if let Some(count) = spec.channels {
        target = with_channels(target, u16::from(count));
    }
    let fmtp = spec.format.to_string();
    for entry in fmtp
        .split(rfc_rtp::fmtp::PARAMETER_SEPARATOR)
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if let Some((key, value)) = entry.split_once(rfc_rtp::fmtp::NAME_VALUE_SEPARATOR) {
            target = with_parameter(target, key.trim(), value.trim());
        }
    }
    for item in rtcp_feedback(payload) {
        target = with_rtcp_feedback(target, item);
    }
    target
}

fn rtcp_feedback(payload: &PayloadParams) -> impl Iterator<Item = RtcpFeedback> {
    [
        payload
            .fb_transport_cc()
            .then_some(RtcpFeedbackKind::TransportCc),
        payload.fb_nack().then_some(RtcpFeedbackKind::Nack),
        payload.fb_pli().then_some(RtcpFeedbackKind::NackPli),
        payload.fb_fir().then_some(RtcpFeedbackKind::CcmFir),
        payload.fb_remb().then_some(RtcpFeedbackKind::GoogRemb),
    ]
    .into_iter()
    .flatten()
    .map(|kind| RtcpFeedback::new(kind, None))
}

#[must_use]
#[cfg(any(test, fuzzing))]
pub fn client_rtp_capabilities_from_answer(answer_sdp: &str) -> Option<MediaCapabilities> {
    super::retransmission::validate_answer_sdp(answer_sdp).ok()?;
    let answer = SdpAnswer::from_sdp_string(answer_sdp).ok()?;
    client_rtp_capabilities_from_sdp_answer(&answer).unwrap_or_default()
}

pub(in crate::engine::media_transport::rtc) fn client_rtp_capabilities_from_sdp_answer(
    answer: &SdpAnswer,
) -> Result<Option<MediaCapabilities>, TransportAdapterError> {
    let mut codecs = Vec::new();
    let mut header_extensions = Vec::new();

    for media_line in &answer.media_lines {
        if media_line.disabled {
            continue;
        }
        let rtp_parameters = answer_payload_params(media_line.rtp_params());
        let Some(media_kind) = rtp_parameters.first().map(media_kind) else {
            continue;
        };
        for payload in &rtp_parameters {
            for codec in iter::once(media_capability(media_kind, payload)?)
                .chain(rtx_capability(media_kind, payload)?)
            {
                if !codecs.contains(&codec) {
                    codecs.push(codec);
                }
            }
        }
        for (id, extension) in media_line.extmaps() {
            let header_extension = header_extension((id, extension))?;
            if !header_extensions.contains(&header_extension) {
                header_extensions.push(header_extension);
            }
        }
    }

    if codecs.is_empty() {
        return Ok(None);
    }

    Ok(Some(MediaCapabilities::new(codecs, header_extensions)))
}

pub(in crate::engine::media_transport::rtc) fn answer_payload_params(
    mut payloads: Vec<PayloadParams>,
) -> Vec<PayloadParams> {
    // O-SFU accepts Generic NACK only with a usable RTX mapping. RFC 4585 permits
    // Generic NACK without RTX, so this all-or-nothing projection is O-SFU policy.
    // https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    for payload in &mut payloads {
        payload.set_fb_nack(payload.resend().is_some());
    }
    payloads
}

#[cfg(test)]
#[path = "../TESTS/negotiated_capabilities.rs"]
mod tests;
