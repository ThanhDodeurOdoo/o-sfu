use o_sfu_rfc::{
    rtp::{self as rfc_rtp, HeaderExtensionId},
    webrtc as rfc_webrtc,
};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{
        CodecSetting, HeaderExtension as RouterHeaderExtension, MediaCodecCapability,
        MediaFormat as RouterMediaFormat, PayloadType, RtcpFeedback, RtcpFeedbackKind,
    },
};
use str0m::{format::PayloadParams, rtp::Extension};

use crate::engine::media_transport::TransportAdapterError;

pub(super) fn router_payload_type(value: u8) -> Result<PayloadType, TransportAdapterError> {
    PayloadType::try_new(value).ok_or(TransportAdapterError::InvalidInput)
}

pub(super) fn media_kind(payload: &PayloadParams) -> RouterMediaKind {
    if payload.spec().codec.is_audio() {
        RouterMediaKind::Audio
    } else {
        RouterMediaKind::Video
    }
}

pub(super) fn header_extension(
    (id, extension): (u8, &Extension),
) -> Result<RouterHeaderExtension, TransportAdapterError> {
    let id = HeaderExtensionId::try_new(id).ok_or(TransportAdapterError::InvalidInput)?;
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

pub(super) fn media_format(
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

pub(super) fn rtx_capability(
    kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Result<Option<MediaCodecCapability>, TransportAdapterError> {
    Ok(rtx_payload_types(payload)?.map(|(rtx_pt, associated_pt)| {
        MediaCodecCapability::new(
            kind,
            rfc_rtp::CodecName::Rtx,
            payload.spec().clock_rate.get(),
        )
        .with_payload_type(rtx_pt)
        .with_setting(CodecSetting::RtxAssociation(associated_pt))
    }))
}

pub(super) fn rtx_format(
    kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Result<Option<RouterMediaFormat>, TransportAdapterError> {
    Ok(rtx_payload_types(payload)?.map(|(rtx_pt, associated_pt)| {
        RouterMediaFormat::new(
            kind,
            rfc_rtp::CodecName::Rtx,
            rtx_pt,
            payload.spec().clock_rate.get(),
        )
        .with_setting(CodecSetting::RtxAssociation(associated_pt))
    }))
}

fn rtx_payload_types(
    payload: &PayloadParams,
) -> Result<Option<(PayloadType, PayloadType)>, TransportAdapterError> {
    let Some(rtx_payload_type) = payload.resend() else {
        return Ok(None);
    };
    Ok(Some((
        router_payload_type(*rtx_payload_type)?,
        router_payload_type(*payload.pt())?,
    )))
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
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        if let Some((key, value)) = entry.split_once('=') {
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
