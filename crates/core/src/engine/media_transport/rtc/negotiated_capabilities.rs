use o_sfu_rfc::{rtp as rfc_rtp, webrtc as rfc_webrtc};
use o_sfu_router::{
    HeaderExtension as RouterHeaderExtension, MediaCapabilities, MediaCodecCapability,
    MediaKind as RouterMediaKind, RtcpFeedback, RtcpFeedbackKind,
};
use str0m::{
    change::SdpAnswer,
    format::{Codec, PayloadParams},
};

#[must_use]
pub fn client_rtp_capabilities_from_answer(answer_sdp: &str) -> Option<MediaCapabilities> {
    let answer = SdpAnswer::from_sdp_string(answer_sdp).ok()?;
    let mut codecs = Vec::new();
    let mut header_extensions = Vec::new();

    for media_line in &answer.media_lines {
        if media_line.disabled {
            continue;
        }
        let rtp_parameters = media_line.rtp_params();
        let Some(media_kind) = media_kind_label(&rtp_parameters) else {
            continue;
        };
        for payload in &rtp_parameters {
            let codec = project_codec_capability(media_kind, payload);
            if !codecs.contains(&codec) {
                codecs.push(codec);
            }
            if let Some(rtx_codec) = project_rtx_capability(media_kind, payload)
                && !codecs.contains(&rtx_codec)
            {
                codecs.push(rtx_codec);
            }
        }
        for (id, extension) in media_line.extmaps() {
            let header_extension = RouterHeaderExtension::new(
                rfc_webrtc::RtpHeaderExtensionUri::from(extension.as_uri()),
                id,
            );
            if !header_extensions.contains(&header_extension) {
                header_extensions.push(header_extension);
            }
        }
    }

    if codecs.is_empty() {
        return None;
    }

    Some(MediaCapabilities::new(codecs, header_extensions))
}

fn media_kind_label(payloads: &[PayloadParams]) -> Option<RouterMediaKind> {
    payloads
        .iter()
        .find(|payload| payload.spec().codec != Codec::Rtx)
        .or_else(|| payloads.first())
        .map(|payload| {
            if payload.spec().codec.is_audio() {
                RouterMediaKind::Audio
            } else {
                RouterMediaKind::Video
            }
        })
}

fn project_codec_capability(
    media_kind: RouterMediaKind,
    payload: &PayloadParams,
) -> MediaCodecCapability {
    let spec = payload.spec();
    let mut codec =
        MediaCodecCapability::new(media_kind, spec.codec.to_string(), spec.clock_rate.get())
            .with_payload_type(*payload.pt());
    if let Some(channels) = spec.channels {
        codec = codec.with_channels(u16::from(channels));
    }
    codec = apply_codec_parameters(codec, &spec.format.to_string());
    for feedback in rtcp_feedback(payload) {
        codec = codec.with_rtcp_feedback(feedback);
    }
    codec
}

fn project_rtx_capability(
    media_kind: RouterMediaKind,
    payload: &PayloadParams,
) -> Option<MediaCodecCapability> {
    let resend_payload_type = payload.resend()?;
    Some(
        MediaCodecCapability::new(
            media_kind,
            Codec::Rtx.to_string(),
            payload.spec().clock_rate.get(),
        )
        .with_payload_type(*resend_payload_type)
        .with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, payload.pt().to_string()),
    )
}

fn apply_codec_parameters(
    mut codec: MediaCodecCapability,
    format_params: &str,
) -> MediaCodecCapability {
    for entry in format_params
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((key, value)) = entry.split_once('=') else {
            continue;
        };
        codec = codec.with_parameter(key.trim(), value.trim());
    }
    codec
}

fn rtcp_feedback(payload: &PayloadParams) -> Vec<RtcpFeedback> {
    let mut feedback = Vec::new();
    if payload.fb_nack() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::Nack, None));
    }
    if payload.fb_pli() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    }
    if payload.fb_fir() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None));
    }
    if payload.fb_remb() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None));
    }
    if payload.fb_transport_cc() {
        feedback.push(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));
    }
    feedback
}

#[cfg(test)]
#[path = "TESTS/negotiated_capabilities.rs"]
mod tests;
