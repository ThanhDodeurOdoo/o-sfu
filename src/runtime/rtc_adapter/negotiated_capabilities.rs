use o_sfu_rfc::{rtp as rfc_rtp, webrtc as rfc_webrtc};
use o_sfu_router::{
    HeaderExtension as RouterHeaderExtension, MediaCapabilities, MediaCodecCapability,
    MediaKind as RouterMediaKind, RtcpFeedback, RtcpFeedbackKind,
};
use str0m::{
    change::SdpAnswer,
    format::{Codec, PayloadParams},
};

pub(crate) fn client_rtp_capabilities_from_answer(answer_sdp: &str) -> Option<MediaCapabilities> {
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
            .with_preferred_payload_type(*payload.pt());
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
        .with_preferred_payload_type(*resend_payload_type)
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
mod tests {
    use std::collections::BTreeSet;

    use o_sfu_rfc::rtp as rfc_rtp;

    use super::client_rtp_capabilities_from_answer;

    const CHROMIUM_OPTIONAL_CODECS_ANSWER: &str =
        include_str!("testdata/chromium_optional_codecs_answer.sdp");

    #[test]
    fn chromium_answer_projection_keeps_optional_video_profiles_and_rtx_pairs() {
        let projected = client_rtp_capabilities_from_answer(CHROMIUM_OPTIONAL_CODECS_ANSWER);
        assert!(
            projected.is_some(),
            "captured Chromium answer should project into client RTP capabilities"
        );
        let Some(projected) = projected else {
            return;
        };

        let h264_variants = projected
            .codecs()
            .filter(|codec| codec.codec_name() == "H264")
            .map(|codec| {
                let packetization_mode = codec
                    .settings()
                    .find_map(|setting| match setting {
                        o_sfu_router::CodecSetting::H264PacketizationMode(mode) => Some(*mode),
                        _ => None,
                    })
                    .unwrap_or(u8::MAX);
                let profile_level_id = codec
                    .settings()
                    .find_map(|setting| match setting {
                        o_sfu_router::CodecSetting::H264ProfileLevelId(profile_level_id) => {
                            Some(profile_level_id.clone())
                        }
                        _ => None,
                    })
                    .unwrap_or_default();
                (packetization_mode, profile_level_id)
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            h264_variants,
            BTreeSet::from([
                (0, String::from("42001f")),
                (0, String::from("42e01f")),
                (0, String::from("4d001f")),
                (1, String::from("42001f")),
                (1, String::from("42e01f")),
                (1, String::from("4d001f")),
            ])
        );

        let vp9_profiles = projected
            .codecs()
            .filter(|codec| codec.codec_name() == "VP9")
            .map(|codec| {
                codec.parameters().find_map(|(key, value)| {
                    (key == rfc_rtp::fmtp::VP9_PROFILE_ID).then_some(value)
                })
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            vp9_profiles,
            BTreeSet::from([Some(String::from("0")), Some(String::from("2"))])
        );

        let optional_payload_types = projected
            .codecs()
            .filter(|codec| matches!(codec.codec_name(), "H264" | "VP9"))
            .filter_map(o_sfu_router::MediaCodecCapability::payload_type)
            .collect::<BTreeSet<_>>();
        let rtx_associations = projected
            .codecs()
            .filter(|codec| codec.codec_name() == "rtx")
            .filter_map(|codec| {
                codec.parameters().find_map(|(key, value)| {
                    if key != rfc_rtp::fmtp::RTX_ASSOCIATION {
                        return None;
                    }
                    value.parse::<u8>().ok()
                })
            })
            .collect::<BTreeSet<_>>();
        assert!(optional_payload_types.is_subset(&rtx_associations));
    }
}
