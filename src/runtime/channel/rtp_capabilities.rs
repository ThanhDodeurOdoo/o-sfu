use o_sfu_router::{
    CodecSetting, MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCapabilities, RtpCodecCapability,
    RtpHeaderExtension,
};

use crate::config::MediaCodecFlags;
use crate::rfc::{rtp, webrtc};

const AUDIO_PAYLOAD_TYPE_PCMU: u8 = 0;
const AUDIO_PAYLOAD_TYPE_PCMA: u8 = 8;
const AUDIO_PAYLOAD_TYPE_OPUS: u8 = 111;
const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;
const VIDEO_PAYLOAD_TYPE_VP8_RTX: u8 = 97;
const VIDEO_PAYLOAD_TYPE_H264_BASELINE_PACKETIZED: u8 = 102;
const VIDEO_PAYLOAD_TYPE_H264_BASELINE_PACKETIZED_RTX: u8 = 103;
const VIDEO_PAYLOAD_TYPE_H264_BASELINE_NON_INTERLEAVED: u8 = 104;
const VIDEO_PAYLOAD_TYPE_H264_BASELINE_NON_INTERLEAVED_RTX: u8 = 105;
const VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_PACKETIZED: u8 = 106;
const VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_PACKETIZED_RTX: u8 = 107;
const VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_NON_INTERLEAVED: u8 = 108;
const VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_NON_INTERLEAVED_RTX: u8 = 109;
const VIDEO_PAYLOAD_TYPE_H264_MAIN_PACKETIZED: u8 = 110;
const VIDEO_PAYLOAD_TYPE_H264_MAIN_PACKETIZED_RTX: u8 = 112;
const VIDEO_PAYLOAD_TYPE_H264_MAIN_NON_INTERLEAVED: u8 = 113;
const VIDEO_PAYLOAD_TYPE_H264_MAIN_NON_INTERLEAVED_RTX: u8 = 114;
const VIDEO_PAYLOAD_TYPE_H265: u8 = 115;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0: u8 = 116;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0_RTX: u8 = 117;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2: u8 = 118;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2_RTX: u8 = 119;
const VIDEO_PAYLOAD_TYPE_AV1: u8 = 120;

const H264_PROFILE_LEVEL_BASELINE: &str = "42001f";
const H264_PROFILE_LEVEL_CONSTRAINED_BASELINE: &str = "42e01f";
const H264_PROFILE_LEVEL_MAIN: &str = "4d001f";

const HEADER_EXTENSION_ID_MID: u8 = 1;
const HEADER_EXTENSION_ID_ABS_SEND_TIME: u8 = 4;
const HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC: u8 = 5;
const HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL: u8 = 10;

pub(crate) fn router_rtp_capabilities(codec_flags: MediaCodecFlags) -> RtpCapabilities {
    let mut codecs = Vec::new();
    if codec_flags.opus_enabled() {
        codecs.push(opus_codec_capability());
    }
    if codec_flags.pcmu_enabled() {
        codecs.push(audio_codec_capability(
            rtp::CodecName::from("PCMU"),
            AUDIO_PAYLOAD_TYPE_PCMU,
            8_000,
            None,
        ));
    }
    if codec_flags.pcma_enabled() {
        codecs.push(audio_codec_capability(
            rtp::CodecName::from("PCMA"),
            AUDIO_PAYLOAD_TYPE_PCMA,
            8_000,
            None,
        ));
    }
    if codec_flags.vp8_enabled() {
        codecs.push(video_codec_capability(
            rtp::CodecName::Vp8,
            VIDEO_PAYLOAD_TYPE_VP8,
        ));
        codecs.push(video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_VP8_RTX,
            VIDEO_PAYLOAD_TYPE_VP8,
        ));
    }
    if codec_flags.h264_enabled() {
        codecs.extend(h264_codec_capabilities());
    }
    if codec_flags.h265_enabled() {
        codecs.push(video_codec_capability(
            rtp::CodecName::from("H265"),
            VIDEO_PAYLOAD_TYPE_H265,
        ));
    }
    if codec_flags.vp9_enabled() {
        codecs.extend(vp9_codec_capabilities());
    }
    if codec_flags.av1_enabled() {
        codecs.push(video_codec_capability(
            rtp::CodecName::from("AV1"),
            VIDEO_PAYLOAD_TYPE_AV1,
        ));
    }

    RtpCapabilities::new(codecs, default_header_extensions())
}

fn default_header_extensions() -> Vec<RtpHeaderExtension> {
    vec![
        RtpHeaderExtension::new(webrtc::RtpHeaderExtensionUri::Mid, HEADER_EXTENSION_ID_MID),
        RtpHeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::AbsSendTime,
            HEADER_EXTENSION_ID_ABS_SEND_TIME,
        ),
        RtpHeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::TransportWideCcDraft01,
            HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC,
        ),
        RtpHeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::SsrcAudioLevel,
            HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL,
        ),
    ]
}

fn audio_codec_capability(
    codec_name: rtp::CodecName,
    payload_type: u8,
    clock_rate: u32,
    channels: Option<u16>,
) -> RtpCodecCapability {
    let codec = RtpCodecCapability::new(MediaKind::Audio, codec_name, clock_rate)
        .with_preferred_payload_type(payload_type)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));
    match channels {
        Some(channel_count) => codec.with_channels(channel_count),
        None => codec,
    }
}

fn opus_codec_capability() -> RtpCodecCapability {
    audio_codec_capability(
        rtp::CodecName::Opus,
        AUDIO_PAYLOAD_TYPE_OPUS,
        48_000,
        Some(2),
    )
    .with_setting(CodecSetting::UseInBandFec(true))
}

fn video_codec_capability(codec_name: rtp::CodecName, payload_type: u8) -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Video, codec_name, 90_000)
        .with_preferred_payload_type(payload_type)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn h264_codec_capabilities() -> [RtpCodecCapability; 12] {
    [
        h264_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_BASELINE_PACKETIZED,
            1,
            H264_PROFILE_LEVEL_BASELINE,
        ),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_BASELINE_PACKETIZED_RTX,
            VIDEO_PAYLOAD_TYPE_H264_BASELINE_PACKETIZED,
        ),
        h264_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_BASELINE_NON_INTERLEAVED,
            0,
            H264_PROFILE_LEVEL_BASELINE,
        ),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_BASELINE_NON_INTERLEAVED_RTX,
            VIDEO_PAYLOAD_TYPE_H264_BASELINE_NON_INTERLEAVED,
        ),
        h264_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_PACKETIZED,
            1,
            H264_PROFILE_LEVEL_CONSTRAINED_BASELINE,
        ),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_PACKETIZED_RTX,
            VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_PACKETIZED,
        ),
        h264_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_NON_INTERLEAVED,
            0,
            H264_PROFILE_LEVEL_CONSTRAINED_BASELINE,
        ),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_NON_INTERLEAVED_RTX,
            VIDEO_PAYLOAD_TYPE_H264_CONSTRAINED_NON_INTERLEAVED,
        ),
        h264_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_MAIN_PACKETIZED,
            1,
            H264_PROFILE_LEVEL_MAIN,
        ),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_MAIN_PACKETIZED_RTX,
            VIDEO_PAYLOAD_TYPE_H264_MAIN_PACKETIZED,
        ),
        h264_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_MAIN_NON_INTERLEAVED,
            0,
            H264_PROFILE_LEVEL_MAIN,
        ),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_H264_MAIN_NON_INTERLEAVED_RTX,
            VIDEO_PAYLOAD_TYPE_H264_MAIN_NON_INTERLEAVED,
        ),
    ]
}

fn h264_codec_capability(
    payload_type: u8,
    packetization_mode: u8,
    profile_level_id: &str,
) -> RtpCodecCapability {
    video_codec_capability(rtp::CodecName::H264, payload_type)
        .with_setting(CodecSetting::H264PacketizationMode(packetization_mode))
        .with_setting(CodecSetting::H264ProfileLevelId(
            profile_level_id.to_owned(),
        ))
}

fn vp9_codec_capabilities() -> [RtpCodecCapability; 4] {
    [
        vp9_codec_capability(VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0, 0),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0_RTX,
            VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0,
        ),
        vp9_codec_capability(VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2, 2),
        video_rtx_codec_capability(
            VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2_RTX,
            VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2,
        ),
    ]
}

fn vp9_codec_capability(payload_type: u8, profile_id: u8) -> RtpCodecCapability {
    video_codec_capability(rtp::CodecName::from("VP9"), payload_type)
        .with_parameter(rtp::fmtp::VP9_PROFILE_ID, profile_id.to_string())
}

fn video_rtx_codec_capability(payload_type: u8, associated_payload_type: u8) -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Video, rtp::CodecName::Rtx, 90_000)
        .with_preferred_payload_type(payload_type)
        .with_setting(CodecSetting::RtxAssociation(associated_payload_type.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use o_sfu_router::CodecSetting;

    use crate::config::MediaCodecFlags;

    use super::router_rtp_capabilities;

    #[test]
    fn default_router_capabilities_match_the_browser_codec_baseline() {
        let capabilities = router_rtp_capabilities(MediaCodecFlags::default());
        let codec_names = capabilities
            .codecs()
            .map(|codec| codec.codec_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(codec_names, vec!["opus", "VP8", "rtx"]);
    }

    #[test]
    fn router_capabilities_include_enabled_optional_codecs() {
        let capabilities = router_rtp_capabilities(
            MediaCodecFlags::default()
                .with_pcmu(true)
                .with_h264(true)
                .with_vp9(true),
        );
        let codec_names = capabilities
            .codecs()
            .map(|codec| codec.codec_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            codec_names.get(..4),
            Some(
                &[
                    String::from("opus"),
                    String::from("PCMU"),
                    String::from("VP8"),
                    String::from("rtx"),
                ][..]
            )
        );
        let h264_variants = capabilities
            .codecs()
            .filter(|codec| codec.codec_name() == "H264")
            .map(|codec| {
                let packetization_mode = codec
                    .settings()
                    .find_map(|setting| match setting {
                        CodecSetting::H264PacketizationMode(mode) => Some(*mode),
                        _ => None,
                    })
                    .unwrap_or(u8::MAX);
                let profile_level_id = codec
                    .settings()
                    .find_map(|setting| match setting {
                        CodecSetting::H264ProfileLevelId(profile_level_id) => {
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
        let vp9_profiles = capabilities
            .codecs()
            .filter(|codec| codec.codec_name() == "VP9")
            .map(|codec| {
                codec.settings().find_map(|setting| match setting {
                    CodecSetting::Vp9ProfileId(profile_id) => Some(*profile_id),
                    _ => None,
                })
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(vp9_profiles, BTreeSet::from([Some(0), Some(2)]));
        let rtx_associations = capabilities
            .codecs()
            .filter(|codec| codec.codec_name() == "rtx")
            .filter_map(|codec| {
                codec.settings().find_map(|setting| match setting {
                    CodecSetting::RtxAssociation(payload_type) => Some(payload_type.value()),
                    _ => None,
                })
            })
            .collect::<BTreeSet<_>>();
        assert!(rtx_associations.contains(&96));
        assert!(rtx_associations.contains(&102));
        assert!(rtx_associations.contains(&104));
        assert!(rtx_associations.contains(&106));
        assert!(rtx_associations.contains(&108));
        assert!(rtx_associations.contains(&110));
        assert!(rtx_associations.contains(&113));
        assert!(rtx_associations.contains(&116));
        assert!(rtx_associations.contains(&118));
    }
}
