use o_sfu_rfc::{rtp, webrtc};
use o_sfu_router::{
    CodecSetting, HeaderExtension, MediaCapabilities, MediaCodecCapability, MediaKind,
    RtcpFeedback, RtcpFeedbackKind,
};

use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    runtime::h264_payloads::{H264_PAYLOAD_SPECS, H264PayloadSpec},
};

const AUDIO_PAYLOAD_TYPE_PCMU: u8 = 0;
const AUDIO_PAYLOAD_TYPE_PCMA: u8 = 8;
const AUDIO_PAYLOAD_TYPE_OPUS: u8 = 111;
const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;
const VIDEO_PAYLOAD_TYPE_H265: u8 = 115;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0: u8 = 116;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_0_RTX: u8 = 117;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2: u8 = 118;
const VIDEO_PAYLOAD_TYPE_VP9_PROFILE_2_RTX: u8 = 119;
const VIDEO_PAYLOAD_TYPE_AV1: u8 = 120;

const HEADER_EXTENSION_ID_MID: u8 = 1;
const HEADER_EXTENSION_ID_ABS_SEND_TIME: u8 = 4;
const HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC: u8 = 5;
const HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL: u8 = 10;

#[must_use]
pub fn router_rtp_capabilities(codec_flags: MediaCodecFlags) -> MediaCapabilities {
    router_rtp_capabilities_with_preferences(codec_flags, CodecPreferences::default())
}

#[must_use]
pub fn router_rtp_capabilities_with_preferences(
    codec_flags: MediaCodecFlags,
    codec_preferences: CodecPreferences,
) -> MediaCapabilities {
    let mut codecs = Vec::new();
    for codec in codec_preferences.audio_order() {
        if codec.enabled_by(codec_flags) {
            push_audio_codec(&mut codecs, codec);
        }
    }
    for codec in codec_preferences.video_order() {
        if codec.enabled_by(codec_flags) {
            push_video_codec(&mut codecs, codec);
        }
    }

    MediaCapabilities::new(codecs, default_header_extensions())
}

fn push_audio_codec(codecs: &mut Vec<MediaCodecCapability>, codec: AudioCodecPreference) {
    match codec {
        AudioCodecPreference::Opus => codecs.push(opus_codec_capability()),
        AudioCodecPreference::Pcmu => codecs.push(audio_codec_capability(
            rtp::CodecName::from("PCMU"),
            AUDIO_PAYLOAD_TYPE_PCMU,
            8_000,
            None,
        )),
        AudioCodecPreference::Pcma => codecs.push(audio_codec_capability(
            rtp::CodecName::from("PCMA"),
            AUDIO_PAYLOAD_TYPE_PCMA,
            8_000,
            None,
        )),
    }
}

fn push_video_codec(codecs: &mut Vec<MediaCodecCapability>, codec: VideoCodecPreference) {
    match codec {
        VideoCodecPreference::Vp8 => {
            codecs.push(video_codec_capability(
                rtp::CodecName::Vp8,
                VIDEO_PAYLOAD_TYPE_VP8,
            ));
        }
        VideoCodecPreference::H264 => push_h264_codec_capabilities(codecs),
        VideoCodecPreference::H265 => codecs.push(video_codec_capability(
            rtp::CodecName::from("H265"),
            VIDEO_PAYLOAD_TYPE_H265,
        )),
        VideoCodecPreference::Vp9 => codecs.extend(vp9_codec_capabilities()),
        VideoCodecPreference::Av1 => codecs.push(video_codec_capability(
            rtp::CodecName::from("AV1"),
            VIDEO_PAYLOAD_TYPE_AV1,
        )),
    }
}

fn default_header_extensions() -> Vec<HeaderExtension> {
    vec![
        HeaderExtension::new(webrtc::RtpHeaderExtensionUri::Mid, HEADER_EXTENSION_ID_MID),
        HeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::AbsSendTime,
            HEADER_EXTENSION_ID_ABS_SEND_TIME,
        ),
        HeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::TransportWideCcDraft01,
            HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC,
        ),
        HeaderExtension::new(
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
) -> MediaCodecCapability {
    let codec = MediaCodecCapability::new(MediaKind::Audio, codec_name, clock_rate)
        .with_preferred_payload_type(payload_type)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));
    match channels {
        Some(channel_count) => codec.with_channels(channel_count),
        None => codec,
    }
}

fn opus_codec_capability() -> MediaCodecCapability {
    audio_codec_capability(
        rtp::CodecName::Opus,
        AUDIO_PAYLOAD_TYPE_OPUS,
        48_000,
        Some(2),
    )
    .with_setting(CodecSetting::UseInBandFec(true))
}

fn video_codec_capability(codec_name: rtp::CodecName, payload_type: u8) -> MediaCodecCapability {
    MediaCodecCapability::new(MediaKind::Video, codec_name, 90_000)
        .with_preferred_payload_type(payload_type)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn push_h264_codec_capabilities(codecs: &mut Vec<MediaCodecCapability>) {
    codecs.extend(
        H264_PAYLOAD_SPECS
            .iter()
            .copied()
            .map(h264_codec_capability),
    );
}

fn h264_codec_capability(spec: H264PayloadSpec) -> MediaCodecCapability {
    video_codec_capability(rtp::CodecName::H264, spec.payload_type())
        .with_setting(CodecSetting::H264PacketizationMode(
            spec.packetization_mode().fmtp_value(),
        ))
        .with_setting(CodecSetting::H264ProfileLevelId(
            spec.profile_level_id_parameter(),
        ))
}

fn vp9_codec_capabilities() -> [MediaCodecCapability; 4] {
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

fn vp9_codec_capability(payload_type: u8, profile_id: u8) -> MediaCodecCapability {
    video_codec_capability(rtp::CodecName::from("VP9"), payload_type)
        .with_parameter(rtp::fmtp::VP9_PROFILE_ID, profile_id.to_string())
}

fn video_rtx_codec_capability(
    payload_type: u8,
    associated_payload_type: u8,
) -> MediaCodecCapability {
    MediaCodecCapability::new(MediaKind::Video, rtp::CodecName::Rtx, 90_000)
        .with_preferred_payload_type(payload_type)
        .with_setting(CodecSetting::RtxAssociation(associated_payload_type.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use o_sfu_router::CodecSetting;

    use super::{router_rtp_capabilities, router_rtp_capabilities_with_preferences};
    use crate::{CodecPreferences, MediaCodecFlags, VideoCodecPreference};

    #[test]
    fn default_router_capabilities_match_the_browser_codec_baseline() {
        let capabilities = router_rtp_capabilities(MediaCodecFlags::default());
        let codec_names = capabilities
            .codecs()
            .map(|codec| codec.codec_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(codec_names, vec!["opus", "VP8"]);
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
                    String::from("H264"),
                ][..]
            )
        );
        let h264_variants = capabilities
            .codecs()
            .filter(|codec| codec.codec_name() == "H264")
            .map(|codec| {
                let payload_type = codec.payload_type().unwrap_or(u8::MAX);
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
                (payload_type, packetization_mode, profile_level_id)
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            h264_variants,
            BTreeSet::from([
                (35, 0, String::from("4d001f")),
                (108, 1, String::from("42e01f")),
                (114, 1, String::from("64001f")),
                (123, 1, String::from("4d001f")),
                (124, 0, String::from("42e01f")),
                (125, 0, String::from("42001f")),
                (127, 1, String::from("42001f")),
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
        assert!(!rtx_associations.contains(&96));
        assert!(!rtx_associations.contains(&35));
        assert!(!rtx_associations.contains(&108));
        assert!(!rtx_associations.contains(&114));
        assert!(!rtx_associations.contains(&123));
        assert!(!rtx_associations.contains(&124));
        assert!(!rtx_associations.contains(&125));
        assert!(!rtx_associations.contains(&127));
        assert!(rtx_associations.contains(&116));
        assert!(rtx_associations.contains(&118));
    }

    #[test]
    fn router_capabilities_follow_configured_codec_preferences() {
        let capabilities = router_rtp_capabilities_with_preferences(
            MediaCodecFlags::default().with_h264(true).with_vp9(true),
            CodecPreferences::default()
                .with_video_order(&[VideoCodecPreference::H264, VideoCodecPreference::Vp9]),
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
                    String::from("H264"),
                    String::from("H264"),
                    String::from("H264"),
                ][..]
            )
        );
    }
}
