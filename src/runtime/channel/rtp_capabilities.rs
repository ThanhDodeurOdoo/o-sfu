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
const VIDEO_PAYLOAD_TYPE_H264: u8 = 102;
const VIDEO_PAYLOAD_TYPE_H265: u8 = 104;
const VIDEO_PAYLOAD_TYPE_VP9: u8 = 106;
const VIDEO_PAYLOAD_TYPE_AV1: u8 = 108;

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
        codecs.push(video_codec_capability(
            rtp::CodecName::H264,
            VIDEO_PAYLOAD_TYPE_H264,
        ));
    }
    if codec_flags.h265_enabled() {
        codecs.push(video_codec_capability(
            rtp::CodecName::from("H265"),
            VIDEO_PAYLOAD_TYPE_H265,
        ));
    }
    if codec_flags.vp9_enabled() {
        codecs.push(video_codec_capability(
            rtp::CodecName::from("VP9"),
            VIDEO_PAYLOAD_TYPE_VP9,
        ));
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

fn video_rtx_codec_capability(payload_type: u8, associated_payload_type: u8) -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Video, rtp::CodecName::Rtx, 90_000)
        .with_preferred_payload_type(payload_type)
        .with_setting(CodecSetting::RtxAssociation(associated_payload_type.into()))
}

#[cfg(test)]
mod tests {
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
        let capabilities =
            router_rtp_capabilities(MediaCodecFlags::default().with_pcmu(true).with_h264(true));
        let codec_names = capabilities
            .codecs()
            .map(|codec| codec.codec_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(codec_names, vec!["opus", "PCMU", "VP8", "rtx", "H264"]);
    }
}
