use o_sfu_router::{
    MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCapabilities, RtpCodecCapability,
    RtpHeaderExtension,
};

use crate::rfc::webrtc;

const AUDIO_CODEC_OPUS: &str = "opus";
const VIDEO_CODEC_VP8: &str = "VP8";
const VIDEO_CODEC_RTX: &str = "rtx";

const AUDIO_PAYLOAD_TYPE_OPUS: u8 = 111;
const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;
const VIDEO_PAYLOAD_TYPE_VP8_RTX: u8 = 97;

const RTP_PARAMETER_APT: &str = "apt";
const RTP_PARAMETER_USE_IN_BAND_FEC: &str = "useinbandfec";

const HEADER_EXTENSION_ID_MID: u8 = 1;
const HEADER_EXTENSION_ID_ABS_SEND_TIME: u8 = 4;
const HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC: u8 = 5;
const HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL: u8 = 10;

pub(super) fn default_router_rtp_capabilities() -> RtpCapabilities {
    RtpCapabilities::new(
        vec![
            default_audio_codec_capability(),
            default_video_codec_capability(),
            default_video_rtx_codec_capability(),
        ],
        vec![
            RtpHeaderExtension::new(
                webrtc::rtp_header_extension_uri::MID,
                HEADER_EXTENSION_ID_MID,
            ),
            RtpHeaderExtension::new(
                webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
                HEADER_EXTENSION_ID_ABS_SEND_TIME,
            ),
            RtpHeaderExtension::new(
                webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
                HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC,
            ),
            RtpHeaderExtension::new(
                webrtc::rtp_header_extension_uri::SSRC_AUDIO_LEVEL,
                HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL,
            ),
        ],
    )
}

fn default_audio_codec_capability() -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Audio, AUDIO_CODEC_OPUS, 48_000)
        .with_preferred_payload_type(AUDIO_PAYLOAD_TYPE_OPUS)
        .with_channels(2)
        .with_parameter(RTP_PARAMETER_USE_IN_BAND_FEC, "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn default_video_codec_capability() -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Video, VIDEO_CODEC_VP8, 90_000)
        .with_preferred_payload_type(VIDEO_PAYLOAD_TYPE_VP8)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn default_video_rtx_codec_capability() -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Video, VIDEO_CODEC_RTX, 90_000)
        .with_preferred_payload_type(VIDEO_PAYLOAD_TYPE_VP8_RTX)
        .with_parameter(RTP_PARAMETER_APT, VIDEO_PAYLOAD_TYPE_VP8.to_string())
}
