use o_sfu_rfc::{rtp, webrtc};

use crate::{
    MediaKind,
    rtp::{
        CodecSetting, HeaderExtension, MediaCapabilities, MediaCodecCapability, MediaFormat,
        MediaStream, PayloadType, RtcpFeedback, RtcpFeedbackKind, StreamBinding,
    },
};

const AUDIO_PAYLOAD_TYPE_OPUS: PayloadType = PayloadType::new(111);
const VIDEO_PAYLOAD_TYPE_VP8: PayloadType = PayloadType::new(96);
const VIDEO_PAYLOAD_TYPE_VP8_RTX: PayloadType = PayloadType::new(97);

const HEADER_EXTENSION_ID_MID: u8 = 1;
const HEADER_EXTENSION_ID_ABS_SEND_TIME: u8 = 4;
const HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC: u8 = 5;
const HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL: u8 = 10;

#[must_use]
pub fn sample_client_rtp_capabilities() -> MediaCapabilities {
    MediaCapabilities::new(
        vec![
            opus_codec_capability(),
            video_codec_capability(),
            video_rtx_codec_capability(),
        ],
        default_header_extensions(),
    )
}

#[must_use]
pub fn sample_client_rtp_capabilities_without_video_rtx() -> MediaCapabilities {
    MediaCapabilities::new(
        vec![
            opus_codec_capability(),
            video_codec_capability_without_transport_cc(),
        ],
        vec![HeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::Mid,
            HEADER_EXTENSION_ID_MID,
        )],
    )
}

#[must_use]
pub fn sample_audio_rtp_parameters(ssrc: u32) -> MediaStream {
    MediaStream::new(
        vec![opus_codec_parameters()],
        vec![
            HeaderExtension::new(webrtc::RtpHeaderExtensionUri::Mid, HEADER_EXTENSION_ID_MID),
            HeaderExtension::new(
                webrtc::RtpHeaderExtensionUri::SsrcAudioLevel,
                HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL,
            ),
        ],
        vec![StreamBinding::new().with_ssrc(ssrc)],
    )
}

#[must_use]
pub fn sample_video_rtp_parameters(mid: Option<&str>, ssrc: u32) -> MediaStream {
    with_optional_mid(
        MediaStream::new(
            vec![video_codec_parameters(), video_rtx_codec_parameters()],
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
            ],
            vec![StreamBinding::new().with_ssrc(ssrc)],
        ),
        mid,
    )
}

#[must_use]
pub fn sample_simulcast_video_rtp_parameters(mid: Option<&str>) -> MediaStream {
    with_optional_mid(
        MediaStream::new(
            vec![video_codec_parameters()],
            vec![HeaderExtension::new(
                webrtc::RtpHeaderExtensionUri::Mid,
                HEADER_EXTENSION_ID_MID,
            )],
            vec![
                StreamBinding::new()
                    .with_ssrc(31_001)
                    .with_rid("lo")
                    .with_max_bitrate(150_000),
                StreamBinding::new()
                    .with_ssrc(31_002)
                    .with_rid("hi")
                    .with_max_bitrate(900_000),
            ],
        ),
        mid,
    )
}

fn with_optional_mid(stream: MediaStream, mid: Option<&str>) -> MediaStream {
    match mid {
        Some(mid) => stream.with_mid(mid.to_owned()),
        None => stream,
    }
}

fn video_codec_parameters() -> MediaFormat {
    MediaFormat::new(
        MediaKind::Video,
        rtp::CodecName::Vp8,
        VIDEO_PAYLOAD_TYPE_VP8,
        90_000,
    )
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None))
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
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

fn opus_codec_capability() -> MediaCodecCapability {
    MediaCodecCapability::new(MediaKind::Audio, rtp::CodecName::Opus, 48_000)
        .with_payload_type(AUDIO_PAYLOAD_TYPE_OPUS)
        .with_channels(2)
        .with_setting(CodecSetting::UseInBandFec(true))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn video_codec_capability() -> MediaCodecCapability {
    video_codec_capability_with_feedback(true)
}

fn video_codec_capability_without_transport_cc() -> MediaCodecCapability {
    video_codec_capability_with_feedback(false)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
}

fn video_codec_capability_with_feedback(include_transport_cc: bool) -> MediaCodecCapability {
    let codec = MediaCodecCapability::new(MediaKind::Video, rtp::CodecName::Vp8, 90_000)
        .with_payload_type(VIDEO_PAYLOAD_TYPE_VP8)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None));
    if include_transport_cc {
        codec.with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
    } else {
        codec
    }
}

fn video_rtx_codec_capability() -> MediaCodecCapability {
    MediaCodecCapability::new(MediaKind::Video, rtp::CodecName::Rtx, 90_000)
        .with_payload_type(VIDEO_PAYLOAD_TYPE_VP8_RTX)
        .with_setting(CodecSetting::RtxAssociation(VIDEO_PAYLOAD_TYPE_VP8))
}

fn opus_codec_parameters() -> MediaFormat {
    MediaFormat::new(
        MediaKind::Audio,
        rtp::CodecName::Opus,
        AUDIO_PAYLOAD_TYPE_OPUS,
        48_000,
    )
    .with_channels(2)
    .with_setting(CodecSetting::UseInBandFec(true))
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn video_rtx_codec_parameters() -> MediaFormat {
    MediaFormat::new(
        MediaKind::Video,
        rtp::CodecName::Rtx,
        VIDEO_PAYLOAD_TYPE_VP8_RTX,
        90_000,
    )
    .with_setting(CodecSetting::RtxAssociation(VIDEO_PAYLOAD_TYPE_VP8))
}
