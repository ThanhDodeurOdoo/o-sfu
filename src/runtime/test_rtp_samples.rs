use o_sfu_router::{
    CodecSetting, MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCapabilities, RtpCodecCapability,
    RtpCodecParameters, RtpEncoding, RtpHeaderExtension, RtpParameters,
};

use crate::rfc::{rtp, webrtc};

const AUDIO_PAYLOAD_TYPE_OPUS: u8 = 111;
const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;
const VIDEO_PAYLOAD_TYPE_VP8_RTX: u8 = 97;

const HEADER_EXTENSION_ID_MID: u8 = 1;
const HEADER_EXTENSION_ID_ABS_SEND_TIME: u8 = 4;
const HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC: u8 = 5;
const HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL: u8 = 10;

pub(crate) fn sample_client_rtp_capabilities() -> RtpCapabilities {
    RtpCapabilities::new(
        vec![
            opus_codec_capability(),
            video_codec_capability(),
            video_rtx_codec_capability(),
        ],
        default_header_extensions(),
    )
}

pub(crate) fn sample_client_rtp_capabilities_without_video_rtx() -> RtpCapabilities {
    RtpCapabilities::new(
        vec![
            opus_codec_capability(),
            video_codec_capability_without_transport_cc(),
        ],
        vec![RtpHeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::Mid,
            HEADER_EXTENSION_ID_MID,
        )],
    )
}

pub(crate) fn sample_audio_rtp_parameters(ssrc: u32) -> RtpParameters {
    RtpParameters::new(
        vec![opus_codec_parameters()],
        vec![
            RtpHeaderExtension::new(webrtc::RtpHeaderExtensionUri::Mid, HEADER_EXTENSION_ID_MID),
            RtpHeaderExtension::new(
                webrtc::RtpHeaderExtensionUri::SsrcAudioLevel,
                HEADER_EXTENSION_ID_SSRC_AUDIO_LEVEL,
            ),
        ],
        vec![RtpEncoding::new().with_ssrc(ssrc)],
    )
}

pub(crate) fn sample_video_rtp_parameters(mid: Option<&str>, ssrc: u32) -> RtpParameters {
    with_optional_mid(
        RtpParameters::new(
            vec![video_codec_parameters(), video_rtx_codec_parameters()],
            vec![
                RtpHeaderExtension::new(
                    webrtc::RtpHeaderExtensionUri::Mid,
                    HEADER_EXTENSION_ID_MID,
                ),
                RtpHeaderExtension::new(
                    webrtc::RtpHeaderExtensionUri::AbsSendTime,
                    HEADER_EXTENSION_ID_ABS_SEND_TIME,
                ),
                RtpHeaderExtension::new(
                    webrtc::RtpHeaderExtensionUri::TransportWideCcDraft01,
                    HEADER_EXTENSION_ID_TRANSPORT_WIDE_CC,
                ),
            ],
            vec![RtpEncoding::new().with_ssrc(ssrc)],
        ),
        mid,
    )
}

pub(crate) fn sample_simulcast_video_rtp_parameters(mid: Option<&str>) -> RtpParameters {
    with_optional_mid(
        RtpParameters::new(
            vec![video_codec_parameters()],
            vec![RtpHeaderExtension::new(
                webrtc::RtpHeaderExtensionUri::Mid,
                HEADER_EXTENSION_ID_MID,
            )],
            vec![
                RtpEncoding::new()
                    .with_ssrc(31_001)
                    .with_rid("lo")
                    .with_max_bitrate(150_000),
                RtpEncoding::new()
                    .with_ssrc(31_002)
                    .with_rid("hi")
                    .with_max_bitrate(900_000),
            ],
        ),
        mid,
    )
}

fn with_optional_mid(parameters: RtpParameters, mid: Option<&str>) -> RtpParameters {
    match mid {
        Some(mid) => parameters.with_mid(mid.to_owned()),
        None => parameters,
    }
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

fn opus_codec_capability() -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Audio, rtp::CodecName::Opus, 48_000)
        .with_preferred_payload_type(AUDIO_PAYLOAD_TYPE_OPUS)
        .with_channels(2)
        .with_setting(CodecSetting::UseInBandFec(true))
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn video_codec_capability() -> RtpCodecCapability {
    video_codec_capability_with_feedback(true)
}

fn video_codec_capability_without_transport_cc() -> RtpCodecCapability {
    video_codec_capability_with_feedback(false)
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
}

fn video_codec_capability_with_feedback(include_transport_cc: bool) -> RtpCodecCapability {
    let codec = RtpCodecCapability::new(MediaKind::Video, rtp::CodecName::Vp8, 90_000)
        .with_preferred_payload_type(VIDEO_PAYLOAD_TYPE_VP8)
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

fn video_rtx_codec_capability() -> RtpCodecCapability {
    RtpCodecCapability::new(MediaKind::Video, rtp::CodecName::Rtx, 90_000)
        .with_preferred_payload_type(VIDEO_PAYLOAD_TYPE_VP8_RTX)
        .with_setting(CodecSetting::RtxAssociation(VIDEO_PAYLOAD_TYPE_VP8.into()))
}

fn opus_codec_parameters() -> RtpCodecParameters {
    RtpCodecParameters::new(
        MediaKind::Audio,
        rtp::CodecName::Opus,
        AUDIO_PAYLOAD_TYPE_OPUS,
        48_000,
    )
    .with_channels(2)
    .with_setting(CodecSetting::UseInBandFec(true))
    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
}

fn video_codec_parameters() -> RtpCodecParameters {
    RtpCodecParameters::new(
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

fn video_rtx_codec_parameters() -> RtpCodecParameters {
    RtpCodecParameters::new(
        MediaKind::Video,
        rtp::CodecName::Rtx,
        VIDEO_PAYLOAD_TYPE_VP8_RTX,
        90_000,
    )
    .with_setting(CodecSetting::RtxAssociation(VIDEO_PAYLOAD_TYPE_VP8.into()))
}
