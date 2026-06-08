use o_sfu_rfc::{rtp, webrtc};
use o_sfu_router::{
    CodecSetting, HeaderExtension, MediaCapabilities, MediaCodecCapability, MediaKind,
    RtcpFeedback, RtcpFeedbackKind,
};

use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    engine::h264_payloads::{H264_PAYLOAD_SPECS, H264PayloadSpec},
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
        .with_payload_type(payload_type)
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
        .with_payload_type(payload_type)
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
        .with_payload_type(payload_type)
        .with_setting(CodecSetting::RtxAssociation(associated_payload_type.into()))
}

#[cfg(test)]
#[path = "TESTS/rtp_capabilities.rs"]
mod tests;
