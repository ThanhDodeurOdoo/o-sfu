use o_sfu_rfc::{rtp as rfc_rtp, webrtc};
use o_sfu_router::{
    CodecSetting, HeaderExtension, MediaCapabilities, MediaCodecCapability, MediaKind, PayloadType,
    RtcpFeedback, RtcpFeedbackKind,
};

use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    engine::rtp::{
        self,
        h264::{H264_PAYLOAD_SPECS, H264PayloadSpec},
        payload_type,
    },
};

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
            rfc_rtp::CodecName::from("PCMU"),
            payload_type::PCMU,
            8_000,
            None,
        )),
        AudioCodecPreference::Pcma => codecs.push(audio_codec_capability(
            rfc_rtp::CodecName::from("PCMA"),
            payload_type::PCMA,
            8_000,
            None,
        )),
    }
}

fn push_video_codec(codecs: &mut Vec<MediaCodecCapability>, codec: VideoCodecPreference) {
    match codec {
        VideoCodecPreference::Vp8 => {
            codecs.push(video_codec_capability(
                rfc_rtp::CodecName::Vp8,
                payload_type::VP8,
            ));
        }
        VideoCodecPreference::H264 => push_h264_codec_capabilities(codecs),
        VideoCodecPreference::H265 => codecs.push(video_codec_capability(
            rfc_rtp::CodecName::from("H265"),
            payload_type::H265,
        )),
        VideoCodecPreference::Vp9 => codecs.extend(vp9_codec_capabilities()),
        VideoCodecPreference::Av1 => codecs.push(video_codec_capability(
            rfc_rtp::CodecName::from("AV1"),
            payload_type::AV1,
        )),
    }
}

fn default_header_extensions() -> Vec<HeaderExtension> {
    vec![
        HeaderExtension::new(webrtc::RtpHeaderExtensionUri::Mid, rtp::MID_EXTENSION_ID),
        HeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::AbsSendTime,
            rtp::ABS_SEND_TIME_EXTENSION_ID,
        ),
        HeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::TransportWideCcDraft01,
            rtp::TRANSPORT_WIDE_CC_EXTENSION_ID,
        ),
        HeaderExtension::new(
            webrtc::RtpHeaderExtensionUri::SsrcAudioLevel,
            rtp::SSRC_AUDIO_LEVEL_EXTENSION_ID,
        ),
    ]
}

fn audio_codec_capability(
    codec_name: rfc_rtp::CodecName,
    payload_type: PayloadType,
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
        rfc_rtp::CodecName::Opus,
        payload_type::OPUS,
        48_000,
        Some(2),
    )
    .with_setting(CodecSetting::UseInBandFec(true))
}

fn video_codec_capability(
    codec_name: rfc_rtp::CodecName,
    payload_type: PayloadType,
) -> MediaCodecCapability {
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
    video_codec_capability(rfc_rtp::CodecName::H264, spec.payload_type())
        .with_setting(CodecSetting::H264PacketizationMode(
            spec.packetization_mode(),
        ))
        .with_setting(CodecSetting::H264ProfileLevelId(
            spec.profile_level_id().fmtp_value(),
        ))
}

fn vp9_codec_capabilities() -> [MediaCodecCapability; 4] {
    [
        vp9_codec_capability(payload_type::VP9_PROFILE_0, 0),
        video_rtx_codec_capability(payload_type::VP9_PROFILE_0_RTX, payload_type::VP9_PROFILE_0),
        vp9_codec_capability(payload_type::VP9_PROFILE_2, 2),
        video_rtx_codec_capability(payload_type::VP9_PROFILE_2_RTX, payload_type::VP9_PROFILE_2),
    ]
}

fn vp9_codec_capability(payload_type: PayloadType, profile_id: u8) -> MediaCodecCapability {
    video_codec_capability(rfc_rtp::CodecName::from("VP9"), payload_type)
        .with_parameter(rfc_rtp::fmtp::VP9_PROFILE_ID, profile_id.to_string())
}

fn video_rtx_codec_capability(
    payload_type: PayloadType,
    associated_payload_type: PayloadType,
) -> MediaCodecCapability {
    MediaCodecCapability::new(MediaKind::Video, rfc_rtp::CodecName::Rtx, 90_000)
        .with_payload_type(payload_type)
        .with_setting(CodecSetting::RtxAssociation(associated_payload_type))
}

#[cfg(test)]
#[path = "TESTS/rtp_capabilities.rs"]
mod tests;
