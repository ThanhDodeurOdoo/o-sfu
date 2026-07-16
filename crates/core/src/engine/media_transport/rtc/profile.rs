use o_sfu_rfc::rtp::h264::{LevelIdc, PacketizationMode, Profile, ProfileLevelId};
use o_sfu_router::{MediaKind as RouterMediaKind, rtp::MediaCapabilities};
use str0m::{
    Rtc, RtcConfig,
    format::{Codec, CodecConfig, FormatParams},
    media::{Frequency, MediaKind},
};

use super::rtp_projection;
use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    engine::media_transport::TransportAdapterError,
};

const VP8_PAYLOAD_TYPE: u8 = 96;
const H264_PROFILES: &[(u8, PacketizationMode, Profile)] = &[
    (127, PacketizationMode::NonInterleaved, Profile::Baseline),
    (125, PacketizationMode::SingleNalUnit, Profile::Baseline),
    (
        108,
        PacketizationMode::NonInterleaved,
        Profile::ConstrainedBaseline,
    ),
    (
        124,
        PacketizationMode::SingleNalUnit,
        Profile::ConstrainedBaseline,
    ),
    (123, PacketizationMode::NonInterleaved, Profile::Main),
    (35, PacketizationMode::SingleNalUnit, Profile::Main),
    (114, PacketizationMode::NonInterleaved, Profile::High),
];

#[derive(Debug)]
pub(in crate::engine::media_transport) struct RtpProfile {
    config: RtcConfig,
    router_capabilities: MediaCapabilities,
    audio_names: Vec<String>,
    video_names: Vec<String>,
    simulcast_codec: Option<Codec>,
}

impl RtpProfile {
    pub(in crate::engine::media_transport) fn compile(
        flags: MediaCodecFlags,
        preferences: CodecPreferences,
    ) -> Result<Self, TransportAdapterError> {
        let mut config = Rtc::builder().clear_codecs().set_rtp_mode(true);
        let codecs = config.codec_config();
        for codec in preferences.audio_order() {
            if !codec.enabled_by(flags) {
                continue;
            }
            match codec {
                AudioCodecPreference::Opus => codecs.enable_opus(true),
                AudioCodecPreference::Pcmu => codecs.enable_pcmu(true),
                AudioCodecPreference::Pcma => codecs.enable_pcma(true),
            }
        }
        for codec in preferences.video_order() {
            if !codec.enabled_by(flags) {
                continue;
            }
            match codec {
                VideoCodecPreference::Vp8 => codecs.add_config(
                    VP8_PAYLOAD_TYPE.into(),
                    None,
                    Codec::Vp8,
                    Frequency::NINETY_KHZ,
                    None,
                    FormatParams::default(),
                ),
                VideoCodecPreference::H264 => add_h264_codecs(codecs),
                VideoCodecPreference::H265 => codecs.enable_h265(true),
                VideoCodecPreference::Vp9 => codecs.enable_vp9(true),
                VideoCodecPreference::Av1 => codecs.enable_av1(true),
            }
        }
        let simulcast_codec = preferences
            .video_order()
            .into_iter()
            .find(|codec| codec.enabled_by(flags))
            .and_then(|codec| match codec {
                VideoCodecPreference::Vp8 => Some(Codec::Vp8),
                VideoCodecPreference::H264 => Some(Codec::H264),
                VideoCodecPreference::H265
                | VideoCodecPreference::Vp9
                | VideoCodecPreference::Av1 => None,
            });

        let mut router_codecs = Vec::new();
        let mut audio_names = Vec::new();
        let mut video_names = Vec::new();
        for payload in config.codec_config().params() {
            let kind = rtp_projection::media_kind(payload);
            router_codecs.push(rtp_projection::media_capability(kind, payload)?);
            router_codecs.extend(rtp_projection::rtx_capability(kind, payload)?);
            let names = if kind == RouterMediaKind::Audio {
                &mut audio_names
            } else {
                &mut video_names
            };
            let name = payload.spec().codec.to_string();
            if !names.contains(&name) {
                names.push(name);
            }
        }
        let header_extensions = config
            .extension_map()
            .iter()
            .map(rtp_projection::header_extension)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            config,
            router_capabilities: MediaCapabilities::new(router_codecs, header_extensions),
            audio_names,
            video_names,
            simulcast_codec,
        })
    }

    pub(super) fn session_config(&self) -> RtcConfig {
        self.config.clone()
    }

    pub(in crate::engine::media_transport) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }

    pub(super) fn codec_names(&self, kind: MediaKind) -> &[String] {
        if kind.is_video() {
            &self.video_names
        } else {
            &self.audio_names
        }
    }

    pub(super) fn simulcast_codec(&self) -> Option<Codec> {
        self.simulcast_codec
    }
}

fn add_h264_codecs(codecs: &mut CodecConfig) {
    for &(payload_type, packetization_mode, profile) in H264_PROFILES {
        codecs.add_h264(
            payload_type.into(),
            None,
            packetization_mode == PacketizationMode::NonInterleaved,
            ProfileLevelId::new(profile, LevelIdc::Level3_1).packed_value(),
        );
    }
}

#[cfg(test)]
#[path = "TESTS/profile.rs"]
mod tests;
