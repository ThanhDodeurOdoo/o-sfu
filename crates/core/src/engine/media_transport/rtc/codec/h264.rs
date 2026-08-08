//! H.264 policy for o-sfu's promoted simulcast matrix.
//!
//! Publication parameters promote RID simulcast only for packetization mode 1
//! with `profile-level-id=42e01f`. Encoded H.264 payloads remain opaque to
//! [`super::packet`].

use o_sfu_rfc::rtp::{self as rfc_rtp, h264::PacketizationMode};
use o_sfu_router::rtp::{CodecSetting, MediaFormat, MediaStream};
use str0m::media::Simulcast;

use super::rid::{self, LayerSpec};
use crate::{VideoBitrateLimits, engine::media_transport::SessionUploadEncoding};

const CHROMIUM_PACKETIZATION_MODE: PacketizationMode = PacketizationMode::NonInterleaved;
const CHROMIUM_CONSTRAINED_BASELINE_PROFILE_LEVEL_ID: &str = "42e01f";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SimulcastProfile {
    video_bitrate_limits: VideoBitrateLimits,
}

impl SimulcastProfile {
    pub(super) const fn new(video_bitrate_limits: VideoBitrateLimits) -> Self {
        Self {
            video_bitrate_limits,
        }
    }

    pub(super) fn recv_simulcast(self, parameters: Option<&MediaStream>) -> Option<Simulcast> {
        self.layers(parameters)
            .map(|layers| rid::recv_simulcast(&layers))
    }

    pub(super) fn upload_encodings(
        self,
        parameters: Option<&MediaStream>,
    ) -> Vec<SessionUploadEncoding> {
        self.layers(parameters).map_or_else(Vec::new, |layers| {
            layers
                .into_iter()
                .map(|layer| SessionUploadEncoding {
                    rid: layer.rid.to_owned(),
                    max_bitrate: layer.max_bitrate,
                    resolution_scale: None,
                    max_framerate: None,
                })
                .collect()
        })
    }

    fn layers(self, parameters: Option<&MediaStream>) -> Option<Vec<LayerSpec<'_>>> {
        parameters.map_or_else(
            || Some(rid::default_layers(self.video_bitrate_limits).into()),
            Self::layers_from_parameters,
        )
    }

    fn layers_from_parameters(parameters: &MediaStream) -> Option<Vec<LayerSpec<'_>>> {
        if !parameters.formats().any(is_promoted_format) {
            return None;
        }
        rid::layers_from_bindings(parameters)
    }
}

fn is_promoted_format(format: &MediaFormat) -> bool {
    if format.codec() != &rfc_rtp::CodecName::H264 {
        return false;
    }
    let mut packetization_mode = None;
    let mut profile_level_id = None;
    for setting in format.settings() {
        match setting {
            CodecSetting::H264PacketizationMode(mode) => packetization_mode = Some(*mode),
            CodecSetting::H264ProfileLevelId(value) => profile_level_id = Some(value.as_str()),
            _ => {}
        }
    }
    packetization_mode == Some(CHROMIUM_PACKETIZATION_MODE)
        && profile_level_id.is_some_and(|value| {
            value.eq_ignore_ascii_case(CHROMIUM_CONSTRAINED_BASELINE_PROFILE_LEVEL_ID)
        })
}

#[cfg(test)]
#[path = "TESTS/h264.rs"]
mod tests;
