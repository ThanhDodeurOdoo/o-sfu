//! Production H.264 simulcast profile for the first promoted browser matrix.
//!
//! The promoted matrix is intentionally narrow: Chromium-compatible constrained
//! baseline H.264 using packetization-mode 1. Broader profile, packetization,
//! browser, and repair-mode support must pass through this boundary before the
//! RTC edge exposes RID simulcast metadata for it.

use o_sfu_rfc::rtp as rfc_rtp;
use o_sfu_router::{CodecSetting, MediaFormat, MediaStream as RouterRtpParameters};

use super::common::{self, SimulcastLayerSpec};
use crate::{VideoBitrateLimits, engine::media_transport::SessionUploadEncoding};

const CHROMIUM_PACKETIZATION_MODE: u8 = 1;
const CHROMIUM_CONSTRAINED_BASELINE_PROFILE_LEVEL_ID: &str = "42e01f";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct H264SimulcastProfile {
    video_bitrate_limits: VideoBitrateLimits,
}

impl H264SimulcastProfile {
    pub(super) const fn new(video_bitrate_limits: VideoBitrateLimits) -> Self {
        Self {
            video_bitrate_limits,
        }
    }

    pub(super) fn from_parameters(
        rtp_parameters: &RouterRtpParameters,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        rtp_parameters
            .formats()
            .any(is_promoted_format)
            .then(|| Self::new(video_bitrate_limits))
    }

    pub(super) fn default_layers(self) -> [SimulcastLayerSpec<'static>; 2] {
        common::default_layer_specs(self.video_bitrate_limits)
    }

    pub(super) fn layers_from_parameters(
        rtp_parameters: &RouterRtpParameters,
    ) -> Option<Vec<SimulcastLayerSpec<'_>>> {
        if !rtp_parameters.formats().any(is_promoted_format) {
            return None;
        }
        common::layers_from_rid_bindings(rtp_parameters)
    }

    pub(super) fn upload_encodings_from_specs(
        layers: &[SimulcastLayerSpec<'_>],
    ) -> Vec<SessionUploadEncoding> {
        layers
            .iter()
            .map(|layer| SessionUploadEncoding {
                rid: layer.rid.to_owned(),
                max_bitrate: layer.max_bitrate,
                resolution_scale: None,
                max_framerate: None,
            })
            .collect()
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
