//! Production VP8 RID simulcast profile.

use o_sfu_rfc::rtp as rfc_rtp;
use o_sfu_router::MediaStream as RouterRtpParameters;

use super::common::{self, SimulcastLayerSpec};
use crate::VideoBitrateLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Vp8SimulcastProfile {
    video_bitrate_limits: VideoBitrateLimits,
}

impl Vp8SimulcastProfile {
    pub(super) const fn new(video_bitrate_limits: VideoBitrateLimits) -> Self {
        Self {
            video_bitrate_limits,
        }
    }

    pub(super) fn default_layers(self) -> [SimulcastLayerSpec<'static>; 2] {
        common::default_layer_specs(self.video_bitrate_limits)
    }

    pub(super) fn layers_from_parameters(
        rtp_parameters: &RouterRtpParameters,
    ) -> Option<Vec<SimulcastLayerSpec<'_>>> {
        if !rtp_parameters
            .formats()
            .any(|format| format.codec() == &rfc_rtp::CodecName::Vp8)
        {
            return None;
        }
        common::layers_from_rid_bindings(rtp_parameters)
    }
}
