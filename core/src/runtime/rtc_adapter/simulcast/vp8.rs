//! Production VP8 RID simulcast profile.

use o_sfu_rfc::{rtp as rfc_rtp, webrtc};
use o_sfu_router::MediaStream as RouterRtpParameters;

use super::common::{self, SimulcastLayerSpec};
use crate::{VideoBitrateLimits, runtime::source_model::UploadLayerPolicyRole};

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
        let mut layers = Vec::new();
        for encoding in rtp_parameters.encodings() {
            let Some(rid) = encoding.rid() else {
                continue;
            };
            if !webrtc::sdp::rid::is_id(rid)
                || layers
                    .iter()
                    .any(|layer: &SimulcastLayerSpec<'_>| layer.rid == rid)
            {
                continue;
            }
            layers.push(SimulcastLayerSpec {
                rid,
                max_bitrate: encoding.max_bitrate(),
                resolution_scale: resolution_scale_for_index(layers.len()),
                max_framerate: None,
                policy_role: policy_role_for_index(layers.len()),
            });
        }
        (layers.len() >= 2).then_some(layers)
    }
}

fn resolution_scale_for_index(index: usize) -> u16 {
    if index == 0 { 2 } else { 1 }
}

fn policy_role_for_index(index: usize) -> UploadLayerPolicyRole {
    if index == 0 {
        UploadLayerPolicyRole::Thumbnail
    } else {
        UploadLayerPolicyRole::Featured
    }
}
