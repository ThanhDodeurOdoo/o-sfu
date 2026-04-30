//! H.264 simulcast interop gate.
//!
//! H.264 can be negotiated as a codec today, but it is deliberately not a
//! production simulcast profile until the browser, packetization, profile, RTX,
//! and decoder-refresh matrix is proven together.

use o_sfu_rfc::rtp as rfc_rtp;
use o_sfu_router::{CodecSetting, MediaStream as RouterRtpParameters};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct H264SimulcastProfile {
    packetization_modes: Vec<u8>,
    profile_level_ids: Vec<String>,
}

impl H264SimulcastProfile {
    pub(super) fn from_parameters(rtp_parameters: &RouterRtpParameters) -> Option<Self> {
        let mut packetization_modes = Vec::new();
        let mut profile_level_ids = Vec::new();
        for format in rtp_parameters
            .formats()
            .filter(|format| format.codec() == &rfc_rtp::CodecName::H264)
        {
            for setting in format.settings() {
                match setting {
                    CodecSetting::H264PacketizationMode(mode) => {
                        if !packetization_modes.contains(mode) {
                            packetization_modes.push(*mode);
                        }
                    }
                    CodecSetting::H264ProfileLevelId(profile_level_id) => {
                        if !profile_level_ids.contains(profile_level_id) {
                            profile_level_ids.push(profile_level_id.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        (!packetization_modes.is_empty() || !profile_level_ids.is_empty()).then_some(Self {
            packetization_modes,
            profile_level_ids,
        })
    }

    pub(super) const fn is_promoted() -> bool {
        false
    }

    pub(super) const fn rtx_allowed() -> bool {
        false
    }

    pub(super) fn packetization_modes(&self) -> &[u8] {
        self.packetization_modes.as_slice()
    }

    pub(super) fn profile_level_ids(&self) -> &[String] {
        self.profile_level_ids.as_slice()
    }
}
