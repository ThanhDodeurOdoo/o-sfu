//! Shared RID simulcast helpers for RTC-edge codec profiles.

use o_sfu_rfc::webrtc;
use str0m::media::{
    Mid, Rid as Str0mRid, Simulcast as Str0mSimulcast, SimulcastLayer as Str0mSimulcastLayer,
};

use crate::{
    VideoBitrateLimits,
    runtime::{source_model::UploadLayerPolicyRole, transport_adapter::SessionUploadEncoding},
};

pub(super) const DEFAULT_LOW_RID: &str = "lo";
pub(super) const DEFAULT_HIGH_RID: &str = "hi";
pub(super) const DEFAULT_LOW_MAX_BITRATE_BPS: u64 = 150_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::rtc_adapter) struct NegotiatedRid {
    pub(in crate::runtime::rtc_adapter) rid: Str0mRid,
    pub(in crate::runtime::rtc_adapter) max_bitrate: Option<u64>,
}

#[derive(Clone, Copy)]
pub(super) struct SimulcastLayerSpec<'a> {
    pub(super) rid: &'a str,
    pub(super) max_bitrate: Option<u64>,
    pub(super) resolution_scale: u16,
    pub(super) max_framerate: Option<u16>,
    pub(super) policy_role: UploadLayerPolicyRole,
}

pub(super) fn default_layer_specs(
    video_bitrate_limits: VideoBitrateLimits,
) -> [SimulcastLayerSpec<'static>; 2] {
    let high_max_bitrate = video_bitrate_limits.max_video_bitrate_bps();
    [
        SimulcastLayerSpec {
            rid: DEFAULT_LOW_RID,
            max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE_BPS.min(high_max_bitrate)),
            resolution_scale: 2,
            max_framerate: None,
            policy_role: UploadLayerPolicyRole::Thumbnail,
        },
        SimulcastLayerSpec {
            rid: DEFAULT_HIGH_RID,
            max_bitrate: Some(high_max_bitrate),
            resolution_scale: 1,
            max_framerate: None,
            policy_role: UploadLayerPolicyRole::Featured,
        },
    ]
}

pub(super) fn recv_simulcast_from_specs(layers: &[SimulcastLayerSpec<'_>]) -> Str0mSimulcast {
    Str0mSimulcast {
        send: Vec::new(),
        recv: layers
            .iter()
            .map(|layer| Str0mSimulcastLayer {
                rid: Str0mRid::from(layer.rid),
                attributes: layer.max_bitrate.map(|max_bitrate| {
                    vec![(
                        webrtc::sdp::rid_restriction::MAX_BITRATE.to_owned(),
                        max_bitrate.to_string(),
                    )]
                }),
            })
            .collect(),
    }
}

pub(super) fn upload_encodings_from_specs(
    layers: &[SimulcastLayerSpec<'_>],
) -> Vec<SessionUploadEncoding> {
    layers
        .iter()
        .map(|layer| SessionUploadEncoding {
            rid: layer.rid.to_owned(),
            max_bitrate: layer.max_bitrate,
            resolution_scale: Some(layer.resolution_scale),
            max_framerate: layer.max_framerate,
            policy_role: Some(layer.policy_role),
        })
        .collect()
}

pub(super) fn send_rids_for_mid(answer_sdp: &str, mid: Mid) -> Vec<NegotiatedRid> {
    let Some(section) = media_section_for_mid(answer_sdp, mid) else {
        return Vec::new();
    };
    section
        .lines()
        .filter_map(parse_send_rid)
        .collect::<Vec<_>>()
}

fn media_section_for_mid(sdp: &str, mid: Mid) -> Option<&str> {
    let marker = format!(
        "{}{}:{mid}",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::MID
    );
    let marker_start = sdp.find(&marker)?;
    let media_prefix = format!("\n{}", webrtc::sdp::MEDIA_PREFIX);
    let section_start = sdp[..marker_start]
        .rfind(&media_prefix)
        .map_or(0, |index| index + 1);
    let section_end = sdp[marker_start..]
        .find(&media_prefix)
        .map_or(sdp.len(), |offset| marker_start + offset + 1);
    Some(&sdp[section_start..section_end])
}

fn parse_send_rid(line: &str) -> Option<NegotiatedRid> {
    let rid_prefix = format!(
        "{}{}:",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::RID
    );
    let rid_value = line.trim_end_matches('\r').strip_prefix(&rid_prefix)?;
    let mut parts = rid_value.splitn(3, ' ');
    let rid = parts.next()?;
    if !webrtc::sdp::rid::is_id(rid) {
        return None;
    }
    let direction = parts.next()?;
    if webrtc::RtpStreamDirection::parse(direction) != Some(webrtc::RtpStreamDirection::Send) {
        return None;
    }
    Some(NegotiatedRid {
        rid: Str0mRid::from(rid),
        max_bitrate: parts.next().and_then(parse_max_bitrate),
    })
}

fn parse_max_bitrate(restrictions: &str) -> Option<u64> {
    restrictions
        .split(';')
        .filter_map(|restriction| restriction.split_once('='))
        .find_map(|(key, value)| {
            (key.trim() == webrtc::sdp::rid_restriction::MAX_BITRATE)
                .then(|| value.trim().parse::<u64>().ok())
                .flatten()
        })
}
