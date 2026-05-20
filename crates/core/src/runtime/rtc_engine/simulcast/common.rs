//! Shared RID simulcast helpers for RTC-edge codec profiles.

use o_sfu_rfc::webrtc;
use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{
    Mid, Rid as Str0mRid, Simulcast as Str0mSimulcast, SimulcastLayer as Str0mSimulcastLayer,
};

use crate::{Bitrate, VideoBitrateLimits};

pub(super) const DEFAULT_LOW_RID: &str = "lo";
pub(super) const DEFAULT_HIGH_RID: &str = "hi";
pub(super) const DEFAULT_LOW_MAX_BITRATE: Bitrate = Bitrate::from_kbps(150);
const DEFAULT_LOW_RESOLUTION_SCALE: u16 = 4;
const DEFAULT_HIGH_RESOLUTION_SCALE: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::rtc_engine) struct NegotiatedRid {
    pub rid: Str0mRid,
    pub max_bitrate: Option<Bitrate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SimulcastLayerSpec<'a> {
    pub(super) rid: &'a str,
    pub(super) max_bitrate: Option<Bitrate>,
    pub(super) resolution_scale: u16,
    pub(super) max_framerate: Option<u16>,
}

pub(super) fn default_layer_specs(
    video_bitrate_limits: VideoBitrateLimits,
) -> [SimulcastLayerSpec<'static>; 2] {
    let high_max_bitrate = video_bitrate_limits.max_video_bitrate();
    [
        SimulcastLayerSpec {
            rid: DEFAULT_LOW_RID,
            max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE.min(high_max_bitrate)),
            resolution_scale: DEFAULT_LOW_RESOLUTION_SCALE,
            max_framerate: None,
        },
        SimulcastLayerSpec {
            rid: DEFAULT_HIGH_RID,
            max_bitrate: Some(high_max_bitrate),
            resolution_scale: DEFAULT_HIGH_RESOLUTION_SCALE,
            max_framerate: None,
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
                        max_bitrate.as_bps().to_string(),
                    )]
                }),
            })
            .collect(),
    }
}

pub(super) fn layers_from_rid_bindings(
    rtp_parameters: &RouterRtpParameters,
) -> Option<Vec<SimulcastLayerSpec<'_>>> {
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
            max_bitrate: encoding.max_bitrate().map(Bitrate::from_bps),
            resolution_scale: resolution_scale_for_index(layers.len()),
            max_framerate: None,
        });
    }
    (layers.len() >= 2).then_some(layers)
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

fn parse_max_bitrate(restrictions: &str) -> Option<Bitrate> {
    restrictions
        .split(';')
        .filter_map(|restriction| restriction.split_once('='))
        .find_map(|(key, value)| {
            (key.trim() == webrtc::sdp::rid_restriction::MAX_BITRATE)
                .then(|| value.trim().parse::<u64>().ok().map(Bitrate::from_bps))
                .flatten()
        })
}

fn resolution_scale_for_index(index: usize) -> u16 {
    if index == 0 {
        DEFAULT_LOW_RESOLUTION_SCALE
    } else {
        DEFAULT_HIGH_RESOLUTION_SCALE
    }
}
