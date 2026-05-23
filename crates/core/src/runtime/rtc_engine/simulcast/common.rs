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
    let declarations = section
        .lines()
        .filter_map(parse_send_rid)
        .collect::<Vec<_>>();
    accepted_send_simulcast_rids(section)
        .into_iter()
        .filter_map(|accepted_rid| {
            declarations
                .iter()
                .find(|declaration| declaration.rid == accepted_rid)
                .map(|declaration| NegotiatedRid {
                    rid: Str0mRid::from(declaration.rid),
                    max_bitrate: declaration.max_bitrate,
                })
        })
        .collect()
}

fn media_section_for_mid(sdp: &str, mid: Mid) -> Option<&str> {
    let marker = format!(
        "{}{}:{mid}",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::MID
    );
    let media_prefix = format!("\n{}", webrtc::sdp::MEDIA_PREFIX);
    let mut section_search_start = 0;
    while let Some(section_start) =
        find_next_media_section_start(sdp, section_search_start, &media_prefix)
    {
        let section_body_start = section_start + webrtc::sdp::MEDIA_PREFIX.len();
        let section_end = find_next_media_section_start(sdp, section_body_start, &media_prefix)
            .unwrap_or(sdp.len());
        let section = &sdp[section_start..section_end];
        if section
            .lines()
            .map(|line| line.trim_end_matches('\r'))
            .any(|line| line == marker)
        {
            return Some(section);
        }
        section_search_start = section_end;
    }
    None
}

fn find_next_media_section_start(sdp: &str, start: usize, media_prefix: &str) -> Option<usize> {
    let remaining = sdp.get(start..)?;
    if remaining.starts_with(webrtc::sdp::MEDIA_PREFIX) {
        return Some(start);
    }
    remaining
        .find(media_prefix)
        .map(|offset| start + offset + 1)
}

#[derive(Debug, Clone, Copy)]
struct SendRidDeclaration<'a> {
    rid: &'a str,
    max_bitrate: Option<Bitrate>,
}

fn parse_send_rid(line: &str) -> Option<SendRidDeclaration<'_>> {
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
    Some(SendRidDeclaration {
        rid,
        max_bitrate: parts.next().and_then(parse_max_bitrate),
    })
}

fn accepted_send_simulcast_rids(section: &str) -> Vec<&str> {
    section
        .lines()
        .find_map(parse_send_simulcast_line)
        .unwrap_or_default()
}

fn parse_send_simulcast_line(line: &str) -> Option<Vec<&str>> {
    let simulcast_prefix = format!(
        "{}{}:",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::SIMULCAST
    );
    let simulcast_value = line
        .trim_end_matches('\r')
        .strip_prefix(&simulcast_prefix)?;
    let mut parts = simulcast_value.split_whitespace();
    while let Some(direction) = parts.next() {
        let rids = parts.next()?;
        if direction == webrtc::sdp::simulcast::DIRECTION_SEND {
            return Some(parse_simulcast_rid_list(rids));
        }
    }
    None
}

fn parse_simulcast_rid_list(value: &str) -> Vec<&str> {
    value
        .split(webrtc::sdp::simulcast::STREAM_SEPARATOR)
        .filter_map(|stream| {
            let selected_alternative = stream
                .split(webrtc::sdp::simulcast::ALTERNATIVE_SEPARATOR)
                .next()?;
            let rid = webrtc::sdp::simulcast::strip_initial_pause_prefix(selected_alternative)
                .unwrap_or(selected_alternative);
            webrtc::sdp::rid::is_id(rid).then_some(rid)
        })
        .collect()
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
