//! SDP simulcast helpers owned by the RTC edge.
//!
//! The runtime source graph consumes router-native RTP parameters. This module
//! keeps SDP RID and simulcast details at the RTC boundary and only exposes the
//! normalized encoding facts needed by offer generation and answer projection.

use o_sfu_rfc::{rtp as rfc_rtp, webrtc};
use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{
    MediaKind, Mid, Rid as Str0mRid, Simulcast as Str0mSimulcast,
    SimulcastLayer as Str0mSimulcastLayer,
};

use crate::{
    MediaCodecFlags, VideoBitrateLimits,
    runtime::{source_model::UploadLayerPolicyRole, transport_adapter::SessionUploadEncoding},
};

const DEFAULT_LOW_RID: &str = "lo";
const DEFAULT_HIGH_RID: &str = "hi";
const DEFAULT_LOW_MAX_BITRATE_BPS: u64 = 150_000;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NegotiatedRid {
    pub(super) rid: Str0mRid,
    pub(super) max_bitrate: Option<u64>,
}

#[derive(Clone, Copy)]
struct SimulcastLayerSpec<'a> {
    rid: &'a str,
    max_bitrate: Option<u64>,
    resolution_scale: u16,
    max_framerate: Option<u16>,
    policy_role: UploadLayerPolicyRole,
}

pub(super) fn bootstrap_recv_simulcast(
    media_kind: MediaKind,
    codec_flags: MediaCodecFlags,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Str0mSimulcast> {
    if !bootstrap_simulcast_enabled(media_kind, codec_flags) {
        return None;
    }
    Some(recv_simulcast_from_specs(&default_layer_specs(
        video_bitrate_limits,
    )))
}

pub(super) fn bootstrap_upload_encodings(
    media_kind: MediaKind,
    codec_flags: MediaCodecFlags,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    if !bootstrap_simulcast_enabled(media_kind, codec_flags) {
        return Vec::new();
    }
    upload_encodings_from_specs(&default_layer_specs(video_bitrate_limits))
}

pub(super) fn publish_recv_simulcast(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Option<Str0mSimulcast> {
    if !publish_simulcast_enabled(media_kind, rtp_parameters) {
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
    (layers.len() >= 2).then(|| recv_simulcast_from_specs(&layers))
}

pub(super) fn publish_recv_simulcast_or_default(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    codec_flags: MediaCodecFlags,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Str0mSimulcast> {
    publish_recv_simulcast(media_kind, rtp_parameters).or_else(|| {
        publish_uses_default_profile(rtp_parameters)
            .then(|| bootstrap_recv_simulcast(media_kind, codec_flags, video_bitrate_limits))
            .flatten()
    })
}

pub(super) fn publish_upload_encodings(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Vec<SessionUploadEncoding> {
    if !publish_simulcast_enabled(media_kind, rtp_parameters) {
        return Vec::new();
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
    if layers.len() < 2 {
        return Vec::new();
    }
    upload_encodings_from_specs(&layers)
}

pub(super) fn publish_upload_encodings_or_default(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    codec_flags: MediaCodecFlags,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    let encodings = publish_upload_encodings(media_kind, rtp_parameters);
    if !encodings.is_empty() || !publish_uses_default_profile(rtp_parameters) {
        return encodings;
    }
    bootstrap_upload_encodings(media_kind, codec_flags, video_bitrate_limits)
}

fn default_layer_specs(
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

fn bootstrap_simulcast_enabled(media_kind: MediaKind, codec_flags: MediaCodecFlags) -> bool {
    media_kind.is_video() && codec_flags.vp8_enabled()
}

fn publish_simulcast_enabled(media_kind: MediaKind, rtp_parameters: &RouterRtpParameters) -> bool {
    media_kind.is_video()
        && rtp_parameters
            .formats()
            .any(|format| format.codec() == &rfc_rtp::CodecName::Vp8)
}

fn publish_uses_default_profile(rtp_parameters: &RouterRtpParameters) -> bool {
    rtp_parameters.formats().next().is_none() && rtp_parameters.encodings().next().is_none()
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

fn recv_simulcast_from_specs(layers: &[SimulcastLayerSpec<'_>]) -> Str0mSimulcast {
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

fn upload_encodings_from_specs(layers: &[SimulcastLayerSpec<'_>]) -> Vec<SessionUploadEncoding> {
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

#[cfg(test)]
mod tests {
    use o_sfu_router::{MediaFormat, MediaKind as RouterMediaKind, StreamBinding};

    use super::*;

    const ANSWER_HIGH_MAX_BITRATE_BPS: u64 = 900_000;

    #[test]
    fn answer_send_rid_projection_preserves_declared_bitrate() {
        let answer = format!(
            concat!(
                "v=0\r\n",
                "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
                "a=mid:video_0\r\n",
                "a={rid_attr}:lo {send} {max_br}=150000\r\n",
                "a={rid_attr}:hi {send} {max_br}=900000\r\n",
                "a={simulcast_attr}:{send} lo{separator}hi\r\n"
            ),
            rid_attr = webrtc::sdp::attribute::RID,
            simulcast_attr = webrtc::sdp::attribute::SIMULCAST,
            send = webrtc::sdp::rid::DIRECTION_SEND,
            max_br = webrtc::sdp::rid_restriction::MAX_BITRATE,
            separator = webrtc::sdp::simulcast::STREAM_SEPARATOR,
        );

        let rids = send_rids_for_mid(&answer, Mid::from("video_0"));

        assert_eq!(
            rids,
            vec![
                NegotiatedRid {
                    rid: Str0mRid::from(DEFAULT_LOW_RID),
                    max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE_BPS),
                },
                NegotiatedRid {
                    rid: Str0mRid::from(DEFAULT_HIGH_RID),
                    max_bitrate: Some(ANSWER_HIGH_MAX_BITRATE_BPS),
                },
            ]
        );
    }

    #[test]
    fn publish_simulcast_metadata_requires_video_vp8_rid_metadata() {
        let h264 = video_parameters(rfc_rtp::CodecName::H264);

        assert!(publish_recv_simulcast(MediaKind::Video, &h264).is_none());
        assert!(publish_upload_encodings(MediaKind::Video, &h264).is_empty());

        let vp8 = video_parameters(rfc_rtp::CodecName::Vp8);

        assert!(publish_recv_simulcast(MediaKind::Video, &vp8).is_some());
        assert_eq!(
            publish_upload_encodings(MediaKind::Video, &vp8),
            vec![
                SessionUploadEncoding {
                    rid: DEFAULT_LOW_RID.to_owned(),
                    max_bitrate: None,
                    resolution_scale: Some(2),
                    max_framerate: None,
                    policy_role: Some(UploadLayerPolicyRole::Thumbnail),
                },
                SessionUploadEncoding {
                    rid: DEFAULT_HIGH_RID.to_owned(),
                    max_bitrate: None,
                    resolution_scale: Some(1),
                    max_framerate: None,
                    policy_role: Some(UploadLayerPolicyRole::Featured),
                },
            ]
        );
    }

    fn video_parameters(codec: rfc_rtp::CodecName) -> RouterRtpParameters {
        RouterRtpParameters::new(
            vec![MediaFormat::new(RouterMediaKind::Video, codec, 96, 90_000)],
            Vec::new(),
            vec![
                StreamBinding::new().with_rid(DEFAULT_LOW_RID),
                StreamBinding::new().with_rid(DEFAULT_HIGH_RID),
            ],
        )
    }
}
