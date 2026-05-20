//! Codec-profile facade for RTC-edge simulcast negotiation.
//!
//! The room source graph and video policy stay codec-neutral. This module contains
//! the narrow SDP, upload-layer and initial route-gate decisions that differ
//! between simulcast paths.

mod common;
mod consumer;
mod h264;
mod vp8;

pub(in crate::runtime::rtc_engine) use common::NegotiatedRid;
use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{MediaKind, Mid, Simulcast as Str0mSimulcast};

use crate::{
    MediaCodecFlags, VideoBitrateLimits,
    runtime::{media_transport::SessionUploadEncoding, rtc_engine::route_control::PacketLayerGate},
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum SimulcastCodecProfile {
    Vp8(vp8::Vp8SimulcastProfile),
    H264(h264::H264SimulcastProfile),
}

impl SimulcastCodecProfile {
    fn bootstrap(
        media_kind: MediaKind,
        codec_flags: MediaCodecFlags,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        if !media_kind.is_video() {
            return None;
        }
        if codec_flags.vp8_enabled() {
            return Some(Self::Vp8(vp8::Vp8SimulcastProfile::new(
                video_bitrate_limits,
            )));
        }
        codec_flags
            .h264_enabled()
            .then(|| Self::H264(h264::H264SimulcastProfile::new(video_bitrate_limits)))
    }

    fn publish(
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        if !media_kind.is_video() {
            return None;
        }
        if vp8::Vp8SimulcastProfile::layers_from_parameters(rtp_parameters).is_some() {
            return Some(Self::Vp8(vp8::Vp8SimulcastProfile::new(
                video_bitrate_limits,
            )));
        }
        h264::H264SimulcastProfile::from_parameters(rtp_parameters, video_bitrate_limits)
            .map(Self::H264)
    }

    fn recv_simulcast(
        &self,
        rtp_parameters: Option<&RouterRtpParameters>,
    ) -> Option<Str0mSimulcast> {
        match self {
            Self::Vp8(profile) => {
                let layers = rtp_parameters.map_or_else(
                    || profile.default_layers().to_vec(),
                    |parameters| {
                        vp8::Vp8SimulcastProfile::layers_from_parameters(parameters)
                            .unwrap_or_default()
                    },
                );
                (!layers.is_empty()).then(|| common::recv_simulcast_from_specs(&layers))
            }
            Self::H264(profile) => {
                let layers = rtp_parameters.map_or_else(
                    || profile.default_layers().to_vec(),
                    |parameters| {
                        h264::H264SimulcastProfile::layers_from_parameters(parameters)
                            .unwrap_or_default()
                    },
                );
                (!layers.is_empty()).then(|| common::recv_simulcast_from_specs(&layers))
            }
        }
    }

    fn upload_encodings(
        &self,
        rtp_parameters: Option<&RouterRtpParameters>,
    ) -> Vec<SessionUploadEncoding> {
        match self {
            Self::Vp8(profile) => {
                let layers = rtp_parameters.map_or_else(
                    || profile.default_layers().to_vec(),
                    |parameters| {
                        vp8::Vp8SimulcastProfile::layers_from_parameters(parameters)
                            .unwrap_or_default()
                    },
                );
                vp8::Vp8SimulcastProfile::upload_encodings_from_specs(&layers)
            }
            Self::H264(profile) => {
                let _ = h264::H264SimulcastProfile::rtx_allowed();
                let layers = rtp_parameters.map_or_else(
                    || profile.default_layers().to_vec(),
                    |parameters| {
                        h264::H264SimulcastProfile::layers_from_parameters(parameters)
                            .unwrap_or_default()
                    },
                );
                h264::H264SimulcastProfile::upload_encodings_from_specs(&layers)
            }
        }
    }
}

pub(super) fn bootstrap_recv_simulcast(
    media_kind: MediaKind,
    codec_flags: MediaCodecFlags,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Str0mSimulcast> {
    SimulcastCodecProfile::bootstrap(media_kind, codec_flags, video_bitrate_limits)
        .and_then(|profile| profile.recv_simulcast(None))
}

pub(super) fn bootstrap_upload_encodings(
    media_kind: MediaKind,
    codec_flags: MediaCodecFlags,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    SimulcastCodecProfile::bootstrap(media_kind, codec_flags, video_bitrate_limits)
        .map_or_else(Vec::new, |profile| profile.upload_encodings(None))
}

pub(super) fn publish_recv_simulcast(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
) -> Option<Str0mSimulcast> {
    SimulcastCodecProfile::publish(media_kind, rtp_parameters, VideoBitrateLimits::default())
        .and_then(|profile| profile.recv_simulcast(Some(rtp_parameters)))
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
    SimulcastCodecProfile::publish(media_kind, rtp_parameters, VideoBitrateLimits::default())
        .map_or_else(Vec::new, |profile| {
            profile.upload_encodings(Some(rtp_parameters))
        })
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

pub(super) fn send_rids_for_mid(answer_sdp: &str, mid: Mid) -> Vec<NegotiatedRid> {
    common::send_rids_for_mid(answer_sdp, mid)
}

pub(super) fn initial_consumer_packet_gate(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> PacketLayerGate {
    consumer::initial_packet_gate(consumer_rtp_parameters)
}

fn publish_uses_default_profile(rtp_parameters: &RouterRtpParameters) -> bool {
    rtp_parameters.formats().next().is_none() && rtp_parameters.encodings().next().is_none()
}

#[cfg(test)]
mod tests {
    use o_sfu_rfc::{rtp as rfc_rtp, webrtc};
    use o_sfu_router::{CodecSetting, MediaFormat, MediaKind as RouterMediaKind, StreamBinding};
    use str0m::media::{MediaKind, Rid as Str0mRid};

    use super::*;
    use crate::Bitrate;

    const ANSWER_HIGH_MAX_BITRATE: Bitrate = Bitrate::from_kbps(900);

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
                    rid: Str0mRid::from(common::DEFAULT_LOW_RID),
                    max_bitrate: Some(common::DEFAULT_LOW_MAX_BITRATE),
                },
                NegotiatedRid {
                    rid: Str0mRid::from(common::DEFAULT_HIGH_RID),
                    max_bitrate: Some(ANSWER_HIGH_MAX_BITRATE),
                },
            ]
        );
    }

    #[test]
    fn h264_and_vp8_profiles_are_promoted_simulcast_publication_paths() {
        let h264 = h264_parameters(1, "42e01f");

        assert!(publish_recv_simulcast(MediaKind::Video, &h264).is_some());
        assert_eq!(
            publish_upload_encodings(MediaKind::Video, &h264),
            vec![
                SessionUploadEncoding {
                    rid: common::DEFAULT_LOW_RID.to_owned(),
                    max_bitrate: None,
                    resolution_scale: None,
                    max_framerate: None,
                },
                SessionUploadEncoding {
                    rid: common::DEFAULT_HIGH_RID.to_owned(),
                    max_bitrate: None,
                    resolution_scale: None,
                    max_framerate: None,
                },
            ]
        );

        let vp8 = vp8_parameters();

        assert!(publish_recv_simulcast(MediaKind::Video, &vp8).is_some());
        assert_eq!(
            publish_upload_encodings(MediaKind::Video, &vp8),
            vec![
                SessionUploadEncoding {
                    rid: common::DEFAULT_LOW_RID.to_owned(),
                    max_bitrate: None,
                    resolution_scale: Some(4),
                    max_framerate: None,
                },
                SessionUploadEncoding {
                    rid: common::DEFAULT_HIGH_RID.to_owned(),
                    max_bitrate: None,
                    resolution_scale: Some(1),
                    max_framerate: None,
                },
            ]
        );
    }

    #[test]
    fn h264_profile_accepts_only_the_promoted_chromium_matrix() {
        let parameters = h264_parameters(1, "42e01f");
        let profile = SimulcastCodecProfile::publish(
            MediaKind::Video,
            &parameters,
            VideoBitrateLimits::default(),
        );
        assert!(
            matches!(profile, Some(SimulcastCodecProfile::H264(_))),
            "H264 parameters should select the H264 interop profile"
        );
        let Some(SimulcastCodecProfile::H264(profile)) = profile else {
            return;
        };

        assert_eq!(
            profile.default_layers(),
            common::default_layer_specs(VideoBitrateLimits::default())
        );
        assert!(!h264::H264SimulcastProfile::rtx_allowed());

        for parameters in [
            h264_parameters(0, "42e01f"),
            h264_parameters(1, "42001f"),
            h264_parameters(1, "4d001f"),
        ] {
            assert!(
                publish_upload_encodings(MediaKind::Video, &parameters).is_empty(),
                "unsupported H264 variants must remain single-encoding"
            );
        }
    }

    #[test]
    fn h264_only_bootstrap_gets_default_simulcast_metadata() {
        let codec_flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
        let encodings = bootstrap_upload_encodings(
            MediaKind::Video,
            codec_flags,
            VideoBitrateLimits::default(),
        );
        assert!(
            bootstrap_recv_simulcast(MediaKind::Video, codec_flags, VideoBitrateLimits::default())
                .is_some()
        );
        assert_eq!(
            encodings
                .iter()
                .map(|encoding| (
                    encoding.rid.as_str(),
                    encoding.max_bitrate,
                    encoding.resolution_scale,
                    encoding.max_framerate,
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    common::DEFAULT_LOW_RID,
                    Some(common::DEFAULT_LOW_MAX_BITRATE),
                    None,
                    None,
                ),
                (
                    common::DEFAULT_HIGH_RID,
                    Some(VideoBitrateLimits::default().max_video_bitrate()),
                    None,
                    None,
                ),
            ]
        );
    }

    fn vp8_parameters() -> RouterRtpParameters {
        video_parameters(MediaFormat::new(
            RouterMediaKind::Video,
            rfc_rtp::CodecName::Vp8,
            96,
            90_000,
        ))
    }

    fn h264_parameters(packetization_mode: u8, profile_level_id: &str) -> RouterRtpParameters {
        video_parameters(
            MediaFormat::new(
                RouterMediaKind::Video,
                rfc_rtp::CodecName::H264,
                102,
                90_000,
            )
            .with_setting(CodecSetting::H264PacketizationMode(packetization_mode))
            .with_setting(CodecSetting::H264ProfileLevelId(
                profile_level_id.to_owned(),
            )),
        )
    }

    fn video_parameters(format: MediaFormat) -> RouterRtpParameters {
        RouterRtpParameters::new(
            vec![format],
            Vec::new(),
            vec![
                StreamBinding::new().with_rid(common::DEFAULT_LOW_RID),
                StreamBinding::new().with_rid(common::DEFAULT_HIGH_RID),
            ],
        )
    }
}
