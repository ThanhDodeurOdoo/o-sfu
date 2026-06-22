//! Codec-profile facade for RTC-edge simulcast negotiation.
//!
//! The room source graph and video policy stay codec-neutral. This module contains
//! the narrow SDP, upload-layer and initial route-gate decisions that differ
//! between simulcast paths.

mod common;
mod h264;
mod vp8;

pub use common::{NegotiatedRid, SimulcastAnswerError};
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::{MediaKind, Mid, Simulcast as Str0mSimulcast};

use crate::{
    MediaCodecFlags, VideoBitrateLimits,
    engine::media_transport::{SessionUploadEncoding, rtc::route_control::PacketLayerGate},
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

pub(super) fn send_rids_for_mid(
    answer_sdp: &str,
    mid: Mid,
    offered_encodings: &[SessionUploadEncoding],
) -> Result<Vec<NegotiatedRid>, SimulcastAnswerError> {
    common::send_rids_for_mid(answer_sdp, mid, offered_encodings)
}

pub(super) fn initial_consumer_packet_gate(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> PacketLayerGate {
    common::initial_packet_gate(consumer_rtp_parameters)
}

fn publish_uses_default_profile(rtp_parameters: &RouterRtpParameters) -> bool {
    rtp_parameters.formats().next().is_none() && rtp_parameters.bindings().next().is_none()
}

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
