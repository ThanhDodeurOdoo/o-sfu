//! RTC codec policy, negotiation and packet behavior.
//!
//! [`profile`] and [`capabilities`] compile configured codecs into `str0m` and
//! router RTP forms. VP8 and promoted H.264 profiles derive RID simulcast
//! signaling and upload encodings through [`rid`]. [`rid`] also validates
//! answer-side send RIDs and selects the initial consumer packet gate.
//!
//! [`packet`] keeps codec-specific packet inspection and receiver identity
//! projection below the room source graph and video policy.

mod capabilities;
mod h264;
mod packet;
mod profile;
mod rid;
mod vp8;

#[cfg(any(test, feature = "fuzzing"))]
pub use capabilities::client_rtp_capabilities_from_answer;
pub(super) use capabilities::{
    client_rtp_capabilities_from_sdp_answer, header_extension, media_format, router_payload_type,
};
use o_sfu_router::rtp::{MediaCodec, MediaStream};
pub(super) use packet::{
    Packet, PacketIdentity, PacketInspector, ProjectedPacket, Projection, requires_decoder_refresh,
};
pub(in crate::engine::media_transport) use profile::RtpProfile;
pub(super) use rid::{
    NegotiatedRid, initial_packet_gate as initial_consumer_packet_gate, send_rids_for_mid,
};
use str0m::{
    format::Codec,
    media::{MediaKind, Simulcast},
};

use crate::{VideoBitrateLimits, engine::media_transport::SessionUploadEncoding};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulcastProfile {
    Vp8(vp8::SimulcastProfile),
    H264(h264::SimulcastProfile),
}

impl SimulcastProfile {
    fn bootstrap(
        media_kind: MediaKind,
        rtp_profile: &RtpProfile,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        if !media_kind.is_video() {
            return None;
        }
        match rtp_profile.simulcast_codec()? {
            Codec::Vp8 => Some(Self::Vp8(vp8::SimulcastProfile::new(video_bitrate_limits))),
            Codec::H264 => Some(Self::H264(h264::SimulcastProfile::new(
                video_bitrate_limits,
            ))),
            _ => None,
        }
    }

    fn publish(
        media_kind: MediaKind,
        parameters: &MediaStream,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        if !media_kind.is_video() {
            return None;
        }
        match capabilities::primary_codec(parameters)? {
            MediaCodec::Vp8 => Some(Self::Vp8(vp8::SimulcastProfile::new(video_bitrate_limits))),
            MediaCodec::H264 => Some(Self::H264(h264::SimulcastProfile::new(
                video_bitrate_limits,
            ))),
            _ => None,
        }
    }

    fn recv_simulcast(self, parameters: Option<&MediaStream>) -> Option<Simulcast> {
        match self {
            Self::Vp8(profile) => profile.recv_simulcast(parameters),
            Self::H264(profile) => profile.recv_simulcast(parameters),
        }
    }

    fn upload_encodings(self, parameters: Option<&MediaStream>) -> Vec<SessionUploadEncoding> {
        match self {
            Self::Vp8(profile) => profile.upload_encodings(parameters),
            Self::H264(profile) => profile.upload_encodings(parameters),
        }
    }
}

pub(super) fn bootstrap_recv_simulcast(
    media_kind: MediaKind,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Simulcast> {
    SimulcastProfile::bootstrap(media_kind, rtp_profile, video_bitrate_limits)
        .and_then(|profile| profile.recv_simulcast(None))
}

pub(super) fn bootstrap_upload_encodings(
    media_kind: MediaKind,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    SimulcastProfile::bootstrap(media_kind, rtp_profile, video_bitrate_limits)
        .map_or_else(Vec::new, |profile| profile.upload_encodings(None))
}

pub(super) fn publish_recv_simulcast(
    media_kind: MediaKind,
    parameters: &MediaStream,
) -> Option<Simulcast> {
    SimulcastProfile::publish(media_kind, parameters, VideoBitrateLimits::default())
        .and_then(|profile| profile.recv_simulcast(Some(parameters)))
}

pub(super) fn publish_recv_simulcast_or_default(
    media_kind: MediaKind,
    parameters: &MediaStream,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Simulcast> {
    publish_recv_simulcast(media_kind, parameters).or_else(|| {
        publish_uses_default_profile(parameters)
            .then(|| bootstrap_recv_simulcast(media_kind, rtp_profile, video_bitrate_limits))
            .flatten()
    })
}

pub(super) fn publish_upload_encodings(
    media_kind: MediaKind,
    parameters: &MediaStream,
) -> Vec<SessionUploadEncoding> {
    SimulcastProfile::publish(media_kind, parameters, VideoBitrateLimits::default())
        .map_or_else(Vec::new, |profile| {
            profile.upload_encodings(Some(parameters))
        })
}

pub(super) fn publish_upload_encodings_or_default(
    media_kind: MediaKind,
    parameters: &MediaStream,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    let encodings = publish_upload_encodings(media_kind, parameters);
    if !encodings.is_empty() || !publish_uses_default_profile(parameters) {
        return encodings;
    }
    bootstrap_upload_encodings(media_kind, rtp_profile, video_bitrate_limits)
}

fn publish_uses_default_profile(parameters: &MediaStream) -> bool {
    parameters.formats().next().is_none() && parameters.bindings().next().is_none()
}

#[cfg(test)]
#[path = "TESTS/mod.rs"]
mod tests;
