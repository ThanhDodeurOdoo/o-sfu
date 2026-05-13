//! Production RTC user and socket bootstrap primitives.
//!
//! This module contain the real cold-path setup that production worker logic
//! reuses: bind the shard-local UDP socket, construct `str0m::Rtc`, seed local
//! candidates, and create the worker-owned user state.

use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::Instant,
};

use o_sfu_rfc::webrtc;
use str0m::{
    Candidate, Rtc,
    bwe::Bitrate as Str0mBitrate,
    format::{Codec, CodecConfig, FormatParams},
    media::Frequency,
};
use tokio::net::UdpSocket;
use tracing::info;

use super::state::{RtcSessionState, SessionSdpNegotiationState, SharedRtcSocket};
use crate::{
    Bitrate, MediaCodecFlags, RtcPortRange,
    runtime::media_transport::{TransportAdapterError, TransportSessionKey},
};

const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;

pub(super) fn bind_shared_rtc_socket(
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
) -> Result<SharedRtcSocket, TransportAdapterError> {
    let bind_ip = bind_ip_for_public_ip(public_ip);
    for port in rtc_port_range.ports() {
        let bind_addr = SocketAddr::new(bind_ip, port);
        match StdUdpSocket::bind(bind_addr) {
            Ok(socket) => {
                if socket.set_nonblocking(true).is_err() {
                    continue;
                }
                let Ok(socket) = UdpSocket::from_std(socket) else {
                    continue;
                };
                let candidate_addr = SocketAddr::new(public_ip, port);
                info!(
                    %bind_addr,
                    %candidate_addr,
                    "booted shared rtc UDP socket"
                );
                return Ok(SharedRtcSocket {
                    socket: Arc::new(socket),
                    candidate_addr,
                });
            }
            Err(_error) => {}
        }
    }
    Err(TransportAdapterError::TransportUnavailable)
}

fn bind_ip_for_public_ip(public_ip: IpAddr) -> IpAddr {
    match public_ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

pub(super) fn ensure_session_rtc_state(
    users: &mut BTreeMap<TransportSessionKey, RtcSessionState>,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out: Bitrate,
    codec_flags: MediaCodecFlags,
) -> Result<bool, TransportAdapterError> {
    if users.contains_key(session_key) {
        return Ok(false);
    }
    let started_at = Instant::now();
    let mut rtc = rtc_builder(codec_flags)
        .enable_bwe(Some(Str0mBitrate::bps(max_bitrate_out.as_bps())))
        .set_ice_lite(true)
        .build(started_at);
    rtc.bwe()
        .set_desired_bitrate(Str0mBitrate::bps(max_bitrate_out.as_bps()));
    let candidate = Candidate::host(candidate_addr, webrtc::IceTransport::Udp.as_str())
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    if rtc.add_local_candidate(candidate).is_none() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    let local_ice_ufrag = rtc.direct_api().local_ice_credentials().ufrag;
    users.insert(
        session_key.clone(),
        RtcSessionState {
            rtc,
            started_at,
            local_ice_ufrag,
            #[cfg(test)]
            max_bitrate_in: None,
            #[cfg(test)]
            max_bitrate_out: Some(max_bitrate_out),
            dtls_started: false,
            sdp_negotiation: SessionSdpNegotiationState::default(),
            consumer_streams: HashMap::new(),
        },
    );
    Ok(true)
}

fn rtc_builder(codec_flags: MediaCodecFlags) -> str0m::RtcConfig {
    let mut config = Rtc::builder()
        .clear_codecs()
        .enable_opus(codec_flags.opus_enabled())
        .enable_pcmu(codec_flags.pcmu_enabled())
        .enable_pcma(codec_flags.pcma_enabled())
        .set_rtp_mode(true);
    if codec_flags.vp8_enabled() {
        config.codec_config().add_config(
            VIDEO_PAYLOAD_TYPE_VP8.into(),
            None,
            Codec::Vp8,
            Frequency::NINETY_KHZ,
            None,
            FormatParams::default(),
        );
    }
    if codec_flags.h264_enabled() {
        add_h264_codecs_without_rtx(config.codec_config());
    }
    config
        .enable_h265(codec_flags.h265_enabled())
        .enable_vp9(codec_flags.vp9_enabled())
        .enable_av1(codec_flags.av1_enabled())
}

fn add_h264_codecs_without_rtx(codec_config: &mut CodecConfig) {
    const H264_CODECS: &[(u8, bool, u32)] = &[
        (127, true, 0x0042_001f),
        (125, false, 0x0042_001f),
        (108, true, 0x0042_e01f),
        (124, false, 0x0042_e01f),
        (123, true, 0x004d_001f),
        (35, false, 0x004d_001f),
        (114, true, 0x0064_001f),
    ];
    for (payload_type, packetization_mode, profile_level_id) in H264_CODECS {
        codec_config.add_h264(
            (*payload_type).into(),
            None,
            *packetization_mode,
            *profile_level_id,
        );
    }
}
