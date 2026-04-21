//! Production RTC session and socket bootstrap primitives.
//!
//! This module owns the real cold-path setup that production worker logic
//! reuses: bind the shard-local UDP socket, construct `str0m::Rtc`, seed local
//! candidates, and create the worker-owned session state.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::Instant,
};

use o_sfu_rfc::webrtc;
use str0m::bwe::Bitrate;
use str0m::{Candidate, Rtc};
use tokio::net::UdpSocket;
use tracing::info;

use super::state::{RtcSessionState, SessionSdpNegotiationState, SharedRtcSocket};
use crate::config::MediaCodecFlags;
use crate::config::RtcPortRange;
use crate::runtime::transport_adapter::{TransportAdapterError, TransportSessionKey};
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
    sessions: &mut BTreeMap<TransportSessionKey, RtcSessionState>,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out_bps: u64,
    codec_flags: MediaCodecFlags,
) -> Result<bool, TransportAdapterError> {
    if sessions.contains_key(session_key) {
        return Ok(false);
    }
    let started_at = Instant::now();
    let mut rtc = rtc_builder(codec_flags)
        .enable_bwe(Some(Bitrate::bps(max_bitrate_out_bps)))
        .set_ice_lite(true)
        .build(started_at);
    rtc.bwe()
        .set_desired_bitrate(Bitrate::bps(max_bitrate_out_bps));
    let candidate = Candidate::host(candidate_addr, webrtc::IceTransport::Udp.as_str())
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    if rtc.add_local_candidate(candidate).is_none() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    let local_ice_ufrag = rtc.direct_api().local_ice_credentials().ufrag;
    sessions.insert(
        session_key.clone(),
        RtcSessionState {
            rtc,
            started_at,
            local_ice_ufrag,
            #[cfg(test)]
            max_bitrate_in_bps: None,
            #[cfg(test)]
            max_bitrate_out_bps: Some(max_bitrate_out_bps),
            dtls_started: false,
            sdp_negotiation: SessionSdpNegotiationState::default(),
        },
    );
    Ok(true)
}

fn rtc_builder(codec_flags: MediaCodecFlags) -> str0m::RtcConfig {
    Rtc::builder()
        .clear_codecs()
        .enable_opus(codec_flags.opus_enabled())
        .enable_pcmu(codec_flags.pcmu_enabled())
        .enable_pcma(codec_flags.pcma_enabled())
        .enable_vp8(codec_flags.vp8_enabled())
        .enable_h264(codec_flags.h264_enabled())
        .enable_h265(codec_flags.h265_enabled())
        .enable_vp9(codec_flags.vp9_enabled())
        .enable_av1(codec_flags.av1_enabled())
        .set_rtp_mode(true)
}
