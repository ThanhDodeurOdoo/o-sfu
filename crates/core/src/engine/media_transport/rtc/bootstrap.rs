//! cold-path rtc socket and session bootstrap
//!
//! this module is the only production path that creates worker-local str0m state
//! negotiation calls it before offer creation so the packet loop can
//! later treat sockets, ICE credentials, codec configuration and per-session
//! bookkeeping as already initialized worker-local state
//!
//! bootstrap stops at transport setup
//! room policy, media registration, SDP
//! staging and packet routing stay in the worker modules responsible for those
//! contracts

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc;
use str0m::{
    Candidate, Rtc,
    bwe::Bitrate as Str0mBitrate,
    format::{Codec, CodecConfig, FormatParams},
    media::Frequency,
};
use tracing::info;

use super::{
    local_send_rewrite::ConsumerStreamStore,
    packet_loop::{RtcUdpSocket, UdpIngress},
    slots::SessionStore,
    state::{RtcSessionState, SessionSdpNegotiationState, SharedRtcSocket},
};
use crate::{
    Bitrate, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend,
    engine::{
        h264_payloads::H264_PAYLOAD_SPECS,
        media_transport::{TransportAdapterError, TransportSessionKey},
    },
};

const VIDEO_PAYLOAD_TYPE_VP8: u8 = 96;

/// bind the shared worker UDP socket and return the advertised candidate tuple
///
/// the bind address uses the unspecified address for the configured public IP
/// family so one socket can receive traffic for every session assigned to the
/// worker
/// the advertised candidate keeps the configured public IP with the
/// bound port because that is what browsers must see in SDP
///
/// ports are tried in configured order
/// a port that cannot be bound, switched to
/// nonblocking mode or converted into an RTC UDP socket is skipped so startup can
/// continue with the next candidate
///
/// # errors
///
/// returns `TransportUnavailable` when no port in the configured range produces
/// a usable shared socket
pub(super) fn bind_shared_rtc_socket(
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    rtc_udp_io_backend: RtcUdpIoBackend,
) -> Result<SharedRtcSocket, TransportAdapterError> {
    let bind_ip = bind_ip_for_public_ip(public_ip);
    for port in rtc_port_range.ports() {
        let bind_addr = SocketAddr::new(bind_ip, port);
        match StdUdpSocket::bind(bind_addr) {
            Ok(socket) => {
                if socket.set_nonblocking(true).is_err() {
                    continue;
                }
                let Ok(socket) = RtcUdpSocket::from_std(socket, rtc_udp_io_backend) else {
                    continue;
                };
                let candidate_addr = SocketAddr::new(public_ip, port);
                let ingress = UdpIngress::new(socket.clone(), bind_addr, candidate_addr);
                info!(
                    %bind_addr,
                    %candidate_addr,
                    "booted shared rtc UDP socket"
                );
                return Ok(SharedRtcSocket {
                    socket,
                    ingress,
                    candidate_addr,
                });
            }
            Err(_error) => {}
        }
    }
    Err(TransportAdapterError::TransportUnavailable)
}

/// choose the local bind address that matches the advertised IP family
///
/// workers bind to all local interfaces for that family while keeping SDP
/// candidates anchored to the configured public address
fn bind_ip_for_public_ip(public_ip: IpAddr) -> IpAddr {
    match public_ip {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

/// ensure one worker-local [`RtcSessionState`] exists for a session
///
/// this is idempotent because negotiation may ask for readiness more than once
/// while a session is still alive
/// `Ok(true)` means a fresh [`str0m::Rtc`] was created and inserted
/// `Ok(false)` means the existing session state still matches that key
///
/// new sessions start in ICE-lite mode, with RTP mode enabled, bandwidth
/// estimation capped by `max_bitrate_out` and exactly one local host candidate
/// attached to the shared worker socket's advertised address
///
/// # errors
///
/// returns `TransportUnavailable` if the local candidate cannot be represented
/// by str0m or cannot be attached to the newly created rtc state
#[cfg(any(test, feature = "internal-benchmarks", feature = "fuzzing"))]
pub(super) fn ensure_session_rtc_state(
    users: &mut SessionStore,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out: Bitrate,
    codec_flags: MediaCodecFlags,
) -> Result<bool, TransportAdapterError> {
    ensure_session_rtc_state_with_stats_interval(
        users,
        session_key,
        candidate_addr,
        max_bitrate_out,
        codec_flags,
        None,
    )
}

pub(super) fn ensure_session_rtc_state_with_stats_interval(
    users: &mut SessionStore,
    session_key: &TransportSessionKey,
    candidate_addr: SocketAddr,
    max_bitrate_out: Bitrate,
    codec_flags: MediaCodecFlags,
    stats_interval: Option<Duration>,
) -> Result<bool, TransportAdapterError> {
    if users.contains_key(session_key) {
        return Ok(false);
    }
    let started_at = Instant::now();
    let mut rtc = rtc_builder(codec_flags, stats_interval)
        .enable_bwe(Some(Str0mBitrate::bps(max_bitrate_out.as_bps())))
        .set_ice_lite(true)
        .build(started_at);
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
            receiver_bwe_target: None,
            #[cfg(test)]
            receiver_bwe_str0m_update_count: 0,
            dtls_started: false,
            packet_loop_dirty: false,
            sdp_negotiation: SessionSdpNegotiationState::default(),
            consumer_streams: ConsumerStreamStore::default(),
        },
    );
    Ok(true)
}

/// build the codec and RTP configuration used for newly created sessions
///
/// codec flags are applied before any media is declared so initial offers and
/// later staged media additions share one capability surface
/// the builder runs
/// in RTP mode because the SFU forwards RTP packets through worker-local str0m
/// sessions instead of using data channels or peer-connection media sources
fn rtc_builder(codec_flags: MediaCodecFlags, stats_interval: Option<Duration>) -> str0m::RtcConfig {
    let mut config = Rtc::builder()
        .clear_codecs()
        .enable_opus(codec_flags.opus_enabled())
        .enable_pcmu(codec_flags.pcmu_enabled())
        .enable_pcma(codec_flags.pcma_enabled())
        .set_rtp_mode(true);
    if let Some(stats_interval) = stats_interval {
        config = config.set_stats_interval(Some(stats_interval));
    }
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

/// register the h264 payload types that match the existing browser contract
///
/// these entries omit RTX because local forwarding projects one
/// receiver-safe RTP stream per consumer and does not model retransmission as a
/// separate negotiated payload in this bootstrap path
fn add_h264_codecs_without_rtx(codec_config: &mut CodecConfig) {
    for spec in H264_PAYLOAD_SPECS {
        codec_config.add_h264(
            spec.payload_type().into(),
            None,
            spec.packetization_mode().str0m_flag(),
            spec.profile_level_id(),
        );
    }
}

#[cfg(test)]
#[path = "TESTS/bootstrap.rs"]
mod tests;
