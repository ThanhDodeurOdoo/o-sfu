use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::Instant,
};

#[cfg(any(test, feature = "internal-benchmarks"))]
use str0m::IceCreds;
#[cfg(any(test, feature = "internal-benchmarks"))]
use str0m::config::Fingerprint;
use str0m::{Candidate, Rtc};
use tokio::net::UdpSocket;

use super::packet_mode::ACTIVE_PACKET_MODE;
#[cfg(any(test, feature = "internal-benchmarks"))]
use super::state::SessionTransportIds;
use super::state::{RtcSessionState, SessionSdpNegotiationState, SharedRtcSocket};
use crate::config::MediaCodecFlags;
use crate::config::RtcPortRange;
use crate::rfc::webrtc;
use crate::runtime::transport_adapter::{TransportAdapterError, TransportSessionKey};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::runtime::transport_bootstrap::{
    self, TransportDtlsFingerprint, TransportDtlsFingerprintAlgorithm, TransportDtlsParameters,
    TransportDtlsRole, TransportEndpointBootstrap, TransportIceCandidate,
    TransportIceCandidateType, TransportIceParameters, TransportIceProtocol,
};

#[cfg(any(test, feature = "internal-benchmarks"))]
const HOST_CANDIDATE_FOUNDATION: &str = "rtc-host";
#[cfg(any(test, feature = "internal-benchmarks"))]
const ICE_LOCAL_PREFERENCE_MAX: u16 = u16::MAX;
#[cfg(any(test, feature = "internal-benchmarks"))]
const SESSION_TRANSPORT_ID_UPLOAD_PREFIX: &str = "cts-rtc";
#[cfg(any(test, feature = "internal-benchmarks"))]
const SESSION_TRANSPORT_ID_DOWNLOAD_PREFIX: &str = "stc-rtc";

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
                return Ok(SharedRtcSocket {
                    socket: Arc::new(socket),
                    candidate_addr: SocketAddr::new(public_ip, port),
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
    codec_flags: MediaCodecFlags,
) -> Result<bool, TransportAdapterError> {
    if sessions.contains_key(session_key) {
        return Ok(false);
    }
    let started_at = Instant::now();
    let mut rtc = rtc_builder(codec_flags)
        .set_ice_lite(true)
        .build(started_at);
    let candidate = Candidate::host(candidate_addr, webrtc::IceTransport::Udp.as_str())
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    if rtc.add_local_candidate(candidate).is_none() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    #[cfg(any(test, feature = "internal-benchmarks"))]
    let transport_ids = SessionTransportIds {
        upload: format!(
            "{SESSION_TRANSPORT_ID_UPLOAD_PREFIX}-{}",
            uuid::Uuid::new_v4()
        ),
        download: format!(
            "{SESSION_TRANSPORT_ID_DOWNLOAD_PREFIX}-{}",
            uuid::Uuid::new_v4()
        ),
    };
    #[cfg(any(test, feature = "internal-benchmarks"))]
    let local_ice_credentials = rtc.direct_api().local_ice_credentials();
    #[cfg(any(test, feature = "internal-benchmarks"))]
    let local_dtls_fingerprint = rtc.direct_api().local_dtls_fingerprint().clone();
    sessions.insert(
        session_key.clone(),
        RtcSessionState {
            rtc,
            started_at,
            #[cfg(any(test, feature = "internal-benchmarks"))]
            local_ice_credentials,
            #[cfg(any(test, feature = "internal-benchmarks"))]
            local_dtls_fingerprint,
            #[cfg(any(test, feature = "internal-benchmarks"))]
            transport_ids,
            #[cfg(test)]
            remote_dtls_fingerprint: None,
            #[cfg(test)]
            remote_ice_credentials: None,
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
        .set_rtp_mode(ACTIVE_PACKET_MODE.uses_str0m_rtp_mode())
}

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(super) fn build_transport_bootstrap(
    id: &str,
    candidate_addr: SocketAddr,
    local_ice_credentials: &IceCreds,
    local_dtls_fingerprint: &Fingerprint,
) -> TransportEndpointBootstrap {
    TransportEndpointBootstrap {
        id: id.to_owned(),
        ice_parameters: build_ice_parameters(local_ice_credentials),
        ice_candidates: vec![build_host_candidate(candidate_addr)],
        dtls_parameters: TransportDtlsParameters {
            role: TransportDtlsRole::Auto,
            fingerprints: vec![wire_dtls_fingerprint(local_dtls_fingerprint)],
        },
        sctp_parameters: transport_bootstrap::default_sctp_parameters(),
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
fn build_ice_parameters(local_ice_credentials: &IceCreds) -> TransportIceParameters {
    TransportIceParameters {
        username_fragment: local_ice_credentials.ufrag.clone(),
        password: local_ice_credentials.pass.clone(),
        ice_lite: true,
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
fn build_host_candidate(candidate_addr: SocketAddr) -> TransportIceCandidate {
    TransportIceCandidate {
        foundation: String::from(HOST_CANDIDATE_FOUNDATION),
        priority: host_candidate_priority(),
        ip: candidate_addr.ip(),
        protocol: TransportIceProtocol::Udp,
        port: candidate_addr.port(),
        candidate_type: TransportIceCandidateType::Host,
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
fn wire_dtls_fingerprint(fingerprint: &Fingerprint) -> TransportDtlsFingerprint {
    let rendered = fingerprint.to_string();
    let (_algorithm, value) = rendered
        .split_once(' ')
        .unwrap_or((webrtc::DtlsFingerprintAlgorithm::Sha256.as_str(), ""));
    TransportDtlsFingerprint {
        algorithm: TransportDtlsFingerprintAlgorithm::Sha256,
        value: value.to_owned(),
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
fn host_candidate_priority() -> u64 {
    // RFC 8445 section 5.1.2.1 computes candidate priority as
    // (2^24 * type preference) + (2^8 * local preference) + (256 - component ID).
    (u64::from(webrtc::ice::type_preference::HOST) << 24)
        + (u64::from(ICE_LOCAL_PREFERENCE_MAX) << 8)
        + u64::from(256_u16 - webrtc::ice::component::RTP)
}
