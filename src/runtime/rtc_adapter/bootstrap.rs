use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::Instant,
};

use serde_json::json;
use str0m::config::Fingerprint;
use str0m::{Candidate, IceCreds, Rtc};
use tokio::net::UdpSocket;

use super::state::{RtcSessionState, SessionTransportIds, SharedRtcSocket};
use crate::config::RtcPortRange;
use crate::rfc::webrtc;
use crate::runtime::transport_adapter::{TransportAdapterError, TransportSessionKey};
use crate::signaling::webrtc::{
    DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, TransportBootstrap,
};

use crate::runtime::transport_bootstrap;

const HOST_CANDIDATE_FOUNDATION: &str = "rtc-host";
const ICE_LOCAL_PREFERENCE_MAX: u16 = u16::MAX;
const SESSION_TRANSPORT_ID_UPLOAD_PREFIX: &str = "cts-rtc";
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
) -> Result<(), TransportAdapterError> {
    if sessions.contains_key(session_key) {
        return Ok(());
    }
    let mut rtc = Rtc::builder().set_ice_lite(true).build(Instant::now());
    let candidate = Candidate::host(candidate_addr, webrtc::ice::transport::UDP)
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    if rtc.add_local_candidate(candidate).is_none() {
        return Err(TransportAdapterError::TransportUnavailable);
    }
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
    let local_ice_credentials = rtc.direct_api().local_ice_credentials();
    let local_dtls_fingerprint = rtc.direct_api().local_dtls_fingerprint().clone();
    sessions.insert(
        session_key.clone(),
        RtcSessionState {
            rtc,
            local_ice_credentials,
            local_dtls_fingerprint,
            transport_ids,
            remote_dtls_fingerprint: None,
            remote_ice_credentials: None,
            dtls_started: false,
            recv_mids: Vec::new(),
            send_mids: Vec::new(),
        },
    );
    Ok(())
}

pub(super) fn build_transport_bootstrap(
    id: &str,
    candidate_addr: SocketAddr,
    local_ice_credentials: &IceCreds,
    local_dtls_fingerprint: &Fingerprint,
) -> TransportBootstrap {
    TransportBootstrap {
        id: id.to_owned(),
        ice_parameters: build_ice_parameters(local_ice_credentials),
        ice_candidates: vec![build_host_candidate(candidate_addr)],
        dtls_parameters: DtlsParameters {
            role: String::from("auto"),
            fingerprints: vec![wire_dtls_fingerprint(local_dtls_fingerprint)],
        },
        sctp_parameters: transport_bootstrap::default_sctp_parameters(),
    }
}

fn build_ice_parameters(local_ice_credentials: &IceCreds) -> IceParameters {
    IceParameters(json!({
        "usernameFragment": local_ice_credentials.ufrag,
        "password": local_ice_credentials.pass,
        "iceLite": true
    }))
}

fn build_host_candidate(candidate_addr: SocketAddr) -> IceCandidate {
    IceCandidate {
        foundation: String::from(HOST_CANDIDATE_FOUNDATION),
        priority: host_candidate_priority(),
        ip: candidate_addr.ip().to_string(),
        protocol: String::from(webrtc::ice::transport::UDP),
        port: u64::from(candidate_addr.port()),
        candidate_type: String::from(webrtc::ice::candidate_type::HOST),
    }
}

fn wire_dtls_fingerprint(fingerprint: &Fingerprint) -> DtlsFingerprint {
    let rendered = fingerprint.to_string();
    let (algorithm, value) = rendered.split_once(' ').unwrap_or(("sha-256", ""));
    DtlsFingerprint {
        algorithm: algorithm.to_owned(),
        value: value.to_owned(),
    }
}

fn host_candidate_priority() -> u64 {
    // RFC 8445 section 5.1.2.1 computes candidate priority as
    // (2^24 * type preference) + (2^8 * local preference) + (256 - component ID).
    (u64::from(webrtc::ice::type_preference::HOST) << 24)
        + (u64::from(ICE_LOCAL_PREFERENCE_MAX) << 8)
        + u64::from(256_u16 - webrtc::ice::component::RTP)
}
