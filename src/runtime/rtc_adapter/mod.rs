use std::{
    collections::BTreeMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
    time::Instant,
};

use super::{
    stub_bus::StubWebRtcAdapter,
    transport_adapter::{TransportAdapterError, TransportConnectDirection},
    transport_bootstrap,
};
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    shared::SessionId,
    webrtc::{DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, TransportBootstrap},
};
use crate::{config::RtcPortRange, rfc::webrtc};
use o_sfu_router::ParseDiagnosticKind;
use serde_json::json;
use str0m::config::Fingerprint;
use str0m::{Candidate, IceCreds, Rtc};
use tracing::{debug, error, warn};

mod dtls;
mod ice;
mod parse_diagnostic;
mod sdp;

const CANDIDATE_COMPONENT_ID_RTP: u16 = 1;
const HOST_CANDIDATE_FOUNDATION: &str = "rtc-host";
const ICE_LOCAL_PREFERENCE_MAX: u16 = u16::MAX;
const SESSION_TRANSPORT_ID_UPLOAD_PREFIX: &str = "cts-rtc";
const SESSION_TRANSPORT_ID_DOWNLOAD_PREFIX: &str = "stc-rtc";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportLifecycleState {
    BootstrapSent,
    Connected,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TransportStateKey {
    session_id: SessionId,
    direction: TransportConnectDirection,
}

struct SharedRtcSocket {
    #[allow(
        dead_code,
        reason = "reserved now for the upcoming packet loop and kept alive for the advertised ICE port"
    )]
    socket: UdpSocket,
    candidate_addr: SocketAddr,
}

struct RtcSessionState {
    #[allow(
        dead_code,
        reason = "real rtc state is created now and will be driven by the packet loop in the next phase"
    )]
    rtc: Rtc,
    local_ice_credentials: IceCreds,
    local_dtls_fingerprint: Fingerprint,
    transport_ids: SessionTransportIds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionTransportIds {
    upload: String,
    download: String,
}

#[derive(Default)]
struct RtcBootstrapState {
    shared_socket: Option<SharedRtcSocket>,
    sessions: BTreeMap<SessionId, RtcSessionState>,
}

/// Runtime transport adapter for the phase-7 `rtc` backend.
///
/// The adapter now performs real ICE-lite bootstrap work with `str0m` while
/// transport connect, packet-loop driving, and media forwarding remain staged
/// behind the same boundary for later steps.
pub(super) struct RtcTransportAdapter {
    fallback: StubWebRtcAdapter,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    bootstrap_state: Arc<Mutex<RtcBootstrapState>>,
    transport_states: Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>,
}

impl RtcTransportAdapter {
    pub(super) fn new(public_ip: IpAddr, rtc_port_range: RtcPortRange) -> Self {
        Self {
            fallback: StubWebRtcAdapter::default(),
            public_ip,
            rtc_port_range,
            bootstrap_state: Arc::new(Mutex::new(RtcBootstrapState::default())),
            transport_states: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[allow(
        clippy::unused_async,
        reason = "the transport-adapter boundary stays async so packet-loop work can be added without changing runtime call sites"
    )]
    pub(super) async fn transport_bootstrap_payload(
        &self,
        session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let payload = self.bootstrap_transport_payload(session_id, router_capabilities)?;
        validate_bootstrap_payload(&payload)?;
        self.mark_bootstrap_sent(session_id)?;
        Ok(payload)
    }

    pub(super) async fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        if let Some(sdp_offer) = sdp_offer {
            validate_sdp_offer(sdp_offer)?;
        }
        validate_dtls_parameters(dtls_parameters)?;
        self.ensure_connect_transition(session_id, direction)?;
        debug!(
            ?direction,
            session_id = ?session_id,
            "validated DTLS parameters and transport lifecycle state before placeholder rtc connect"
        );
        self.fallback
            .connect_transport(session_id, direction, dtls_parameters, sdp_offer)
            .await?;
        self.mark_connected(session_id, direction)?;
        Ok(())
    }

    fn bootstrap_transport_payload(
        &self,
        session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let Ok(mut bootstrap_state) = self.bootstrap_state.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let candidate_addr = if let Some(shared_socket) = bootstrap_state.shared_socket.as_ref() {
            shared_socket.candidate_addr
        } else {
            let shared_socket = bind_shared_rtc_socket(self.public_ip, self.rtc_port_range)?;
            let candidate_addr = shared_socket.candidate_addr;
            bootstrap_state.shared_socket = Some(shared_socket);
            candidate_addr
        };
        ensure_session_rtc_state(&mut bootstrap_state.sessions, session_id, candidate_addr)?;
        let Some(session_state) = bootstrap_state.sessions.get(session_id) else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        Ok(transport_bootstrap::transport_bootstrap_payload(
            router_capabilities,
            build_transport_bootstrap(
                session_state.transport_ids.download.as_str(),
                candidate_addr,
                &session_state.local_ice_credentials,
                &session_state.local_dtls_fingerprint,
            ),
            build_transport_bootstrap(
                session_state.transport_ids.upload.as_str(),
                candidate_addr,
                &session_state.local_ice_credentials,
                &session_state.local_dtls_fingerprint,
            ),
        ))
    }
}

impl fmt::Debug for RtcTransportAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RtcTransportAdapter")
            .field("public_ip", &self.public_ip)
            .field("rtc_port_range", &self.rtc_port_range)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl Default for RtcTransportAdapter {
    fn default() -> Self {
        Self::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(40_000, 49_999),
        )
    }
}

fn bind_shared_rtc_socket(
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
) -> Result<SharedRtcSocket, TransportAdapterError> {
    let bind_ip = bind_ip_for_public_ip(public_ip);
    for port in rtc_port_range.ports() {
        let bind_addr = SocketAddr::new(bind_ip, port);
        match UdpSocket::bind(bind_addr) {
            Ok(socket) => {
                return Ok(SharedRtcSocket {
                    socket,
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

fn ensure_session_rtc_state(
    sessions: &mut BTreeMap<SessionId, RtcSessionState>,
    session_id: &SessionId,
    candidate_addr: SocketAddr,
) -> Result<(), TransportAdapterError> {
    if sessions.contains_key(session_id) {
        return Ok(());
    }
    let mut rtc = Rtc::builder().set_ice_lite(true).build(Instant::now());
    let candidate = Candidate::host(candidate_addr, webrtc::ICE_TRANSPORT_UDP)
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
        session_id.clone(),
        RtcSessionState {
            rtc,
            local_ice_credentials,
            local_dtls_fingerprint,
            transport_ids,
        },
    );
    Ok(())
}

fn build_transport_bootstrap(
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
        protocol: String::from(webrtc::ICE_TRANSPORT_UDP),
        port: u64::from(candidate_addr.port()),
        candidate_type: String::from(webrtc::ICE_CANDIDATE_TYPE_HOST),
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
    (u64::from(webrtc::ICE_TYPE_PREFERENCE_HOST) << 24)
        + (u64::from(ICE_LOCAL_PREFERENCE_MAX) << 8)
        + u64::from(256_u16 - webrtc::ICE_COMPONENT_RTP)
}

fn validate_sdp_offer(sdp_offer: &str) -> Result<(), TransportAdapterError> {
    let parsed_offer = sdp::parse_offer_sdp(sdp_offer).map_err(|diagnostic| {
        map_sdp_diagnostic_to_adapter_error(diagnostic.as_ref(), diagnostic.replay_context())
    })?;
    log_validated_sdp_media_sections(parsed_offer.media_sections());
    Ok(())
}

fn map_sdp_diagnostic_to_adapter_error(
    diagnostic: &sdp::SdpParseDiagnostic,
    replay_context: &str,
) -> TransportAdapterError {
    match diagnostic {
        sdp::SdpParseDiagnostic::InvalidInput { context, .. } => {
            error!(
                summary = diagnostic.summary(),
                expected = context.expected(),
                got = context.got(),
                line_number = context.line_number().map_or(0, |line| line),
                line = context.line().unwrap_or(""),
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                rfc_url = diagnostic.rfc_reference().url(),
                replay_context,
                "invalid SDP offer on rtc adapter boundary"
            );
            TransportAdapterError::InvalidInput
        }
        sdp::SdpParseDiagnostic::UnsupportedFeature { context, .. } => {
            warn!(
                summary = diagnostic.summary(),
                got = context.got(),
                line_number = context.line_number(),
                line = context.line(),
                rfc_document = diagnostic.rfc_reference().document(),
                rfc_section = diagnostic.rfc_reference().section(),
                rfc_url = diagnostic.rfc_reference().url(),
                replay_context,
                "unsupported SDP feature on rtc adapter boundary"
            );
            TransportAdapterError::UnsupportedFeature
        }
    }
}

fn log_validated_sdp_media_sections(media_sections: &[sdp::ParsedMediaSection]) {
    debug!(
        media_section_count = media_sections.len(),
        "validated SDP offer on rtc adapter boundary"
    );
    for section in media_sections {
        debug!(
            media_kind = ?section.media_kind(),
            port = section.port(),
            transport_protocol = ?section.transport_protocol(),
            payload_format_count = section.formats().len(),
            "parsed SDP media section"
        );
    }
}

impl RtcTransportAdapter {
    fn mark_bootstrap_sent(&self, session_id: &SessionId) -> Result<(), TransportAdapterError> {
        let Ok(mut states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        for direction in [
            TransportConnectDirection::Upload,
            TransportConnectDirection::Download,
        ] {
            states.insert(
                TransportStateKey {
                    session_id: session_id.clone(),
                    direction,
                },
                TransportLifecycleState::BootstrapSent,
            );
        }
        Ok(())
    }

    fn ensure_connect_transition(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
    ) -> Result<(), TransportAdapterError> {
        let key = TransportStateKey {
            session_id: session_id.clone(),
            direction,
        };
        let Ok(states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        match states.get(&key) {
            Some(TransportLifecycleState::BootstrapSent) => Ok(()),
            Some(TransportLifecycleState::Connected) => Err(TransportAdapterError::InvalidInput),
            None => Err(TransportAdapterError::TransportUnavailable),
        }
    }

    fn mark_connected(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
    ) -> Result<(), TransportAdapterError> {
        let key = TransportStateKey {
            session_id: session_id.clone(),
            direction,
        };
        let Ok(mut states) = self.transport_states.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let Some(state) = states.get_mut(&key) else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        *state = TransportLifecycleState::Connected;
        Ok(())
    }
}

fn validate_dtls_parameters(dtls_parameters: &DtlsParameters) -> Result<(), TransportAdapterError> {
    match dtls::parse_dtls_parameters(dtls_parameters) {
        Ok(_parsed) => Ok(()),
        Err(diagnostic) => match diagnostic.kind() {
            ParseDiagnosticKind::InvalidInput => {
                error!(
                    summary = diagnostic.summary(),
                    rfc_document = diagnostic.rfc_reference().document(),
                    rfc_section = diagnostic.rfc_reference().section(),
                    rfc_url = diagnostic.rfc_reference().url(),
                    replay_context = diagnostic.replay_context(),
                    "invalid DTLS payload on rtc adapter boundary"
                );
                Err(TransportAdapterError::InvalidInput)
            }
            ParseDiagnosticKind::UnsupportedFeature => {
                warn!(
                    summary = diagnostic.summary(),
                    rfc_document = diagnostic.rfc_reference().document(),
                    rfc_section = diagnostic.rfc_reference().section(),
                    rfc_url = diagnostic.rfc_reference().url(),
                    replay_context = diagnostic.replay_context(),
                    "unsupported DTLS feature on rtc adapter boundary"
                );
                Err(TransportAdapterError::UnsupportedFeature)
            }
        },
    }
}

fn validate_bootstrap_payload(
    payload: &CurrentTransportBootstrapPayload,
) -> Result<(), TransportAdapterError> {
    validate_ice_candidates(
        payload.download_transport.id.as_str(),
        payload.download_transport.ice_candidates.as_slice(),
    )?;
    validate_ice_candidates(
        payload.upload_transport.id.as_str(),
        payload.upload_transport.ice_candidates.as_slice(),
    )?;
    Ok(())
}

fn validate_ice_candidates(
    transport_id: &str,
    candidates: &[IceCandidate],
) -> Result<(), TransportAdapterError> {
    for candidate in candidates {
        let line = candidate_to_sdp_line(candidate);
        match ice::parse_ice_candidate(line.as_str()) {
            Ok(_parsed) => {}
            Err(diagnostic) => match diagnostic.kind() {
                ParseDiagnosticKind::InvalidInput => {
                    error!(
                        transport_id,
                        summary = diagnostic.summary(),
                        rfc_document = diagnostic.rfc_reference().document(),
                        rfc_section = diagnostic.rfc_reference().section(),
                        rfc_url = diagnostic.rfc_reference().url(),
                        replay_context = diagnostic.replay_context(),
                        "invalid bootstrap ICE candidate on rtc adapter boundary"
                    );
                    return Err(TransportAdapterError::InvalidInput);
                }
                ParseDiagnosticKind::UnsupportedFeature => {
                    warn!(
                        transport_id,
                        summary = diagnostic.summary(),
                        rfc_document = diagnostic.rfc_reference().document(),
                        rfc_section = diagnostic.rfc_reference().section(),
                        rfc_url = diagnostic.rfc_reference().url(),
                        replay_context = diagnostic.replay_context(),
                        "unsupported bootstrap ICE candidate on rtc adapter boundary"
                    );
                    return Err(TransportAdapterError::UnsupportedFeature);
                }
            },
        }
    }
    Ok(())
}

fn candidate_to_sdp_line(candidate: &IceCandidate) -> String {
    format!(
        "candidate:{} {CANDIDATE_COMPONENT_ID_RTP} {} {} {} {} typ {}",
        candidate.foundation,
        candidate.protocol,
        candidate.priority,
        candidate.ip,
        candidate.port,
        candidate.candidate_type,
    )
}

#[cfg(test)]
mod tests {
    use o_sfu_router::RtpCapabilities as RouterRtpCapabilities;
    use serde_json::json;

    use super::{RtcTransportAdapter, validate_bootstrap_payload, validate_dtls_parameters};
    use crate::{
        runtime::transport_adapter::{TransportAdapterError, TransportConnectDirection},
        signaling::{
            current_protocol::CurrentTransportBootstrapPayload,
            shared::SessionId,
            webrtc::{
                DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, PublishOptions,
                PublishOptionsByMediaKind, RtpCapabilities as WireRtpCapabilities, SctpParameters,
                TransportBootstrap,
            },
        },
    };

    const VALID_SDP_OFFER: &str = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
t=0 0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n";

    fn empty_router_capabilities() -> RouterRtpCapabilities {
        RouterRtpCapabilities::new(vec![], vec![])
    }

    fn sample_bootstrap_payload(candidate: IceCandidate) -> CurrentTransportBootstrapPayload {
        CurrentTransportBootstrapPayload {
            router_capabilities: WireRtpCapabilities(json!({
                "codecs": [],
                "headerExtensions": []
            })),
            download_transport: sample_transport_bootstrap("stc-rtc", candidate.clone()),
            upload_transport: sample_transport_bootstrap("cts-rtc", candidate),
            publish_options_by_media_kind: PublishOptionsByMediaKind {
                audio: PublishOptions(json!({ "stopTracks": false })),
                video: PublishOptions(json!({ "stopTracks": false })),
            },
        }
    }

    fn sample_sha256_dtls_parameters(role: &str) -> DtlsParameters {
        DtlsParameters {
            role: role.to_owned(),
            fingerprints: vec![DtlsFingerprint {
                algorithm: String::from("sha-256"),
                value: String::from(
                    "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
                ),
            }],
        }
    }

    fn sample_candidate(protocol: &str, port: u64) -> IceCandidate {
        IceCandidate {
            foundation: String::from("foundation"),
            priority: 2_113_937_151,
            ip: String::from("203.0.113.10"),
            protocol: protocol.to_owned(),
            port,
            candidate_type: String::from("host"),
        }
    }

    fn sample_transport_bootstrap(id: &str, candidate: IceCandidate) -> TransportBootstrap {
        TransportBootstrap {
            id: id.to_owned(),
            ice_parameters: IceParameters(json!({
                "usernameFragment": "ufrag",
                "password": "pwd",
                "iceLite": true
            })),
            ice_candidates: vec![candidate],
            dtls_parameters: sample_sha256_dtls_parameters("auto"),
            sctp_parameters: SctpParameters(json!({
                "port": 5000,
                "OS": 1024,
                "MIS": 1024,
                "maxMessageSize": 262_144
            })),
        }
    }

    #[test]
    fn validate_dtls_parameters_accepts_client_sha256_payload() {
        let result = validate_dtls_parameters(&sample_sha256_dtls_parameters("client"));
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn validate_dtls_parameters_maps_invalid_payload_to_invalid_input() {
        let result = validate_dtls_parameters(&DtlsParameters {
            role: String::from("client"),
            fingerprints: vec![],
        });
        assert_eq!(result, Err(TransportAdapterError::InvalidInput));
    }

    #[test]
    fn validate_dtls_parameters_maps_unsupported_payload_to_unsupported_feature() {
        let result = validate_dtls_parameters(&DtlsParameters {
            role: String::from("client"),
            fingerprints: vec![DtlsFingerprint {
                algorithm: String::from("sha-1"),
                value: String::from("AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD"),
            }],
        });
        assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
    }

    #[test]
    fn validate_bootstrap_payload_accepts_supported_candidate_shape() {
        let payload = sample_bootstrap_payload(sample_candidate("udp", 40_000));
        let result = validate_bootstrap_payload(&payload);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn validate_bootstrap_payload_rejects_unsupported_candidate_shape() {
        let payload = sample_bootstrap_payload(sample_candidate("tcp", 9));
        let result = validate_bootstrap_payload(&payload);
        assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
    }

    #[tokio::test]
    async fn rtc_transport_connect_rejects_invalid_dtls_before_fallback() {
        let adapter = RtcTransportAdapter::default();
        let result = adapter
            .connect_transport(
                &SessionId::Integer(7),
                TransportConnectDirection::Upload,
                &DtlsParameters {
                    role: String::from("client"),
                    fingerprints: vec![],
                },
                None,
            )
            .await;
        assert_eq!(result, Err(TransportAdapterError::InvalidInput));
    }

    #[tokio::test]
    async fn rtc_transport_connect_requires_bootstrap_first() {
        let adapter = RtcTransportAdapter::default();
        let result = adapter
            .connect_transport(
                &SessionId::Integer(8),
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                None,
            )
            .await;
        assert_eq!(result, Err(TransportAdapterError::TransportUnavailable));
    }

    #[tokio::test]
    async fn rtc_transport_connect_succeeds_after_bootstrap() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(9);
        let bootstrap_result = adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await;
        assert!(bootstrap_result.is_ok());
        let connect_result = adapter
            .connect_transport(
                &session_id,
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                Some(VALID_SDP_OFFER),
            )
            .await;
        assert_eq!(connect_result, Ok(()));
    }

    #[tokio::test]
    async fn rtc_transport_bootstrap_uses_real_ice_and_dtls_parameters() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(13);
        let payload = adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await;
        assert!(payload.is_ok());
        let Some(payload) = payload.ok() else {
            return;
        };
        assert!(payload.download_transport.id.starts_with("stc-rtc-"));
        assert!(payload.upload_transport.id.starts_with("cts-rtc-"));
        assert_ne!(payload.download_transport.id, payload.upload_transport.id);
        assert_eq!(
            payload.download_transport.ice_parameters.0.get("iceLite"),
            Some(&json!(true))
        );
        assert_eq!(
            payload.upload_transport.ice_parameters.0.get("iceLite"),
            Some(&json!(true))
        );
        let download_candidate = payload.download_transport.ice_candidates.first();
        let upload_candidate = payload.upload_transport.ice_candidates.first();
        assert!(download_candidate.is_some());
        assert!(upload_candidate.is_some());
        let (Some(download_candidate), Some(upload_candidate)) =
            (download_candidate, upload_candidate)
        else {
            return;
        };
        assert_eq!(download_candidate.ip, "127.0.0.1");
        assert_eq!(upload_candidate.ip, "127.0.0.1");
        assert_eq!(download_candidate.port, upload_candidate.port);
        assert!((40_000..=49_999).contains(&download_candidate.port));
        let fingerprint = payload
            .download_transport
            .dtls_parameters
            .fingerprints
            .first();
        assert!(fingerprint.is_some());
        let Some(fingerprint) = fingerprint else {
            return;
        };
        assert_eq!(fingerprint.algorithm, "sha-256");
        assert_ne!(fingerprint.value, "AA:BB:CC");
        assert!(fingerprint.value.contains(':'));
    }

    #[tokio::test]
    async fn rtc_transport_connect_rejects_duplicate_direction_connect() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(10);
        let bootstrap_result = adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await;
        assert!(bootstrap_result.is_ok());
        let first_connect = adapter
            .connect_transport(
                &session_id,
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                None,
            )
            .await;
        assert_eq!(first_connect, Ok(()));
        let second_connect = adapter
            .connect_transport(
                &session_id,
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                None,
            )
            .await;
        assert_eq!(second_connect, Err(TransportAdapterError::InvalidInput));
    }

    #[tokio::test]
    async fn rtc_transport_connect_rejects_invalid_sdp_before_fallback() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(11);
        let bootstrap_result = adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await;
        assert!(bootstrap_result.is_ok());
        let connect_result = adapter
            .connect_transport(
                &session_id,
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                Some("v=0\r\ns=-\r\nt=0 0\r\n"),
            )
            .await;
        assert_eq!(connect_result, Err(TransportAdapterError::InvalidInput));
    }

    #[tokio::test]
    async fn rtc_transport_connect_rejects_unsupported_sdp_before_fallback() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(12);
        let bootstrap_result = adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await;
        assert!(bootstrap_result.is_ok());
        let connect_result = adapter
            .connect_transport(
                &session_id,
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                Some("m=audio 9 RTP/SAVPF 111\r\n"),
            )
            .await;
        assert_eq!(
            connect_result,
            Err(TransportAdapterError::UnsupportedFeature)
        );
    }
}
