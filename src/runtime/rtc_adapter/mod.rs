use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use super::{
    stub_bus::StubWebRtcAdapter,
    transport_adapter::{TransportAdapter, TransportAdapterError, TransportConnectDirection},
};
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    shared::SessionId,
    webrtc::{DtlsParameters, IceCandidate},
};
use o_sfu_router::ParseDiagnosticKind;
use tracing::{debug, error, warn};

mod dtls;
mod ice;
mod parse_diagnostic;
mod sdp;

const CANDIDATE_COMPONENT_ID_RTP: u16 = 1;

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

/// Placeholder transport adapter for the selected phase-7 backend (`rtc`).
///
/// During the library-selection phase this delegates to the deterministic stub
/// transport behavior so signaling and channel lifecycle flows remain stable
/// while SDP/ICE/DTLS integration is added incrementally.
#[derive(Debug, Default)]
pub(super) struct RtcTransportAdapter {
    fallback: StubWebRtcAdapter,
    transport_states: Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>,
}

impl TransportAdapter for RtcTransportAdapter {
    fn transport_bootstrap_payload(
        &self,
        session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let payload = self
            .fallback
            .transport_bootstrap_payload(session_id, router_capabilities)?;
        validate_bootstrap_payload(&payload)?;
        self.mark_bootstrap_sent(session_id)?;
        Ok(payload)
    }

    fn connect_transport(
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
            .connect_transport(session_id, direction, dtls_parameters, sdp_offer)?;
        self.mark_connected(session_id, direction)?;
        Ok(())
    }
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
        runtime::transport_adapter::{
            TransportAdapter, TransportAdapterError, TransportConnectDirection,
        },
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

    #[test]
    fn rtc_transport_connect_rejects_invalid_dtls_before_fallback() {
        let adapter = RtcTransportAdapter::default();
        let result = adapter.connect_transport(
            &SessionId::Integer(7),
            TransportConnectDirection::Upload,
            &DtlsParameters {
                role: String::from("client"),
                fingerprints: vec![],
            },
            None,
        );
        assert_eq!(result, Err(TransportAdapterError::InvalidInput));
    }

    #[test]
    fn rtc_transport_connect_requires_bootstrap_first() {
        let adapter = RtcTransportAdapter::default();
        let result = adapter.connect_transport(
            &SessionId::Integer(8),
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
        );
        assert_eq!(result, Err(TransportAdapterError::TransportUnavailable));
    }

    #[test]
    fn rtc_transport_connect_succeeds_after_bootstrap() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(9);
        let bootstrap_result =
            adapter.transport_bootstrap_payload(&session_id, &empty_router_capabilities());
        assert!(bootstrap_result.is_ok());
        let connect_result = adapter.connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            Some(VALID_SDP_OFFER),
        );
        assert_eq!(connect_result, Ok(()));
    }

    #[test]
    fn rtc_transport_connect_rejects_duplicate_direction_connect() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(10);
        let bootstrap_result =
            adapter.transport_bootstrap_payload(&session_id, &empty_router_capabilities());
        assert!(bootstrap_result.is_ok());
        let first_connect = adapter.connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
        );
        assert_eq!(first_connect, Ok(()));
        let second_connect = adapter.connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
        );
        assert_eq!(second_connect, Err(TransportAdapterError::InvalidInput));
    }

    #[test]
    fn rtc_transport_connect_rejects_invalid_sdp_before_fallback() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(11);
        let bootstrap_result =
            adapter.transport_bootstrap_payload(&session_id, &empty_router_capabilities());
        assert!(bootstrap_result.is_ok());
        let connect_result = adapter.connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            Some("v=0\r\ns=-\r\nt=0 0\r\n"),
        );
        assert_eq!(connect_result, Err(TransportAdapterError::InvalidInput));
    }

    #[test]
    fn rtc_transport_connect_rejects_unsupported_sdp_before_fallback() {
        let adapter = RtcTransportAdapter::default();
        let session_id = SessionId::Integer(12);
        let bootstrap_result =
            adapter.transport_bootstrap_payload(&session_id, &empty_router_capabilities());
        assert!(bootstrap_result.is_ok());
        let connect_result = adapter.connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            Some("m=audio 9 RTP/SAVPF 111\r\n"),
        );
        assert_eq!(
            connect_result,
            Err(TransportAdapterError::UnsupportedFeature)
        );
    }
}
