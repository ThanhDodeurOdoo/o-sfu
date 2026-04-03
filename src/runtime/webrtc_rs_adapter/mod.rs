use super::{
    stub_bus::StubWebRtcAdapter,
    transport_adapter::{TransportAdapter, TransportAdapterError, TransportConnectDirection},
};
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    shared::SessionId,
    webrtc::{DtlsParameters, IceCandidate},
};
use tracing::{debug, error, warn};

mod dtls;
mod ice;
#[allow(
    dead_code,
    reason = "Phase-7 SDP parsing scaffolding is prepared before transport wiring starts using it."
)]
mod sdp;

const CANDIDATE_FIELD_FOUNDATION: &str = "foundation";
const CANDIDATE_FIELD_PRIORITY: &str = "priority";
const CANDIDATE_FIELD_IP: &str = "ip";
const CANDIDATE_FIELD_PROTOCOL: &str = "protocol";
const CANDIDATE_FIELD_PORT: &str = "port";
const CANDIDATE_FIELD_TYPE: &str = "type";
const CANDIDATE_COMPONENT_ID_RTP: u16 = 1;

/// Placeholder transport adapter for the selected phase-7 backend (`webrtc-rs`).
///
/// During the library-selection phase this delegates to the deterministic stub
/// transport behavior so signaling and channel lifecycle flows remain stable
/// while SDP/ICE/DTLS integration is added incrementally.
#[derive(Debug, Default)]
pub(super) struct WebRtcRsTransportAdapter {
    fallback: StubWebRtcAdapter,
}

impl TransportAdapter for WebRtcRsTransportAdapter {
    fn transport_bootstrap_payload(
        &self,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        let payload = self
            .fallback
            .transport_bootstrap_payload(router_capabilities)?;
        validate_bootstrap_payload(&payload)?;
        Ok(payload)
    }

    fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
    ) -> Result<(), TransportAdapterError> {
        validate_dtls_parameters(dtls_parameters)?;
        debug!(
            ?direction,
            session_id = ?session_id,
            "validated DTLS parameters before placeholder webrtc-rs transport connect"
        );
        self.fallback
            .connect_transport(session_id, direction, dtls_parameters)
    }
}

fn validate_dtls_parameters(dtls_parameters: &DtlsParameters) -> Result<(), TransportAdapterError> {
    match dtls::parse_dtls_parameters(dtls_parameters) {
        Ok(_parsed) => Ok(()),
        Err(diagnostic) => match diagnostic.kind() {
            dtls::DtlsDiagnosticKind::InvalidInput => {
                error!(
                    summary = diagnostic.summary(),
                    rfc_document = diagnostic.rfc_reference().document(),
                    rfc_section = diagnostic.rfc_reference().section(),
                    rfc_url = diagnostic.rfc_reference().url(),
                    replay_context = diagnostic.replay_context(),
                    "invalid DTLS payload on webrtc-rs adapter boundary"
                );
                Err(TransportAdapterError::InvalidInput)
            }
            dtls::DtlsDiagnosticKind::UnsupportedFeature => {
                warn!(
                    summary = diagnostic.summary(),
                    rfc_document = diagnostic.rfc_reference().document(),
                    rfc_section = diagnostic.rfc_reference().section(),
                    rfc_url = diagnostic.rfc_reference().url(),
                    replay_context = diagnostic.replay_context(),
                    "unsupported DTLS feature on webrtc-rs adapter boundary"
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
        let line = candidate_to_sdp_line(candidate)?;
        match ice::parse_ice_candidate(line.as_str()) {
            Ok(_parsed) => {}
            Err(diagnostic) => match diagnostic.kind() {
                ice::IceDiagnosticKind::InvalidInput => {
                    error!(
                        transport_id,
                        summary = diagnostic.summary(),
                        rfc_document = diagnostic.rfc_reference().document(),
                        rfc_section = diagnostic.rfc_reference().section(),
                        rfc_url = diagnostic.rfc_reference().url(),
                        replay_context = diagnostic.replay_context(),
                        "invalid bootstrap ICE candidate on webrtc-rs adapter boundary"
                    );
                    return Err(TransportAdapterError::InvalidInput);
                }
                ice::IceDiagnosticKind::UnsupportedFeature => {
                    warn!(
                        transport_id,
                        summary = diagnostic.summary(),
                        rfc_document = diagnostic.rfc_reference().document(),
                        rfc_section = diagnostic.rfc_reference().section(),
                        rfc_url = diagnostic.rfc_reference().url(),
                        replay_context = diagnostic.replay_context(),
                        "unsupported bootstrap ICE candidate on webrtc-rs adapter boundary"
                    );
                    return Err(TransportAdapterError::UnsupportedFeature);
                }
            },
        }
    }
    Ok(())
}

fn candidate_to_sdp_line(candidate: &IceCandidate) -> Result<String, TransportAdapterError> {
    let Some(object) = candidate.0.as_object() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let foundation = extract_candidate_string_field(object, CANDIDATE_FIELD_FOUNDATION)?;
    let priority = extract_candidate_u64_field(object, CANDIDATE_FIELD_PRIORITY)?;
    let ip = extract_candidate_string_field(object, CANDIDATE_FIELD_IP)?;
    let protocol = extract_candidate_string_field(object, CANDIDATE_FIELD_PROTOCOL)?;
    let port = extract_candidate_u64_field(object, CANDIDATE_FIELD_PORT)?;
    let candidate_type = extract_candidate_string_field(object, CANDIDATE_FIELD_TYPE)?;
    Ok(format!(
        "candidate:{foundation} {CANDIDATE_COMPONENT_ID_RTP} {protocol} {priority} {ip} {port} typ {candidate_type}"
    ))
}

fn extract_candidate_string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, TransportAdapterError> {
    let Some(value) = object.get(key) else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let Some(string) = value.as_str() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    Ok(string.to_owned())
}

fn extract_candidate_u64_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<u64, TransportAdapterError> {
    let Some(value) = object.get(key) else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let Some(number) = value.as_u64() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    Ok(number)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{WebRtcRsTransportAdapter, validate_bootstrap_payload, validate_dtls_parameters};
    use crate::{
        runtime::transport_adapter::{
            TransportAdapter, TransportAdapterError, TransportConnectDirection,
        },
        signaling::{
            current_protocol::CurrentTransportBootstrapPayload,
            shared::SessionId,
            webrtc::{
                DtlsParameters, IceCandidate, IceParameters, PublishOptions,
                PublishOptionsByMediaKind, RtpCapabilities, SctpParameters, TransportBootstrap,
            },
        },
    };

    fn sample_bootstrap_payload(candidate: IceCandidate) -> CurrentTransportBootstrapPayload {
        CurrentTransportBootstrapPayload {
            router_capabilities: RtpCapabilities(json!({
                "codecs": [],
                "headerExtensions": []
            })),
            download_transport: sample_transport_bootstrap("stc-webrtc-rs", candidate.clone()),
            upload_transport: sample_transport_bootstrap("cts-webrtc-rs", candidate),
            publish_options_by_media_kind: PublishOptionsByMediaKind {
                audio: PublishOptions(json!({ "stopTracks": false })),
                video: PublishOptions(json!({ "stopTracks": false })),
            },
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
            dtls_parameters: DtlsParameters(json!({
                "role": "auto",
                "fingerprints": [{
                    "algorithm": "sha-256",
                    "value": "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
                }]
            })),
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
        let result = validate_dtls_parameters(&DtlsParameters(json!({
            "role": "client",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
            }]
        })));
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn validate_dtls_parameters_maps_invalid_payload_to_invalid_input() {
        let result = validate_dtls_parameters(&DtlsParameters(json!({
            "role": "client",
            "fingerprints": []
        })));
        assert_eq!(result, Err(TransportAdapterError::InvalidInput));
    }

    #[test]
    fn validate_dtls_parameters_maps_unsupported_payload_to_unsupported_feature() {
        let result = validate_dtls_parameters(&DtlsParameters(json!({
            "role": "client",
            "fingerprints": [{
                "algorithm": "sha-1",
                "value": "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD"
            }]
        })));
        assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
    }

    #[test]
    fn validate_bootstrap_payload_accepts_supported_candidate_shape() {
        let payload = sample_bootstrap_payload(IceCandidate(json!({
            "foundation": "foundation",
            "priority": 2_113_937_151_u64,
            "ip": "203.0.113.10",
            "protocol": "udp",
            "port": 40_000_u64,
            "type": "host"
        })));
        let result = validate_bootstrap_payload(&payload);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn validate_bootstrap_payload_rejects_unsupported_candidate_shape() {
        let payload = sample_bootstrap_payload(IceCandidate(json!({
            "foundation": "foundation",
            "priority": 2_113_937_151_u64,
            "ip": "203.0.113.10",
            "protocol": "tcp",
            "port": 9_u64,
            "type": "host"
        })));
        let result = validate_bootstrap_payload(&payload);
        assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
    }

    #[test]
    fn webrtc_rs_transport_connect_rejects_invalid_dtls_before_fallback() {
        let adapter = WebRtcRsTransportAdapter::default();
        let result = adapter.connect_transport(
            &SessionId::Integer(7),
            TransportConnectDirection::Upload,
            &DtlsParameters(json!({
                "role": "client",
                "fingerprints": []
            })),
        );
        assert_eq!(result, Err(TransportAdapterError::InvalidInput));
    }
}
