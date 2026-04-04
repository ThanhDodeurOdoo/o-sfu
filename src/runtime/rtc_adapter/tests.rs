use std::{sync::atomic::Ordering, time::Duration};

use o_sfu_router::RtpCapabilities as RouterRtpCapabilities;
use serde_json::json;
use tokio::time::sleep;

use super::{RtcTransportAdapter, validation};
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
    sample_sha256_dtls_parameters_with_value(
        role,
        "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
    )
}

fn sample_sha256_dtls_parameters_with_value(role: &str, value: &str) -> DtlsParameters {
    DtlsParameters {
        role: role.to_owned(),
        fingerprints: vec![DtlsFingerprint {
            algorithm: String::from("sha-256"),
            value: value.to_owned(),
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
    let result = validation::validate_dtls_parameters(&sample_sha256_dtls_parameters("client"));
    assert_eq!(result, Ok(()));
}

#[test]
fn validate_dtls_parameters_maps_invalid_payload_to_invalid_input() {
    let result = validation::validate_dtls_parameters(&DtlsParameters {
        role: String::from("client"),
        fingerprints: vec![],
    });
    assert_eq!(result, Err(TransportAdapterError::InvalidInput));
}

#[test]
fn validate_dtls_parameters_maps_unsupported_payload_to_unsupported_feature() {
    let result = validation::validate_dtls_parameters(&DtlsParameters {
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
    let result = validation::validate_bootstrap_payload(&payload);
    assert_eq!(result, Ok(()));
}

#[test]
fn validate_bootstrap_payload_rejects_unsupported_candidate_shape() {
    let payload = sample_bootstrap_payload(sample_candidate("tcp", 9));
    let result = validation::validate_bootstrap_payload(&payload);
    assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
}

#[tokio::test]
async fn rtc_transport_connect_rejects_invalid_dtls_before_rtc_connect() {
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
    let (Some(download_candidate), Some(upload_candidate)) = (download_candidate, upload_candidate)
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
async fn rtc_transport_close_session_cleans_bootstrap_state() {
    let adapter = RtcTransportAdapter::default();
    let session_id = SessionId::Integer(14);
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    let close_result = adapter.close_session(&session_id).await;
    assert_eq!(close_result, Ok(()));
    let connect_result = adapter
        .connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
        )
        .await;
    assert_eq!(
        connect_result,
        Err(TransportAdapterError::TransportUnavailable)
    );
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
async fn rtc_transport_connect_rejects_invalid_sdp_before_rtc_connect() {
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
async fn rtc_transport_connect_rejects_unsupported_sdp_before_rtc_connect() {
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

#[tokio::test]
async fn rtc_transport_connect_allows_both_transport_directions_with_one_dtls_context() {
    let adapter = RtcTransportAdapter::default();
    let session_id = SessionId::Integer(16);
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    let upload_connect_result = adapter
        .connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
        )
        .await;
    assert_eq!(upload_connect_result, Ok(()));
    let download_connect_result = adapter
        .connect_transport(
            &session_id,
            TransportConnectDirection::Download,
            &sample_sha256_dtls_parameters("client"),
            None,
        )
        .await;
    assert_eq!(download_connect_result, Ok(()));
}

#[tokio::test]
async fn rtc_transport_connect_rejects_mismatched_fingerprint_between_directions() {
    let adapter = RtcTransportAdapter::default();
    let session_id = SessionId::Integer(17);
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    let first_connect_result = adapter
        .connect_transport(
            &session_id,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
        )
        .await;
    assert_eq!(first_connect_result, Ok(()));
    let second_connect_result = adapter
        .connect_transport(
            &session_id,
            TransportConnectDirection::Download,
            &sample_sha256_dtls_parameters_with_value(
                "client",
                "11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00",
            ),
            None,
        )
        .await;
    assert_eq!(
        second_connect_result,
        Err(TransportAdapterError::InvalidInput)
    );
}

#[tokio::test]
async fn rtc_transport_bootstrap_starts_packet_loop() {
    let adapter = RtcTransportAdapter::default();
    assert!(!adapter.packet_loop_started.load(Ordering::Acquire));
    let session_id = SessionId::Integer(15);
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));
}
