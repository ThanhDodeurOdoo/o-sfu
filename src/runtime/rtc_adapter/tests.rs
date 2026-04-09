use std::{
    slice,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use o_sfu_router::{
    RtpCapabilities as RouterRtpCapabilities, RtpEncoding as RouterRtpEncoding,
    RtpParameters as RouterRtpParameters,
};
use serde_json::json;
use str0m::media::{MediaKind as Str0mMediaKind, Mid};
use tokio::time::sleep;

use super::{RtcTransportAdapter, packet_loop::take_write_payload, validation};
use crate::{
    runtime::transport_adapter::{TransportAdapterError, TransportConnectDirection},
    signaling::{
        current_protocol::CurrentTransportBootstrapPayload,
        shared::{SessionId, StreamType},
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
const FIREFOX_OFFER_AUDIO_ONLY: &str = include_str!("testdata/firefox_offer_audio_only.sdp");
const SAFARI_DATA_CHANNEL_OFFER: &str = include_str!("testdata/safari_datachannel_offer.sdp");

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

fn sample_router_rtp_parameters(mid: &str, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc)],
    )
    .with_mid(mid.to_owned())
}

#[test]
fn take_write_payload_clones_for_non_final_destination() {
    let mut data = vec![1, 2, 3, 4];
    let payload = take_write_payload(&mut data, false);
    assert_eq!(payload, vec![1, 2, 3, 4]);
    assert_eq!(data, vec![1, 2, 3, 4]);
}

#[test]
fn take_write_payload_moves_for_final_destination() {
    let mut data = vec![5, 6, 7, 8];
    let payload = take_write_payload(&mut data, true);
    assert_eq!(payload, vec![5, 6, 7, 8]);
    assert!(data.is_empty());
}

#[test]
fn validate_dtls_parameters_accepts_client_sha256_payload() {
    let result = validation::validate_dtls_parameters(&sample_sha256_dtls_parameters("client"));
    assert_eq!(result, Ok(()));
}

#[test]
fn validate_sdp_offer_accepts_firefox_offer_fixture() {
    let result = validation::validate_sdp_offer(FIREFOX_OFFER_AUDIO_ONLY);
    assert_eq!(result, Ok(()));
}

#[test]
fn validate_sdp_offer_maps_safari_datachannel_fixture_to_unsupported_feature() {
    let result = validation::validate_sdp_offer(SAFARI_DATA_CHANNEL_OFFER);
    assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
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
            None,
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
            None,
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
            None,
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

#[allow(
    clippy::significant_drop_tightening,
    reason = "the test intentionally inspects the guarded rtc state in one contiguous scope"
)]
#[tokio::test]
async fn rtc_publish_media_uses_signaled_mid_and_ssrc() {
    let adapter = RtcTransportAdapter::default();
    let session_id = SessionId::Integer(18);
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 42_424);
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());

    let transport_media_id = adapter
        .add_recv_media(
            &session_id,
            StreamType::Audio,
            Str0mMediaKind::Audio,
            &rtp_parameters,
        )
        .await;
    assert!(transport_media_id.is_ok());
    let Some(transport_media_id) = transport_media_id.ok() else {
        return;
    };

    let expected_mid: Mid = "aud-up".into();
    {
        assert!(!adapter.bootstrap_state.is_poisoned());
        let Ok(mut bootstrap_state) = adapter.bootstrap_state.lock() else {
            return;
        };
        assert_eq!(
            bootstrap_state.resolve_mid(transport_media_id),
            Some(expected_mid)
        );
        let session_state = bootstrap_state.sessions.get_mut(&session_id);
        assert!(session_state.is_some());
        let Some(session_state) = session_state else {
            return;
        };
        assert!(session_state.rtc.media(expected_mid).is_some());
        let mut direct_api = session_state.rtc.direct_api();
        let stream_rx = direct_api.stream_rx_by_mid(expected_mid, None);
        assert!(stream_rx.is_some());
        let Some(stream_rx) = stream_rx else {
            return;
        };
        assert_eq!(*stream_rx.ssrc(), 42_424);
    }
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "the test intentionally inspects the guarded rtc state in one contiguous scope"
)]
#[tokio::test]
async fn rtc_consume_media_uses_negotiated_mid_and_ssrc() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_id = SessionId::Integer(19);
    let consumer_session_id = SessionId::Integer(20);
    let producer_rtp_parameters = sample_router_rtp_parameters("aud-up", 51_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("aud-down", 61_000);

    assert!(
        adapter
            .transport_bootstrap_payload(&producer_session_id, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert!(
        adapter
            .transport_bootstrap_payload(&consumer_session_id, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_id,
            StreamType::Audio,
            Str0mMediaKind::Audio,
            &producer_rtp_parameters,
        )
        .await;
    assert!(source_media_id.is_ok());
    let Some(source_media_id) = source_media_id.ok() else {
        return;
    };

    let result = adapter
        .add_send_media(
            &consumer_session_id,
            Str0mMediaKind::Audio,
            &producer_session_id,
            source_media_id,
            &consumer_rtp_parameters,
        )
        .await;
    assert!(result.is_ok());

    let expected_source_mid: Mid = "aud-up".into();
    let expected_dest_mid: Mid = "aud-down".into();
    {
        assert!(!adapter.bootstrap_state.is_poisoned());
        let Ok(mut bootstrap_state) = adapter.bootstrap_state.lock() else {
            return;
        };
        let destinations = bootstrap_state
            .media_route_index
            .get(&(producer_session_id.clone(), expected_source_mid));
        assert!(destinations.is_some());
        let Some(destinations) = destinations else {
            return;
        };
        assert!(destinations.source_active);
        assert!(destinations.destinations.iter().any(|dest| {
            dest.dest_session == consumer_session_id && dest.dest_mid == expected_dest_mid
        }));
        let session_state = bootstrap_state.sessions.get_mut(&consumer_session_id);
        assert!(session_state.is_some());
        let Some(session_state) = session_state else {
            return;
        };
        let mut direct_api = session_state.rtc.direct_api();
        let stream_tx = direct_api.stream_tx_by_mid(expected_dest_mid, None);
        assert!(stream_tx.is_some());
        let Some(stream_tx) = stream_tx else {
            return;
        };
        assert_eq!(*stream_tx.ssrc(), 61_000);
    }
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "the test intentionally inspects the guarded rtc route state in one contiguous scope"
)]
#[tokio::test]
async fn rtc_route_activity_updates_producer_and_consumer_flags() {
    let adapter = RtcTransportAdapter::default();
    let producer_session_id = SessionId::Integer(23);
    let consumer_session_id = SessionId::Integer(24);
    let producer_rtp_parameters = sample_router_rtp_parameters("vid-up", 91_000);
    let consumer_rtp_parameters = sample_router_rtp_parameters("vid-down", 92_000);

    assert!(
        adapter
            .transport_bootstrap_payload(&producer_session_id, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert!(
        adapter
            .transport_bootstrap_payload(&consumer_session_id, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let source_media_id = adapter
        .add_recv_media(
            &producer_session_id,
            StreamType::Camera,
            Str0mMediaKind::Video,
            &producer_rtp_parameters,
        )
        .await;
    assert!(source_media_id.is_ok());
    let Some(source_media_id) = source_media_id.ok() else {
        return;
    };

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_id,
            Str0mMediaKind::Video,
            &producer_session_id,
            source_media_id,
            &consumer_rtp_parameters,
        )
        .await;
    assert!(consumer_media_id.is_ok());
    let Some(consumer_media_id) = consumer_media_id.ok() else {
        return;
    };

    assert!(
        adapter
            .set_producer_active(&producer_session_id, source_media_id, false)
            .await
            .is_ok()
    );
    assert!(
        adapter
            .set_consumer_active(
                &consumer_session_id,
                consumer_media_id,
                &producer_session_id,
                source_media_id,
                false,
            )
            .await
            .is_ok()
    );

    {
        assert!(!adapter.bootstrap_state.is_poisoned());
        let Ok(bootstrap_state) = adapter.bootstrap_state.lock() else {
            return;
        };
        let route_entry = bootstrap_state
            .media_route_index
            .get(&(producer_session_id.clone(), Mid::from("vid-up")));
        assert!(route_entry.is_some());
        let Some(route_entry) = route_entry else {
            return;
        };
        assert!(!route_entry.source_active);
        assert!(route_entry.destinations.iter().any(|destination| {
            destination.dest_session == consumer_session_id
                && destination.dest_mid == Mid::from("vid-down")
                && !destination.active
        }));
    }
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "the test intentionally inspects and seeds the guarded rtc state in one contiguous scope"
)]
#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_counts_recent_media_bytes() {
    let adapter = RtcTransportAdapter::default();
    let session_id = SessionId::Integer(21);
    let rtp_parameters = sample_router_rtp_parameters("cam-up", 77_777);

    assert!(
        adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert!(
        adapter
            .add_recv_media(
                &session_id,
                StreamType::Camera,
                Str0mMediaKind::Video,
                &rtp_parameters,
            )
            .await
            .is_ok()
    );

    {
        assert!(!adapter.bootstrap_state.is_poisoned());
        let Ok(mut bootstrap_state) = adapter.bootstrap_state.lock() else {
            return;
        };
        bootstrap_state.record_incoming_media(
            &session_id,
            Mid::from("cam-up"),
            120,
            Instant::now(),
        );
    }

    let snapshot = adapter.incoming_bitrate_snapshot(slice::from_ref(&session_id));
    assert_eq!(snapshot.total, 960);
    assert_eq!(snapshot.audio, 0);
    assert_eq!(snapshot.camera, 960);
    assert_eq!(snapshot.screen, 0);
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "the test intentionally inspects and seeds the guarded rtc state in one contiguous scope"
)]
#[tokio::test]
async fn rtc_incoming_bitrate_snapshot_expires_after_one_second() {
    let adapter = RtcTransportAdapter::default();
    let session_id = SessionId::Integer(22);
    let rtp_parameters = sample_router_rtp_parameters("aud-up", 88_888);

    assert!(
        adapter
            .transport_bootstrap_payload(&session_id, &empty_router_capabilities())
            .await
            .is_ok()
    );
    assert!(
        adapter
            .add_recv_media(
                &session_id,
                StreamType::Audio,
                Str0mMediaKind::Audio,
                &rtp_parameters,
            )
            .await
            .is_ok()
    );

    let now = Instant::now();
    let snapshot = {
        assert!(!adapter.bootstrap_state.is_poisoned());
        let Ok(mut bootstrap_state) = adapter.bootstrap_state.lock() else {
            return;
        };
        bootstrap_state.record_incoming_media(&session_id, Mid::from("aud-up"), 64, now);
        bootstrap_state.incoming_bitrate_snapshot_at(
            slice::from_ref(&session_id),
            now + Duration::from_secs(2),
        )
    };
    assert_eq!(snapshot.total, 0);
    assert_eq!(snapshot.audio, 0);
    assert_eq!(snapshot.camera, 0);
    assert_eq!(snapshot.screen, 0);
}
