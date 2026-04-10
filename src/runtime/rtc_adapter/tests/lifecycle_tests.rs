use super::fixtures::*;

#[tokio::test]
async fn rtc_transport_connect_rejects_invalid_dtls_before_rtc_connect() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 7, SessionId::Integer(7));
    let result = adapter
        .connect_transport(
            &session_key,
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
    let session_key = transport_key(1, 8, SessionId::Integer(8));
    let result = adapter
        .connect_transport(
            &session_key,
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
    let session_key = transport_key(1, 9, SessionId::Integer(9));
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    let connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            None,
            Some(VALID_SDP_OFFER),
        )
        .await;
    assert_eq!(connect_result, Ok(()));
}

#[tokio::test]
async fn rtc_transport_connect_accepts_remote_ice_credentials() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 90, SessionId::Integer(90));
    assert!(
        adapter
            .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let result = adapter
        .connect_transport(
            &session_key,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            Some(&sample_ice_parameters("client-ufrag", "client-password")),
            None,
        )
        .await;
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn rtc_transport_connect_rejects_invalid_remote_ice_credentials() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 91, SessionId::Integer(91));
    assert!(
        adapter
            .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let result = adapter
        .connect_transport(
            &session_key,
            TransportConnectDirection::Upload,
            &sample_sha256_dtls_parameters("client"),
            Some(&IceParameters(json!({
                "usernameFragment": "client-ufrag"
            }))),
            None,
        )
        .await;
    assert_eq!(result, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn rtc_transport_bootstrap_uses_real_ice_and_dtls_parameters() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 13, SessionId::Integer(13));
    let payload = adapter
        .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
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
    let session_key = transport_key(1, 14, SessionId::Integer(14));
    let bootstrap_result = adapter
        .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
        .await;
    assert!(bootstrap_result.is_ok());
    let close_result = adapter.close_session(&session_key).await;
    assert_eq!(close_result, Ok(()));
    let connect_result = adapter
        .connect_transport(
            &session_key,
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
async fn rtc_transport_close_session_cleans_remote_addr_demux_state() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 140, SessionId::Integer(140));
    assert!(
        adapter
            .transport_bootstrap_payload(&session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );

    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_000);
    adapter
        .debug_remember_remote_addr(source_addr, &session_key)
        .await;
    assert_eq!(
        adapter.debug_remote_addr_owner(source_addr).await,
        Some(session_key.clone())
    );

    assert_eq!(adapter.close_session(&session_key).await, Ok(()));

    assert_eq!(adapter.debug_remote_addr_owner(source_addr).await, None);
    assert!(!adapter.debug_has_any_remote_addr_session().await);
}

#[tokio::test]
async fn rtc_transport_close_last_session_resets_packet_loop_worker() {
    let adapter = RtcTransportAdapter::default();
    let first_session_key = transport_key(1, 141, SessionId::Integer(141));
    assert!(
        adapter
            .transport_bootstrap_payload(&first_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));

    assert_eq!(adapter.close_session(&first_session_key).await, Ok(()));
    assert!(!adapter.packet_loop_started.load(Ordering::Acquire));
    assert!(matches!(adapter.worker_handle(), Ok(None)));

    let second_session_key = transport_key(1, 142, SessionId::Integer(142));
    assert!(
        adapter
            .transport_bootstrap_payload(&second_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    sleep(Duration::from_millis(5)).await;
    assert!(adapter.packet_loop_started.load(Ordering::Acquire));
}

#[tokio::test]
async fn rtc_transport_distinguishes_same_session_id_across_channels() {
    let adapter = RtcTransportAdapter::default();
    let first_session_key = transport_key_on_worker(1, 0, 30, SessionId::Integer(30));
    let second_session_key = transport_key_on_worker(2, 1, 30, SessionId::Integer(30));

    let first_payload = adapter
        .transport_bootstrap_payload(&first_session_key, &empty_router_capabilities())
        .await;
    let second_payload = adapter
        .transport_bootstrap_payload(&second_session_key, &empty_router_capabilities())
        .await;
    assert!(first_payload.is_ok());
    assert!(second_payload.is_ok());
    let Some(first_payload) = first_payload.ok() else {
        return;
    };
    let Some(second_payload) = second_payload.ok() else {
        return;
    };
    assert_ne!(
        first_payload.upload_transport.id,
        second_payload.upload_transport.id
    );
    assert_ne!(
        first_payload.download_transport.id,
        second_payload.download_transport.id
    );

    assert_eq!(adapter.close_session(&first_session_key).await, Ok(()));
    assert_eq!(
        adapter
            .connect_transport(
                &second_session_key,
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
                None,
                None,
            )
            .await,
        Ok(())
    );
}
