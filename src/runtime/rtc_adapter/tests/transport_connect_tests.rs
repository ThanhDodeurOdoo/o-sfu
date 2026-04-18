use super::fixtures::*;

#[tokio::test]
async fn rtc_transport_connect_rejects_duplicate_direction_connect() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 10, SessionId::Integer(10));
    let bootstrap_result = bootstrap_transport(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());
    let first_connect = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
            ),
        )
        .await;
    assert_eq!(first_connect, Ok(()));
    let second_connect = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
            ),
        )
        .await;
    assert_eq!(second_connect, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn rtc_transport_connect_rejects_invalid_sdp_before_rtc_connect() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 11, SessionId::Integer(11));
    let bootstrap_result = bootstrap_transport(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());
    let connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
            )
            .with_sdp_offer("v=0\r\ns=-\r\nt=0 0\r\n"),
        )
        .await;
    assert_eq!(connect_result, Err(TransportAdapterError::InvalidInput));
}

#[tokio::test]
async fn rtc_transport_connect_rejects_unsupported_sdp_before_rtc_connect() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 12, SessionId::Integer(12));
    let bootstrap_result = bootstrap_transport(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());
    let connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
            )
            .with_sdp_offer("m=audio 9 RTP/SAVPF 111\r\n"),
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
    let session_key = transport_key(1, 16, SessionId::Integer(16));
    let bootstrap_result = bootstrap_transport(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());
    let upload_connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
            ),
        )
        .await;
    assert_eq!(upload_connect_result, Ok(()));
    let download_connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Download,
                &sample_sha256_dtls_parameters("client"),
            ),
        )
        .await;
    assert_eq!(download_connect_result, Ok(()));
}

#[tokio::test]
async fn rtc_transport_connect_rejects_mismatched_fingerprint_between_directions() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 17, SessionId::Integer(17));
    let bootstrap_result = bootstrap_transport(&adapter, &session_key).await;
    assert!(bootstrap_result.is_ok());
    let first_connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Upload,
                &sample_sha256_dtls_parameters("client"),
            ),
        )
        .await;
    assert_eq!(first_connect_result, Ok(()));
    let second_connect_result = adapter
        .connect_transport(
            &session_key,
            TransportConnectRequest::new(
                TransportConnectDirection::Download,
                &sample_sha256_dtls_parameters_with_value(
                    "client",
                    "11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00",
                ),
            ),
        )
        .await;
    assert_eq!(
        second_connect_result,
        Err(TransportAdapterError::InvalidInput)
    );
}

#[tokio::test]
async fn rtc_transport_connect_allows_late_remote_ice_credentials_on_second_direction() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 92, SessionId::Integer(92));
    assert!(bootstrap_transport(&adapter, &session_key).await.is_ok());

    assert_eq!(
        adapter
            .connect_transport(
                &session_key,
                TransportConnectRequest::new(
                    TransportConnectDirection::Upload,
                    &sample_sha256_dtls_parameters("client"),
                ),
            )
            .await,
        Ok(())
    );

    assert_eq!(
        adapter
            .connect_transport(
                &session_key,
                TransportConnectRequest::new(
                    TransportConnectDirection::Download,
                    &sample_sha256_dtls_parameters("client"),
                )
                .with_ice_parameters(&sample_ice_parameters("client-ufrag", "client-password",)),
            )
            .await,
        Ok(())
    );
}

#[tokio::test]
async fn rtc_transport_connect_rejects_mismatched_remote_ice_credentials_between_directions() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 93, SessionId::Integer(93));
    assert!(bootstrap_transport(&adapter, &session_key).await.is_ok());

    assert_eq!(
        adapter
            .connect_transport(
                &session_key,
                TransportConnectRequest::new(
                    TransportConnectDirection::Upload,
                    &sample_sha256_dtls_parameters("client"),
                )
                .with_ice_parameters(&sample_ice_parameters("client-ufrag", "client-password",)),
            )
            .await,
        Ok(())
    );

    assert_eq!(
        adapter
            .connect_transport(
                &session_key,
                TransportConnectRequest::new(
                    TransportConnectDirection::Download,
                    &sample_sha256_dtls_parameters("client"),
                )
                .with_ice_parameters(&sample_ice_parameters("other-ufrag", "other-password",)),
            )
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
}
