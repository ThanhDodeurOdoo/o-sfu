use super::fixtures::*;

#[tokio::test]
async fn websocket_sends_router_capabilities_in_transport_bootstrap_after_startup() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(10));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, startup)) = authenticated else {
        return;
    };
    assert!(startup.available_features.rtc);

    let batch = read_bus_batch(&mut websocket).await;
    assert!(batch.is_some(), "transport bootstrap batch should exist");
    let Some(batch) = batch else {
        return;
    };
    assert_eq!(batch.len(), 1);
    let Some(envelope) = batch.first() else {
        return;
    };
    assert_eq!(
        envelope
            .need_response
            .as_ref()
            .map(CurrentBusRequestId::as_str),
        Some("s_0_0")
    );
    assert_eq!(envelope.response_to, None);
    let request = serde_json::from_value::<CurrentServerRequest>(envelope.message.clone());
    assert!(
        request.is_ok(),
        "transport bootstrap should deserialize: {request:?}"
    );
    let Some(request) = request.ok() else {
        return;
    };
    let CurrentServerRequest::BootstrapTransports(payload) = request else {
        return;
    };
    assert_eq!(payload.download_transport.id, "stc-stub");
    assert_eq!(payload.upload_transport.id, "cts-stub");
    let codecs = payload
        .router_capabilities
        .0
        .get("codecs")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        !codecs.is_empty(),
        "router capabilities should contain codecs"
    );
    assert!(
        codecs
            .iter()
            .any(|codec| codec.get("mimeType") == Some(&serde_json::json!("audio/opus"))),
        "router capabilities should include opus"
    );
    assert!(
        codecs
            .iter()
            .any(|codec| codec.get("mimeType") == Some(&serde_json::json!("video/VP8"))),
        "router capabilities should include VP8"
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "integration test keeps the negotiated-consumer flow explicit in one place"
)]
#[tokio::test]
async fn websocket_uses_stored_client_capabilities_for_consumer_negotiation() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, ChannelConfig::default()).await;
    let publisher_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(110));
    let subscriber_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(111));
    assert!(publisher_token.is_some());
    assert!(subscriber_token.is_some());
    let Some(publisher_token) = publisher_token else {
        return;
    };
    let Some(subscriber_token) = subscriber_token else {
        return;
    };

    let publisher_auth = authenticate_and_read_startup(&server, &publisher_token).await;
    let subscriber_auth = authenticate_and_read_startup(&server, &subscriber_token).await;
    assert!(publisher_auth.is_some());
    assert!(subscriber_auth.is_some());
    let Some((mut publisher_socket, _publisher_startup)) = publisher_auth else {
        return;
    };
    let Some((mut subscriber_socket, _subscriber_startup)) = subscriber_auth else {
        return;
    };

    assert_eq!(
        acknowledge_transport_bootstrap(&mut publisher_socket).await,
        Some(())
    );
    assert_eq!(
        acknowledge_transport_bootstrap_with_capabilities(
            &mut subscriber_socket,
            test_client_rtp_capabilities_without_video_rtx(),
        )
        .await,
        Some(())
    );

    let publisher_upload_connect = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 20),
    )
    .await;
    assert!(publisher_upload_connect.is_some());

    let subscriber_download_connect = send_bus_request_and_read_response(
        &mut subscriber_socket,
        CurrentClientRequest::ConnectDownloadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 21),
    )
    .await;
    assert!(subscriber_download_connect.is_some());

    let publish_response = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::PublishTrack(CurrentPublishTrackPayload {
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            rtp_parameters: RtpParameters(serde_json::json!({
                "codecs": [
                    {
                        "mimeType": "video/VP8",
                        "payloadType": 96,
                        "clockRate": 90000,
                        "parameters": {},
                        "rtcpFeedback": [
                            { "type": "nack" },
                            { "type": "nack", "parameter": "pli" },
                            { "type": "ccm", "parameter": "fir" },
                            { "type": "goog-remb" },
                            { "type": "transport-cc" }
                        ]
                    },
                    {
                        "mimeType": "video/rtx",
                        "payloadType": 97,
                        "clockRate": 90000,
                        "parameters": { "apt": "96" },
                        "rtcpFeedback": []
                    }
                ],
                "headerExtensions": [
                    { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
                    { "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", "id": 4, "encrypt": false },
                    { "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01", "id": 5, "encrypt": false }
                ],
                "encodings": [{ "ssrc": 22222 }]
            })),
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 22),
    )
    .await;
    assert!(publish_response.is_some());

    let subscriber_batch = read_bus_batch(&mut subscriber_socket).await;
    assert!(subscriber_batch.is_some());
    let Some(subscriber_batch) = subscriber_batch else {
        return;
    };
    let Some(subscriber_envelope) = subscriber_batch.first() else {
        return;
    };
    let server_request =
        serde_json::from_value::<CurrentServerRequest>(subscriber_envelope.message.clone());
    assert!(server_request.is_ok());
    let Some(server_request) = server_request.ok() else {
        return;
    };
    let CurrentServerRequest::BootstrapRemoteTrack(track) = server_request else {
        panic!("expected INIT_CONSUMER server request");
    };
    let codecs = track
        .rtp_parameters
        .0
        .get("codecs")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        codecs
            .iter()
            .any(|codec| codec.get("mimeType") == Some(&serde_json::json!("video/VP8"))),
        "negotiated consumer parameters should retain VP8"
    );
    assert!(
        codecs
            .iter()
            .all(|codec| codec.get("mimeType") != Some(&serde_json::json!("video/rtx"))),
        "negotiated consumer parameters should drop unsupported RTX"
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "integration test keeps the deferred bootstrap ordering explicit in one place"
)]
#[tokio::test]
async fn websocket_bootstraps_late_join_when_capabilities_arrive_after_download_connect() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, ChannelConfig::default()).await;
    let publisher_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(210));
    let subscriber_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(211));
    assert!(publisher_token.is_some());
    assert!(subscriber_token.is_some());
    let Some(publisher_token) = publisher_token else {
        return;
    };
    let Some(subscriber_token) = subscriber_token else {
        return;
    };

    let publisher_auth = authenticate_and_read_startup(&server, &publisher_token).await;
    let subscriber_auth = authenticate_and_read_startup(&server, &subscriber_token).await;
    assert!(publisher_auth.is_some());
    assert!(subscriber_auth.is_some());
    let Some((mut publisher_socket, _publisher_startup)) = publisher_auth else {
        return;
    };
    let Some((mut subscriber_socket, _subscriber_startup)) = subscriber_auth else {
        return;
    };

    assert_eq!(
        acknowledge_transport_bootstrap(&mut publisher_socket).await,
        Some(())
    );

    let subscriber_bootstrap = read_bus_batch(&mut subscriber_socket).await;
    assert!(subscriber_bootstrap.is_some());
    let Some(subscriber_bootstrap) = subscriber_bootstrap else {
        return;
    };
    let Some(subscriber_bootstrap_envelope) = subscriber_bootstrap.first() else {
        return;
    };
    let bootstrap_request_id = subscriber_bootstrap_envelope.need_response.clone();
    assert!(bootstrap_request_id.is_some());
    let Some(bootstrap_request_id) = bootstrap_request_id else {
        return;
    };
    let bootstrap_request = serde_json::from_value::<CurrentServerRequest>(
        subscriber_bootstrap_envelope.message.clone(),
    );
    assert!(bootstrap_request.is_ok());
    let Some(CurrentServerRequest::BootstrapTransports(_)) = bootstrap_request.ok() else {
        panic!("expected INIT_TRANSPORTS request");
    };

    let publisher_upload_connect = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 40),
    )
    .await;
    assert!(publisher_upload_connect.is_some());

    let subscriber_download_connect = send_bus_request_and_read_response(
        &mut subscriber_socket,
        CurrentClientRequest::ConnectDownloadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 41),
    )
    .await;
    assert!(subscriber_download_connect.is_some());

    let publish_response = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::PublishTrack(CurrentPublishTrackPayload {
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            rtp_parameters: RtpParameters(serde_json::json!({
                "codecs": [
                    {
                        "mimeType": "video/VP8",
                        "payloadType": 96,
                        "clockRate": 90000,
                        "parameters": {},
                        "rtcpFeedback": [
                            { "type": "nack" },
                            { "type": "nack", "parameter": "pli" },
                            { "type": "ccm", "parameter": "fir" },
                            { "type": "goog-remb" },
                            { "type": "transport-cc" }
                        ]
                    },
                    {
                        "mimeType": "video/rtx",
                        "payloadType": 97,
                        "clockRate": 90000,
                        "parameters": { "apt": "96" },
                        "rtcpFeedback": []
                    }
                ],
                "headerExtensions": [
                    { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
                    { "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", "id": 4, "encrypt": false },
                    { "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01", "id": 5, "encrypt": false }
                ],
                "encodings": [{ "ssrc": 22222 }]
            })),
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 42),
    )
    .await;
    assert!(publish_response.is_some());

    let delayed_bootstrap_ack = respond_to_server_request(
        &mut subscriber_socket,
        bootstrap_request_id,
        test_client_rtp_capabilities(),
    )
    .await;
    assert_eq!(delayed_bootstrap_ack, Some(()));

    let subscriber_batch = read_bus_batch(&mut subscriber_socket).await;
    assert!(subscriber_batch.is_some());
    let Some(subscriber_batch) = subscriber_batch else {
        return;
    };
    let Some(subscriber_envelope) = subscriber_batch.first() else {
        return;
    };
    let server_request =
        serde_json::from_value::<CurrentServerRequest>(subscriber_envelope.message.clone());
    assert!(server_request.is_ok());
    let Some(CurrentServerRequest::BootstrapRemoteTrack(track)) = server_request.ok() else {
        panic!("expected INIT_CONSUMER after delayed capabilities");
    };
    assert_eq!(track.session_id, SessionId::Integer(210));
    assert_eq!(track.stream_type, StreamType::Camera);
}
