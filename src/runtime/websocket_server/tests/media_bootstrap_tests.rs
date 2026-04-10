use super::fixtures::*;

#[allow(
    clippy::too_many_lines,
    reason = "integration test keeps full publish flow in one place for readability"
)]
#[tokio::test]
async fn websocket_publish_sends_init_consumer_to_download_ready_peers() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let publisher_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(21));
    let subscriber_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(22));
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

    assert!(
        acknowledge_transport_bootstrap(&mut publisher_socket)
            .await
            .is_some()
    );
    assert!(
        acknowledge_transport_bootstrap(&mut subscriber_socket)
            .await
            .is_some()
    );

    let publisher_upload_connect = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 10),
    )
    .await;
    assert!(publisher_upload_connect.is_some());
    assert_eq!(
        publisher_upload_connect.map(|envelope| envelope.message),
        Some(serde_json::json!({}))
    );

    let subscriber_download_connect = send_bus_request_and_read_response(
        &mut subscriber_socket,
        CurrentClientRequest::ConnectDownloadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 11),
    )
    .await;
    assert!(subscriber_download_connect.is_some());
    assert_eq!(
        subscriber_download_connect.map(|envelope| envelope.message),
        Some(serde_json::json!({}))
    );

    let publish_response = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::PublishTrack(CurrentPublishTrackPayload {
            stream_type: StreamType::Audio,
            media_kind: MediaKind::Audio,
            rtp_parameters: RtpParameters(serde_json::json!({
                "codecs": [{
                    "mimeType": "audio/opus",
                    "payloadType": 111,
                    "clockRate": 48000,
                    "channels": 2,
                    "parameters": { "useinbandfec": "1" },
                    "rtcpFeedback": [{ "type": "transport-cc" }]
                }],
                "headerExtensions": [
                    { "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level", "id": 10, "encrypt": false }
                ],
                "encodings": [{ "ssrc": 11111 }]
            })),
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 12),
    )
    .await;
    assert!(publish_response.is_some());
    let Some(publish_response) = publish_response else {
        return;
    };
    assert_eq!(
        publish_response.message,
        serde_json::json!({"id": "producer-1"})
    );

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
    if let CurrentServerRequest::BootstrapRemoteTrack(track) = server_request {
        assert_eq!(track.source_id, "producer-1");
        assert_eq!(track.session_id, SessionId::Integer(21));
        assert_eq!(track.media_kind, MediaKind::Audio);
        assert_eq!(track.stream_type, StreamType::Audio);
        assert!(track.active);
    } else {
        panic!("expected INIT_CONSUMER server request");
    }
}

/// Late-join scenario: publisher publishes first, then subscriber connects
/// download transport and receives `INIT_CONSUMER` for the existing producer.
#[allow(
    clippy::too_many_lines,
    reason = "integration test keeps full late-join flow in one place for readability"
)]
#[tokio::test]
async fn websocket_late_join_sends_init_consumer_for_existing_producers() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let publisher_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(31));
    let subscriber_token =
        signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(32));
    assert!(publisher_token.is_some());
    assert!(subscriber_token.is_some());
    let Some(publisher_token) = publisher_token else {
        return;
    };
    let Some(subscriber_token) = subscriber_token else {
        return;
    };

    // Publisher: authenticate, bootstrap, connect upload, publish.
    let publisher_auth = authenticate_and_read_startup(&server, &publisher_token).await;
    assert!(publisher_auth.is_some());
    let Some((mut publisher_socket, _publisher_startup)) = publisher_auth else {
        return;
    };
    assert!(
        acknowledge_transport_bootstrap(&mut publisher_socket)
            .await
            .is_some()
    );
    let publisher_upload_connect = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 10),
    )
    .await;
    assert!(publisher_upload_connect.is_some());
    let publish_response = send_bus_request_and_read_response(
        &mut publisher_socket,
        CurrentClientRequest::PublishTrack(CurrentPublishTrackPayload {
            stream_type: StreamType::Audio,
            media_kind: MediaKind::Audio,
            rtp_parameters: RtpParameters(serde_json::json!({
                "codecs": [{
                    "mimeType": "audio/opus",
                    "payloadType": 111,
                    "clockRate": 48000,
                    "channels": 2,
                    "parameters": { "useinbandfec": "1" },
                    "rtcpFeedback": [{ "type": "transport-cc" }]
                }],
                "headerExtensions": [
                    { "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level", "id": 10, "encrypt": false }
                ],
                "encodings": [{ "ssrc": 11111 }]
            })),
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 12),
    )
    .await;
    assert!(publish_response.is_some());
    let Some(publish_response) = publish_response else {
        return;
    };
    assert_eq!(
        publish_response.message,
        serde_json::json!({"id": "producer-1"})
    );

    // Subscriber: authenticate, bootstrap, then connect download AFTER publish.
    let subscriber_auth = authenticate_and_read_startup(&server, &subscriber_token).await;
    assert!(subscriber_auth.is_some());
    let Some((mut subscriber_socket, _subscriber_startup)) = subscriber_auth else {
        return;
    };

    // Subscriber should see a SESSION_LEAVE for the publisher that was already there,
    // or possibly just the bootstrap. Read the departure message if present.
    // Actually, no departure — publisher is still there. Subscriber just joined.
    assert!(
        acknowledge_transport_bootstrap(&mut subscriber_socket)
            .await
            .is_some()
    );

    // Connect subscriber download transport — this should trigger late-join bootstrap.
    let subscriber_download_connect = send_bus_request_and_read_response(
        &mut subscriber_socket,
        CurrentClientRequest::ConnectDownloadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 11),
    )
    .await;
    assert!(subscriber_download_connect.is_some());
    assert_eq!(
        subscriber_download_connect.map(|envelope| envelope.message),
        Some(serde_json::json!({}))
    );

    // Subscriber should now receive INIT_CONSUMER for the publisher's existing track.
    let subscriber_batch = read_bus_batch(&mut subscriber_socket).await;
    assert!(
        subscriber_batch.is_some(),
        "subscriber should receive late-join INIT_CONSUMER"
    );
    let Some(subscriber_batch) = subscriber_batch else {
        return;
    };
    let Some(subscriber_envelope) = subscriber_batch.first() else {
        panic!("expected at least one envelope in late-join batch");
    };
    let server_request =
        serde_json::from_value::<CurrentServerRequest>(subscriber_envelope.message.clone());
    assert!(server_request.is_ok());
    let Some(server_request) = server_request.ok() else {
        return;
    };
    if let CurrentServerRequest::BootstrapRemoteTrack(track) = server_request {
        assert_eq!(track.source_id, "producer-1");
        assert_eq!(track.session_id, SessionId::Integer(31));
        assert_eq!(track.media_kind, MediaKind::Audio);
        assert_eq!(track.stream_type, StreamType::Audio);
        assert!(track.active);
    } else {
        panic!("expected INIT_CONSUMER server request for late-join");
    }
}
