use super::fixtures::*;

#[tokio::test]
async fn websocket_emits_stub_webrtc_directional_connect_events() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let session_id = SessionId::Integer(211);
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    let acknowledged = acknowledge_transport_bootstrap(&mut websocket).await;
    assert!(acknowledged.is_some());

    let upload_response = send_bus_request_and_read_response(
        &mut websocket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 9, 1),
    )
    .await;
    assert!(upload_response.is_some());
    let download_response = send_bus_request_and_read_response(
        &mut websocket,
        CurrentClientRequest::ConnectDownloadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 9, 2),
    )
    .await;
    assert!(download_response.is_some());

    let events = wait_for_stub_webrtc_events(&adapter, 5).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    let expected = vec![
        StubWebRtcEvent::BootstrapRequested,
        StubWebRtcEvent::TransportConnectRequested {
            session_id: session_id.clone(),
            direction: TransportConnectDirection::Upload,
            dtls_parameters: sample_client_dtls_parameters(),
        },
        StubWebRtcEvent::TransportConnected {
            session_id: session_id.clone(),
            direction: TransportConnectDirection::Upload,
        },
        StubWebRtcEvent::TransportConnectRequested {
            session_id: session_id.clone(),
            direction: TransportConnectDirection::Download,
            dtls_parameters: sample_client_dtls_parameters(),
        },
        StubWebRtcEvent::TransportConnected {
            session_id,
            direction: TransportConnectDirection::Download,
        },
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn websocket_emits_stub_webrtc_rejected_connect_event_for_invalid_dtls() {
    let adapter = Arc::new(StubWebRtcAdapter::default());
    let transport_adapter =
        RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
    let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let session_id = SessionId::Integer(212);
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    let acknowledged = acknowledge_transport_bootstrap(&mut websocket).await;
    assert!(acknowledged.is_some());

    let connect_response = send_bus_request_and_read_response(
        &mut websocket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: invalid_dtls_parameters_for_stub_rejection(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 12, 1),
    )
    .await;
    assert!(connect_response.is_some());

    let events = wait_for_stub_webrtc_events(&adapter, 3).await;
    assert!(events.is_some());
    let Some(events) = events else {
        return;
    };
    let expected = vec![
        StubWebRtcEvent::BootstrapRequested,
        StubWebRtcEvent::TransportConnectRequested {
            session_id: session_id.clone(),
            direction: TransportConnectDirection::Upload,
            dtls_parameters: invalid_dtls_parameters_for_stub_rejection(),
        },
        StubWebRtcEvent::TransportConnectRejected {
            session_id,
            direction: TransportConnectDirection::Upload,
        },
    ];
    assert_eq!(events, expected);
}

#[tokio::test]
async fn websocket_returns_stub_responses_for_client_bus_requests() {
    let server = spawn_test_server(1_000, 100).await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let channel = create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(11));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_startup(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut websocket, _startup)) = authenticated else {
        return;
    };
    let acknowledged = acknowledge_transport_bootstrap(&mut websocket).await;
    assert!(
        acknowledged.is_some(),
        "transport bootstrap should round-trip"
    );

    let connect_response = send_bus_request_and_read_response(
        &mut websocket,
        CurrentClientRequest::ConnectUploadTransport(CurrentTransportConnectPayload {
            dtls_parameters: sample_client_dtls_parameters(),
            ice_parameters: None,
            sdp_offer: None,
        }),
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 0),
    )
    .await;
    assert!(connect_response.is_some());
    let Some(connect_envelope) = connect_response else {
        return;
    };
    assert_eq!(connect_envelope.message, serde_json::json!({}));
    assert_eq!(
        connect_envelope
            .response_to
            .as_ref()
            .map(CurrentBusRequestId::as_str),
        Some("c_0_0")
    );

    let publish_response = send_bus_request_and_read_response(
        &mut websocket,
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
        CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 1),
    )
    .await;
    assert!(publish_response.is_some());
    let Some(publish_envelope) = publish_response else {
        return;
    };
    assert_eq!(
        publish_envelope.message,
        serde_json::json!({
            "id": "producer-1"
        })
    );
    assert_eq!(
        publish_envelope
            .response_to
            .as_ref()
            .map(CurrentBusRequestId::as_str),
        Some("c_0_1")
    );
}
