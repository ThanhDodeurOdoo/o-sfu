#[allow(
    clippy::panic,
    reason = "test assertions use panic for clear failure messages"
)]
mod websocket_server_tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        net::{TcpListener, TcpStream},
        task::JoinHandle,
        time::{sleep, timeout},
    };
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{self, protocol::frame::coding::CloseCode},
    };

    use super::super::*;
    use crate::{
        config::{Config, RtcPortRange, TransportBackend},
        runtime::{
            channel::{ChannelConfig, ChannelManager},
            http_server::app,
            metrics::RuntimeMetrics,
            stub_bus::{StubWebRtcAdapter, StubWebRtcEvent},
            transport_adapter::{RuntimeTransportAdapter, TransportConnectDirection},
        },
        signaling::{
            auth::{RegisteredJwtClaims, WebSocketConnectClaims, sign},
            current_bus::{
                CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId,
            },
            current_protocol::{
                CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackPayload,
                CurrentServerMessage, CurrentServerRequest, CurrentSessionInfoUpdatePayload,
                CurrentStartupPayload, CurrentTransportConnectPayload, CurrentWebSocketCredentials,
            },
            shared::{AvailableFeatures, RecordingState, SessionId, SessionInfo, StreamType},
            webrtc::{DtlsFingerprint, DtlsParameters, MediaKind, RtpParameters},
        },
    };

    const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
    const TEST_CHANNEL_KEY: &str = "Y2hhbm5lbC1rZXk=";
    type TestWebSocket =
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
    type CreateChannelQuery = ChannelConfig;

    struct TestServer {
        addr: SocketAddr,
        handle: JoinHandle<()>,
        channels: Arc<ChannelManager>,
        state: RuntimeState,
    }

    impl TestServer {
        fn url(&self) -> String {
            format!("ws://{}/", self.addr)
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    fn test_config(
        authentication_timeout_ms: u64,
        session_timeout_ms: u64,
        ping_interval_ms: u64,
        channel_size: usize,
    ) -> Config {
        Config {
            auth_key: TEST_AUTH_KEY.to_owned(),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            authentication_timeout_ms,
            channel_size,
            session_timeout_ms,
            ping_interval_ms,
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            rtc_media_worker_count: 1,
            transport_backend: TransportBackend::Stub,
        }
    }

    async fn spawn_test_server(
        authentication_timeout_ms: u64,
        channel_size: usize,
    ) -> Option<TestServer> {
        spawn_test_server_with_timeouts(
            authentication_timeout_ms,
            10_000,
            60_000,
            channel_size,
            RuntimeTransportAdapter::stub(),
        )
        .await
    }

    async fn spawn_test_server_with_timeouts(
        authentication_timeout_ms: u64,
        session_timeout_ms: u64,
        ping_interval_ms: u64,
        channel_size: usize,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Option<TestServer> {
        let channels = Arc::new(ChannelManager::new());
        let state = RuntimeState {
            config: test_config(
                authentication_timeout_ms,
                session_timeout_ms,
                ping_interval_ms,
                channel_size,
            ),
            channels: Arc::clone(&channels),
            metrics: Arc::new(RuntimeMetrics::default()),
            transport_adapter,
        };
        let state_for_server = state.clone();
        let listener = TcpListener::bind(state.config.bind_address).await.ok()?;
        let addr = listener.local_addr().ok()?;
        let handle = tokio::spawn(async move {
            let result = axum::serve(listener, app(state_for_server)).await;
            assert!(
                result.is_ok(),
                "test server should stop cleanly: {result:?}"
            );
        });
        Some(TestServer {
            addr,
            handle,
            channels,
            state,
        })
    }

    async fn spawn_test_server_with_adapter(
        authentication_timeout_ms: u64,
        channel_size: usize,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Option<TestServer> {
        spawn_test_server_with_timeouts(
            authentication_timeout_ms,
            10_000,
            60_000,
            channel_size,
            transport_adapter,
        )
        .await
    }

    async fn wait_for_stub_webrtc_events(
        adapter: &StubWebRtcAdapter,
        event_count: usize,
    ) -> Option<Vec<StubWebRtcEvent>> {
        timeout(Duration::from_secs(1), async {
            loop {
                let events = adapter.snapshot_events();
                if events.len() >= event_count {
                    return events;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .ok()
    }

    async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
        let websocket = connect_async(server.url()).await.ok()?;
        Some(websocket.0)
    }

    fn signed_connect_claims(
        key: &str,
        channel_uuid: &str,
        session_id: SessionId,
    ) -> Option<String> {
        sign(
            &WebSocketConnectClaims {
                registered: RegisteredJwtClaims::default(),
                sfu_channel_uuid: channel_uuid.to_owned(),
                session_id,
                label: Some("Alice".to_owned()),
                permissions: None,
            },
            key,
        )
        .ok()
    }

    async fn create_channel(
        server: &TestServer,
        issuer: &str,
        key: Option<&str>,
        config: ChannelConfig,
    ) -> Arc<Channel> {
        server
            .channels
            .create_or_get(issuer, key, &config, None)
            .await
    }

    async fn authenticate_with_jwt(server: &TestServer, token: &str) -> Option<TestWebSocket> {
        let mut websocket = connect_websocket(server).await?;
        let payload = serde_json::to_string(&serde_json::json!({ "jwt": token })).ok()?;
        websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .ok()?;
        Some(websocket)
    }

    async fn authenticate_and_read_startup(
        server: &TestServer,
        token: &str,
    ) -> Option<(TestWebSocket, CurrentStartupPayload)> {
        let mut websocket = authenticate_with_jwt(server, token).await?;
        let startup_json = read_text_message(&mut websocket).await?;
        let startup = serde_json::from_str::<CurrentStartupPayload>(&startup_json).ok()?;
        Some((websocket, startup))
    }

    async fn read_message(
        websocket: &mut TestWebSocket,
    ) -> Option<tungstenite::Result<tungstenite::Message>> {
        websocket.next().await
    }

    async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
        let message = read_message(websocket).await?;
        let message = message.ok()?;
        match message {
            tungstenite::Message::Text(payload) => Some(payload.to_string()),
            _ => None,
        }
    }

    async fn read_bus_batch(websocket: &mut TestWebSocket) -> Option<CurrentBusBatch> {
        let payload = read_text_message(websocket).await?;
        serde_json::from_str(&payload).ok()
    }

    async fn read_server_request(
        websocket: &mut TestWebSocket,
    ) -> Option<(CurrentBusEnvelope, CurrentServerRequest)> {
        let batch = read_bus_batch(websocket).await?;
        let envelope = batch.first()?.clone();
        let request =
            serde_json::from_value::<CurrentServerRequest>(envelope.message.clone()).ok()?;
        Some((envelope, request))
    }

    async fn acknowledge_transport_bootstrap(websocket: &mut TestWebSocket) -> Option<()> {
        acknowledge_transport_bootstrap_with_capabilities(websocket, test_client_rtp_capabilities())
            .await
    }

    async fn acknowledge_transport_bootstrap_with_capabilities(
        websocket: &mut TestWebSocket,
        capabilities: serde_json::Value,
    ) -> Option<()> {
        let batch = read_bus_batch(websocket).await?;
        let envelope = batch.first()?;
        let response = serde_json::to_string(&vec![CurrentBusEnvelope {
            message: capabilities,
            need_response: None,
            response_to: envelope.need_response.clone(),
        }])
        .ok()?;
        websocket
            .send(tungstenite::Message::Text(response.into()))
            .await
            .ok()?;
        Some(())
    }

    async fn respond_to_server_request(
        websocket: &mut TestWebSocket,
        request_id: CurrentBusRequestId,
        message: serde_json::Value,
    ) -> Option<()> {
        let response = serde_json::to_string(&vec![CurrentBusEnvelope {
            message,
            need_response: None,
            response_to: Some(request_id),
        }])
        .ok()?;
        websocket
            .send(tungstenite::Message::Text(response.into()))
            .await
            .ok()?;
        Some(())
    }

    fn test_client_rtp_capabilities() -> serde_json::Value {
        serde_json::json!({
            "codecs": [
                {
                    "mimeType": "audio/opus",
                    "kind": "audio",
                    "preferredPayloadType": 111,
                    "clockRate": 48000,
                    "channels": 2,
                    "parameters": { "useinbandfec": "1" },
                    "rtcpFeedback": [{ "type": "transport-cc" }]
                },
                {
                    "mimeType": "video/VP8",
                    "kind": "video",
                    "preferredPayloadType": 96,
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
                    "kind": "video",
                    "preferredPayloadType": 97,
                    "clockRate": 90000,
                    "parameters": { "apt": "96" },
                    "rtcpFeedback": []
                }
            ],
            "headerExtensions": [
                {
                    "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                    "preferredId": 1,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                },
                {
                    "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
                    "preferredId": 4,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                },
                {
                    "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
                    "preferredId": 5,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                },
                {
                    "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
                    "preferredId": 10,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                }
            ]
        })
    }

    fn test_client_rtp_capabilities_without_video_rtx() -> serde_json::Value {
        serde_json::json!({
            "codecs": [
                {
                    "mimeType": "audio/opus",
                    "kind": "audio",
                    "preferredPayloadType": 111,
                    "clockRate": 48000,
                    "channels": 2,
                    "parameters": { "useinbandfec": "1" },
                    "rtcpFeedback": [{ "type": "transport-cc" }]
                },
                {
                    "mimeType": "video/VP8",
                    "kind": "video",
                    "preferredPayloadType": 96,
                    "clockRate": 90000,
                    "parameters": {},
                    "rtcpFeedback": [
                        { "type": "nack" },
                        { "type": "nack", "parameter": "pli" },
                        { "type": "ccm", "parameter": "fir" },
                        { "type": "goog-remb" }
                    ]
                }
            ],
            "headerExtensions": [
                {
                    "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                    "preferredId": 1,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                }
            ]
        })
    }

    async fn send_bus_request_and_read_response(
        websocket: &mut TestWebSocket,
        request: CurrentClientRequest,
        request_id: CurrentBusRequestId,
    ) -> Option<CurrentBusEnvelope> {
        let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
            message: serde_json::to_value(request).ok()?,
            need_response: Some(request_id),
            response_to: None,
        }])
        .ok()?;
        websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .ok()?;
        let response_batch = read_bus_batch(websocket).await?;
        response_batch.first().cloned()
    }

    async fn send_bus_message(
        websocket: &mut TestWebSocket,
        message: CurrentClientMessage,
    ) -> Option<()> {
        let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
            message: serde_json::to_value(message).ok()?,
            need_response: None,
            response_to: None,
        }])
        .ok()?;
        websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await
            .ok()?;
        Some(())
    }

    async fn read_server_message(websocket: &mut TestWebSocket) -> Option<CurrentServerMessage> {
        let batch = read_bus_batch(websocket).await?;
        let envelope = batch.first()?;
        serde_json::from_value(envelope.message.clone()).ok()
    }

    async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
        loop {
            let message = read_message(websocket).await?;
            if let tungstenite::Message::Close(frame) = message.ok()? {
                return frame.map(|frame| frame.code);
            }
        }
    }

    async fn setup_authenticated_session(
        server: &TestServer,
        channel: &Arc<Channel>,
        session_id: SessionId,
    ) -> Option<TestWebSocket> {
        let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id)?;
        let (mut websocket, _startup) = authenticate_and_read_startup(server, &token).await?;
        acknowledge_transport_bootstrap(&mut websocket).await?;
        Some(websocket)
    }

    fn sample_client_dtls_parameters() -> DtlsParameters {
        DtlsParameters {
            role: String::from("client"),
            fingerprints: vec![DtlsFingerprint {
                algorithm: String::from("sha-256"),
                value: String::from(
                    "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
                ),
            }],
        }
    }

    fn invalid_dtls_parameters_for_stub_rejection() -> DtlsParameters {
        DtlsParameters {
            role: String::new(),
            fingerprints: vec![],
        }
    }

    #[tokio::test]
    async fn websocket_times_out_when_client_never_authenticates() {
        let server = spawn_test_server(25, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let websocket = connect_websocket(&server).await;
        assert!(websocket.is_some());
        let Some(mut websocket) = websocket else {
            return;
        };

        let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
        assert!(
            close_code.is_ok(),
            "timeout close should arrive promptly: {close_code:?}"
        );
        assert_eq!(close_code.ok().flatten(), Some(CloseCode::Library(4107)));

        sleep(Duration::from_millis(20)).await;
        let metrics = server.state.metrics.snapshot();
        assert_eq!(metrics.ws_connections_accepted, 1);
        assert_eq!(metrics.ws_handshake_rejected_timeout, 1);
    }

    #[tokio::test]
    async fn websocket_authenticates_with_channel_key_and_sends_startup_payload() {
        let server = spawn_test_server(1_000, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel = create_channel(
            &server,
            "issuer-a",
            Some(TEST_CHANNEL_KEY),
            CreateChannelQuery::default(),
        )
        .await;
        let token = signed_connect_claims(TEST_CHANNEL_KEY, channel.uuid(), SessionId::Integer(7));
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };
        let websocket = connect_websocket(&server).await;
        assert!(websocket.is_some());
        let Some(mut websocket) = websocket else {
            return;
        };
        let payload = serde_json::to_string(&CurrentWebSocketCredentials {
            channel_uuid: Some(channel.uuid().to_owned()),
            jwt: token,
        })
        .ok();
        assert!(payload.is_some());
        let Some(payload) = payload else {
            return;
        };

        let send_result = websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await;
        assert!(
            send_result.is_ok(),
            "auth payload should send: {send_result:?}"
        );

        let message = read_message(&mut websocket).await;
        assert!(message.is_some(), "startup payload should exist");
        let Some(message) = message else {
            return;
        };
        assert!(
            message.is_ok(),
            "startup payload should be readable: {message:?}"
        );
        let Some(message) = message.ok() else {
            return;
        };
        let tungstenite::Message::Text(startup_json) = message else {
            return;
        };
        let startup = serde_json::from_str::<CurrentStartupPayload>(&startup_json);
        assert!(
            startup.is_ok(),
            "startup payload should deserialize: {startup:?}"
        );
        let Some(startup) = startup.ok() else {
            return;
        };
        assert_eq!(
            startup,
            CurrentStartupPayload {
                available_features: AvailableFeatures {
                    rtc: true,
                    transcription: false,
                    audio_recording: false,
                    video_recording: false,
                },
                recording_state: RecordingState {
                    recording: Some(false),
                    audio: Some(false),
                    transcription: Some(false),
                    video: Some(false),
                },
            }
        );

        let close_result = websocket.close(None).await;
        assert!(close_result.is_ok());
        sleep(Duration::from_millis(20)).await;

        let metrics = server.state.metrics.snapshot();
        assert_eq!(metrics.ws_connections_accepted, 1);
        assert_eq!(metrics.ws_handshake_credentials_received, 1);
        assert_eq!(metrics.ws_sessions_joined, 1);
        assert_eq!(metrics.ws_session_loops_started, 1);
    }

    #[tokio::test]
    async fn websocket_rejects_explicit_channel_uuid_that_disagrees_with_claims() {
        let server = spawn_test_server(1_000, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let first_channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        let second_channel =
            create_channel(&server, "issuer-b", None, CreateChannelQuery::default()).await;
        let token =
            signed_connect_claims(TEST_AUTH_KEY, first_channel.uuid(), SessionId::Integer(8));
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };
        let websocket = connect_websocket(&server).await;
        assert!(websocket.is_some());
        let Some(mut websocket) = websocket else {
            return;
        };
        let payload = serde_json::to_string(&CurrentWebSocketCredentials {
            channel_uuid: Some(second_channel.uuid().to_owned()),
            jwt: token,
        })
        .ok();
        assert!(payload.is_some());
        let Some(payload) = payload else {
            return;
        };

        let send_result = websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await;
        assert!(
            send_result.is_ok(),
            "mismatched auth payload should still send: {send_result:?}"
        );

        assert_eq!(
            read_close_code(&mut websocket).await,
            Some(CloseCode::Library(4106)),
        );
    }

    #[tokio::test]
    async fn websocket_accepts_global_key_without_explicit_channel_uuid() {
        let server = spawn_test_server(1_000, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(9));
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };
        let websocket = connect_websocket(&server).await;
        assert!(websocket.is_some());
        let Some(mut websocket) = websocket else {
            return;
        };
        let payload = serde_json::to_string(&serde_json::json!({ "jwt": token })).ok();
        assert!(payload.is_some());
        let Some(payload) = payload else {
            return;
        };

        let send_result = websocket
            .send(tungstenite::Message::Text(payload.into()))
            .await;
        assert!(
            send_result.is_ok(),
            "jwt-only payload should send: {send_result:?}"
        );

        let message = read_message(&mut websocket).await;
        assert!(message.is_some(), "startup payload should exist");
        let Some(message) = message else {
            return;
        };
        assert!(
            message.is_ok(),
            "startup payload should be readable: {message:?}"
        );
        assert!(matches!(message.ok(), Some(tungstenite::Message::Text(_))));
    }

    #[tokio::test]
    async fn websocket_sends_router_capabilities_in_transport_bootstrap_after_startup() {
        let server = spawn_test_server(1_000, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
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

    #[tokio::test]
    async fn websocket_sends_ping_requests_and_accepts_responses() {
        let server =
            spawn_test_server_with_timeouts(1_000, 200, 20, 100, RuntimeTransportAdapter::stub())
                .await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-ping", None, CreateChannelQuery::default()).await;
        let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(410));
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };
        let authenticated = authenticate_and_read_startup(&server, &token).await;
        assert!(authenticated.is_some());
        let Some((mut websocket, _startup)) = authenticated else {
            return;
        };
        assert!(
            acknowledge_transport_bootstrap(&mut websocket)
                .await
                .is_some()
        );

        let server_request =
            timeout(Duration::from_secs(1), read_server_request(&mut websocket)).await;
        assert!(
            server_request.is_ok(),
            "server should send ping request promptly: {server_request:?}"
        );
        let Some((envelope, CurrentServerRequest::Ping)) = server_request.ok().flatten() else {
            panic!("expected PING server request");
        };
        let request_id = envelope.need_response.clone();
        assert!(request_id.is_some(), "PING should expect a response");
        let Some(request_id) = request_id else {
            return;
        };
        assert!(
            respond_to_server_request(&mut websocket, request_id, serde_json::json!({}))
                .await
                .is_some(),
            "client should be able to answer server ping"
        );

        let no_close = timeout(Duration::from_millis(80), read_close_code(&mut websocket)).await;
        assert!(
            no_close.is_err(),
            "session should remain open after answering ping"
        );
    }

    #[tokio::test]
    async fn websocket_closes_when_ping_response_times_out() {
        let server =
            spawn_test_server_with_timeouts(1_000, 30, 15, 100, RuntimeTransportAdapter::stub())
                .await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel = create_channel(
            &server,
            "issuer-ping-timeout",
            None,
            CreateChannelQuery::default(),
        )
        .await;
        let session_id = SessionId::Integer(411);
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
        assert!(
            acknowledge_transport_bootstrap(&mut websocket)
                .await
                .is_some()
        );

        let server_request =
            timeout(Duration::from_secs(1), read_server_request(&mut websocket)).await;
        assert!(
            server_request.is_ok(),
            "server should send ping request promptly: {server_request:?}"
        );
        let Some((_envelope, CurrentServerRequest::Ping)) = server_request.ok().flatten() else {
            panic!("expected PING server request");
        };

        let close_code = timeout(Duration::from_secs(1), read_close_code(&mut websocket)).await;
        assert!(
            close_code.is_ok(),
            "server should close after ping timeout: {close_code:?}"
        );
        assert_eq!(close_code.ok().flatten(), Some(CloseCode::Error));

        sleep(Duration::from_millis(20)).await;
        let metrics = server.state.metrics.snapshot();
        assert_eq!(metrics.ws_session_loop_exits_ping_timeout, 1);
        assert!(
            !server
                .channels
                .has_session(channel.uuid(), &session_id)
                .await
        );
    }

    #[tokio::test]
    async fn websocket_emits_stub_webrtc_bootstrap_event() {
        let adapter = Arc::new(StubWebRtcAdapter::default());
        let transport_adapter =
            RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
        let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(210));
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };
        let authenticated = authenticate_and_read_startup(&server, &token).await;
        assert!(authenticated.is_some());
        let Some((mut websocket, _startup)) = authenticated else {
            return;
        };

        let batch = read_bus_batch(&mut websocket).await;
        assert!(batch.is_some());

        let events = wait_for_stub_webrtc_events(&adapter, 1).await;
        assert!(events.is_some());
        let Some(events) = events else {
            return;
        };
        assert_eq!(events, vec![StubWebRtcEvent::BootstrapRequested]);
    }

    #[tokio::test]
    async fn websocket_closure_emits_stub_webrtc_session_closed_event() {
        let adapter = Arc::new(StubWebRtcAdapter::default());
        let transport_adapter =
            RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
        let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        let session_id = SessionId::Integer(213);
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
        let batch = read_bus_batch(&mut websocket).await;
        assert!(batch.is_some(), "transport bootstrap should be sent");
        let close_result = websocket.close(None).await;
        assert!(close_result.is_ok());

        let events = wait_for_stub_webrtc_events(&adapter, 2).await;
        assert!(events.is_some());
        let Some(events) = events else {
            return;
        };
        let expected = vec![
            StubWebRtcEvent::BootstrapRequested,
            StubWebRtcEvent::SessionClosed { session_id },
        ];
        assert_eq!(events, expected);
    }

    #[tokio::test]
    async fn stale_replaced_socket_close_cleans_only_the_stale_transport_session() {
        let adapter = Arc::new(StubWebRtcAdapter::default());
        let transport_adapter =
            RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
        let server = spawn_test_server_with_adapter(1_000, 100, transport_adapter).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        let session_id = SessionId::Integer(260);
        let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id.clone());
        assert!(token.is_some());
        let Some(token) = token else {
            return;
        };

        let first_authenticated = authenticate_and_read_startup(&server, &token).await;
        assert!(first_authenticated.is_some());
        let Some((mut first_socket, _first_startup)) = first_authenticated else {
            return;
        };
        assert!(
            read_bus_batch(&mut first_socket).await.is_some(),
            "first session should receive transport bootstrap"
        );

        let second_authenticated = authenticate_and_read_startup(&server, &token).await;
        assert!(second_authenticated.is_some());
        let Some((mut second_socket, _second_startup)) = second_authenticated else {
            return;
        };
        assert!(
            read_bus_batch(&mut second_socket).await.is_some(),
            "replacement session should receive transport bootstrap"
        );

        assert_eq!(
            read_close_code(&mut first_socket).await,
            Some(CloseCode::Library(4108))
        );

        let events = wait_for_stub_webrtc_events(&adapter, 3).await;
        assert!(events.is_some());
        let Some(events) = events else {
            return;
        };
        assert_eq!(
            events,
            vec![
                StubWebRtcEvent::BootstrapRequested,
                StubWebRtcEvent::BootstrapRequested,
                StubWebRtcEvent::SessionClosed {
                    session_id: session_id.clone(),
                }
            ]
        );

        let close_result = second_socket.close(None).await;
        assert!(close_result.is_ok());
        let events = wait_for_stub_webrtc_events(&adapter, 4).await;
        assert!(events.is_some());
        let Some(events) = events else {
            return;
        };
        assert_eq!(
            events.last(),
            Some(&StubWebRtcEvent::SessionClosed { session_id })
        );
    }

    #[tokio::test]
    async fn disconnect_cleanup_still_closes_transport_adapter_session_state() {
        let adapter = Arc::new(StubWebRtcAdapter::default());
        let transport_adapter =
            RuntimeTransportAdapter::from_stub_adapter(Arc::<StubWebRtcAdapter>::clone(&adapter));
        let server = spawn_test_server_with_adapter(1_000, 10, transport_adapter).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

        let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
        let mut bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
        assert!(alice.is_some());
        assert!(bob.is_some());
        let Some(ref mut alice) = alice else {
            return;
        };
        let Some(ref mut bob) = bob else {
            return;
        };

        server
            .channels
            .disconnect_sessions(channel.uuid(), &[SessionId::Integer(1)])
            .await;

        assert_eq!(read_close_code(alice).await, Some(CloseCode::Library(4108)));
        let peer_message = read_server_message(bob).await;
        assert!(
            matches!(peer_message, Some(CurrentServerMessage::SessionDeparted(_))),
            "remaining peer should receive session departure after disconnect: {peer_message:?}"
        );

        let events = wait_for_stub_webrtc_events(&adapter, 3).await;
        assert!(events.is_some());
        let Some(events) = events else {
            return;
        };
        assert_eq!(
            events.last(),
            Some(&StubWebRtcEvent::SessionClosed {
                session_id: SessionId::Integer(1)
            })
        );
    }

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
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
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
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
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
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
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
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
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
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
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

    #[tokio::test]
    async fn websocket_returns_empty_object_for_malformed_bus_requests() {
        let server = spawn_test_server(1_000, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(12));
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

        let malformed_request = serde_json::to_string(&vec![CurrentBusEnvelope {
            message: serde_json::json!({
                "name": "NOT_A_REAL_REQUEST"
            }),
            need_response: Some(CurrentBusRequestId::new(CurrentBusOrigin::Client, 4, 2)),
            response_to: None,
        }]);
        assert!(malformed_request.is_ok());
        let Some(malformed_request) = malformed_request.ok() else {
            return;
        };
        let send_result = websocket
            .send(tungstenite::Message::Text(malformed_request.into()))
            .await;
        assert!(
            send_result.is_ok(),
            "malformed request should still send: {send_result:?}"
        );
        let response = read_bus_batch(&mut websocket).await;
        assert!(response.is_some());
        let Some(response) = response else {
            return;
        };
        let Some(envelope) = response.first() else {
            return;
        };
        assert_eq!(envelope.message, serde_json::json!({}));
        assert_eq!(
            envelope
                .response_to
                .as_ref()
                .map(CurrentBusRequestId::as_str),
            Some("c_4_2")
        );
    }

    #[tokio::test]
    async fn websocket_rejects_invalid_json_payload() {
        let server = spawn_test_server(1_000, 100).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let websocket = connect_websocket(&server).await;
        assert!(websocket.is_some());
        let Some(mut websocket) = websocket else {
            return;
        };

        let send_result = websocket
            .send(tungstenite::Message::Text("not-json".into()))
            .await;
        assert!(
            send_result.is_ok(),
            "invalid payload should still send: {send_result:?}"
        );

        assert_eq!(
            read_close_code(&mut websocket).await,
            Some(CloseCode::Error),
        );
    }

    #[tokio::test]
    async fn websocket_recreates_channel_after_last_disconnect_cleanup() {
        let server = spawn_test_server(1_000, 1).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

        let first_token =
            signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(1));
        let second_token =
            signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), SessionId::Integer(2));
        assert!(first_token.is_some());
        assert!(second_token.is_some());
        let (Some(first_token), Some(second_token)) = (first_token, second_token) else {
            return;
        };

        let first_websocket = authenticate_with_jwt(&server, &first_token).await;
        assert!(first_websocket.is_some());
        let Some(mut first_websocket) = first_websocket else {
            return;
        };
        let startup = read_message(&mut first_websocket).await;
        assert!(startup.is_some(), "first startup payload should exist");
        let Some(startup) = startup else {
            return;
        };
        assert!(
            startup.is_ok(),
            "first startup payload should arrive: {startup:?}"
        );

        let second_websocket = authenticate_with_jwt(&server, &second_token).await;
        assert!(second_websocket.is_some());
        let Some(mut second_websocket) = second_websocket else {
            return;
        };
        assert_eq!(
            read_close_code(&mut second_websocket).await,
            Some(CloseCode::Library(4109)),
        );

        let close_result = first_websocket.close(None).await;
        assert!(
            close_result.is_ok(),
            "first websocket should close cleanly: {close_result:?}"
        );
        drop(first_websocket);
        sleep(Duration::from_millis(20)).await;

        assert!(server.channels.get_by_uuid(channel.uuid()).await.is_none());

        let replacement_channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;
        assert_ne!(replacement_channel.uuid(), channel.uuid());
        let third_token = signed_connect_claims(
            TEST_AUTH_KEY,
            replacement_channel.uuid(),
            SessionId::Integer(3),
        );
        assert!(third_token.is_some());
        let Some(third_token) = third_token else {
            return;
        };

        let third_websocket = authenticate_with_jwt(&server, &third_token).await;
        assert!(third_websocket.is_some());
        let Some(mut third_websocket) = third_websocket else {
            return;
        };
        let startup = read_message(&mut third_websocket).await;
        assert!(startup.is_some(), "third startup payload should exist");
        let Some(startup) = startup else {
            return;
        };
        assert!(
            startup.is_ok(),
            "third startup payload should arrive after cleanup: {startup:?}"
        );
        assert!(matches!(startup.ok(), Some(tungstenite::Message::Text(_))));
    }

    #[tokio::test]
    async fn broadcast_reaches_other_sessions_in_same_channel() {
        let server = spawn_test_server(1_000, 10).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

        let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
        let mut bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
        assert!(alice.is_some());
        assert!(bob.is_some());
        let Some(ref mut alice) = alice else {
            return;
        };
        let Some(ref mut bob) = bob else {
            return;
        };

        let sent = send_bus_message(
            alice,
            CurrentClientMessage::Broadcast(serde_json::json!({"text": "hello"})),
        )
        .await;
        assert!(sent.is_some());

        let msg = read_server_message(bob).await;
        assert!(msg.is_some(), "bob should receive broadcast");
        if let Some(CurrentServerMessage::Broadcast(payload)) = msg {
            assert_eq!(payload.sender_id, SessionId::Integer(1));
            assert_eq!(payload.message, serde_json::json!({"text": "hello"}));
        } else {
            panic!("expected Broadcast, got {msg:?}");
        }
    }

    #[tokio::test]
    async fn session_leave_notifies_remaining_peers() {
        let server = spawn_test_server(1_000, 10).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

        let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
        let bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
        assert!(alice.is_some());
        assert!(bob.is_some());
        let Some(ref mut alice) = alice else {
            return;
        };
        let Some(mut bob) = bob else {
            return;
        };

        let close_result = bob.close(None).await;
        assert!(close_result.is_ok());
        drop(bob);
        sleep(Duration::from_millis(50)).await;

        let msg = read_server_message(alice).await;
        assert!(msg.is_some(), "alice should receive session departure");
        if let Some(CurrentServerMessage::SessionDeparted(payload)) = msg {
            assert_eq!(payload.session_id, SessionId::Integer(2));
        } else {
            panic!("expected SessionDeparted, got {msg:?}");
        }
    }

    #[tokio::test]
    async fn info_change_broadcasts_to_all_sessions() {
        let server = spawn_test_server(1_000, 10).await;
        assert!(server.is_some());
        let Some(server) = server else {
            return;
        };
        let channel =
            create_channel(&server, "issuer-a", None, CreateChannelQuery::default()).await;

        let mut alice = setup_authenticated_session(&server, &channel, SessionId::Integer(1)).await;
        let mut bob = setup_authenticated_session(&server, &channel, SessionId::Integer(2)).await;
        assert!(alice.is_some());
        assert!(bob.is_some());
        let Some(ref mut alice) = alice else {
            return;
        };
        let Some(ref mut bob) = bob else {
            return;
        };

        let sent = send_bus_message(
            alice,
            CurrentClientMessage::UpdateSessionInfo(CurrentSessionInfoUpdatePayload {
                info: SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
                need_refresh: None,
            }),
        )
        .await;
        assert!(sent.is_some());

        let alice_msg = read_server_message(alice).await;
        let bob_msg = read_server_message(bob).await;
        assert!(alice_msg.is_some(), "alice should receive info change");
        assert!(bob_msg.is_some(), "bob should receive info change");
        if let Some(CurrentServerMessage::SessionInfoChanged(snapshot)) = bob_msg {
            assert!(snapshot.contains_key("1"));
            assert_eq!(
                snapshot.get("1").and_then(|info| info.is_talking),
                Some(true)
            );
        } else {
            panic!("expected SessionInfoChanged, got {bob_msg:?}");
        }
    }
}
