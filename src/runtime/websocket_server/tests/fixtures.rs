pub(super) use std::net::{IpAddr, Ipv4Addr, SocketAddr};
pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;

pub(super) use futures_util::{SinkExt, StreamExt};
pub(super) use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{sleep, timeout},
};
pub(super) use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol::frame::coding::CloseCode},
};

pub(super) use crate::{
    config::{Config, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TransportBackend},
    runtime::{
        RuntimeState,
        channel::Channel,
        channel::{ChannelAdmissionPolicy, ChannelConfig, ChannelManager},
        http_server::app,
        metrics::RuntimeMetrics,
        recording::MediaTap,
        stub_bus::{StubWebRtcAdapter, StubWebRtcEvent},
        testing::decode_native_welcome_batch,
        transport_adapter::{
            RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter, TransportConnectDirection,
        },
    },
    signaling::{
        auth::{RegisteredJwtClaims, WebSocketConnectClaims, sign},
        current_bus::{CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
        current_protocol::{
            CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackPayload,
            CurrentServerMessage, CurrentServerRequest, CurrentSessionInfoUpdatePayload,
            CurrentTransportConnectPayload, CurrentWebSocketCredentials,
        },
        protocol::{
            AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId,
            ServerEnvelope, ServerMessage, ServerRequest, SessionDescriptionPayload,
            WelcomePayload,
        },
        shared::{AvailableFeatures, RecordingState, SessionId, SessionInfo, StreamType},
        webrtc::{DtlsFingerprint, DtlsParameters, MediaKind, RtpParameters},
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub(super) const TEST_CHANNEL_KEY: &str = "Y2hhbm5lbC1rZXk=";
pub(super) type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
pub(super) type CreateChannelQuery = ChannelConfig;

pub(super) struct TestServer {
    pub(super) addr: SocketAddr,
    pub(super) handle: JoinHandle<()>,
    pub(super) channels: Arc<ChannelManager>,
    pub(super) state: RuntimeState,
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

pub(super) fn test_config(
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
        enable_native_protocol: false,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        rtc_media_worker_count: 1,
        transport_backend: TransportBackend::Stub,
    }
}

pub(super) async fn spawn_test_server(
    authentication_timeout_ms: u64,
    channel_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts_and_protocol(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
        RuntimeTransportAdapter::builder().stub().build(),
        false,
    )
    .await
}

pub(super) async fn spawn_test_server_with_timeouts(
    authentication_timeout_ms: u64,
    session_timeout_ms: u64,
    ping_interval_ms: u64,
    channel_size: usize,
    transport_adapter: RuntimeTransportAdapter,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts_and_protocol(
        authentication_timeout_ms,
        session_timeout_ms,
        ping_interval_ms,
        channel_size,
        transport_adapter,
        false,
    )
    .await
}

pub(super) async fn spawn_test_server_with_timeouts_and_protocol(
    authentication_timeout_ms: u64,
    session_timeout_ms: u64,
    ping_interval_ms: u64,
    channel_size: usize,
    transport_adapter: RuntimeTransportAdapter,
    enable_native_protocol: bool,
) -> Option<TestServer> {
    let mut config = test_config(
        authentication_timeout_ms,
        session_timeout_ms,
        ping_interval_ms,
        channel_size,
    );
    let channels = Arc::new(ChannelManager::for_test_with_admission_policy(
        ChannelAdmissionPolicy::new(config.channel_size),
    ));
    config.enable_native_protocol = enable_native_protocol;
    let state = RuntimeState {
        config,
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

pub(super) async fn spawn_test_server_with_adapter(
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

pub(super) async fn spawn_native_protocol_test_server(
    authentication_timeout_ms: u64,
    channel_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts_and_protocol(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
        RuntimeTransportAdapter::builder().stub().build(),
        true,
    )
    .await
}

pub(super) fn build_real_rtc_transport_adapter() -> RuntimeTransportAdapter {
    RuntimeTransportAdapter::builder()
        .rtc(RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(47_200, 47_299),
            1,
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
        ))
        .build()
}

pub(super) async fn spawn_native_protocol_rtc_test_server(
    authentication_timeout_ms: u64,
    channel_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts_and_protocol(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
        build_real_rtc_transport_adapter(),
        true,
    )
    .await
}

pub(super) async fn wait_for_stub_webrtc_events(
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

pub(super) async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.url()).await.ok()?;
    Some(websocket.0)
}

pub(super) fn signed_connect_claims(
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

pub(super) async fn create_channel(
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

pub(super) async fn authenticate_with_jwt(
    server: &TestServer,
    token: &str,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_native_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: None,
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub(super) async fn authenticate_with_credentials(
    server: &TestServer,
    credentials: &CurrentWebSocketCredentials,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_native_auth(AuthPayload {
        jwt: credentials.jwt.clone(),
        channel: credentials.channel_uuid.clone(),
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub(super) async fn authenticate_and_read_welcome(
    server: &TestServer,
    token: &str,
) -> Option<(TestWebSocket, WelcomePayload)> {
    let mut websocket = authenticate_with_jwt(server, token).await?;
    let welcome = read_welcome(&mut websocket).await?;
    Some((websocket, welcome))
}

pub(super) async fn read_welcome(websocket: &mut TestWebSocket) -> Option<WelcomePayload> {
    let payload = read_text_message(websocket).await?;
    decode_native_welcome_batch(&payload)
}

pub(super) async fn read_native_server_batch(
    websocket: &mut TestWebSocket,
) -> Option<EnvelopeBatch> {
    let payload = read_text_message(websocket).await?;
    serde_json::from_str(&payload).ok()
}

pub(super) fn first_native_server_request(
    batch: &EnvelopeBatch,
) -> Option<(RequestId, ServerRequest)> {
    let envelope = batch.first()?.clone();
    match ServerEnvelope::decode(envelope).ok()? {
        ServerEnvelope::Request {
            request_id,
            request,
        } => Some((request_id, request)),
        ServerEnvelope::Message(_) | ServerEnvelope::Response { .. } => None,
    }
}

pub(super) fn native_server_messages(batch: &EnvelopeBatch) -> Option<Vec<ServerMessage>> {
    let mut messages = Vec::new();
    for envelope in batch.clone() {
        match ServerEnvelope::decode(envelope).ok()? {
            ServerEnvelope::Message(message) => messages.push(message),
            ServerEnvelope::Request { .. } | ServerEnvelope::Response { .. } => return None,
        }
    }
    Some(messages)
}

pub(super) async fn respond_to_native_negotiation_request(
    websocket: &mut TestWebSocket,
    response_to: RequestId,
    request: ServerRequest,
    sdp: &str,
) -> Option<()> {
    let response = match request {
        ServerRequest::Offer(_) => ClientResponse::Offer(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
        }),
        ServerRequest::Renegotiate(_) => ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
        }),
        ServerRequest::Ping => return None,
    };
    let frame = serde_json::to_string(&vec![
        ClientEnvelope::Response {
            response_to,
            response,
        }
        .into_envelope()
        .ok()?,
    ])
    .ok()?;
    websocket
        .send(tungstenite::Message::Text(frame.into()))
        .await
        .ok()?;
    Some(())
}

pub(super) async fn read_message(
    websocket: &mut TestWebSocket,
) -> Option<tungstenite::Result<tungstenite::Message>> {
    websocket.next().await
}

pub(super) async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    let message = read_message(websocket).await?;
    let message = message.ok()?;
    match message {
        tungstenite::Message::Text(payload) => Some(payload.to_string()),
        _ => None,
    }
}

pub(super) async fn read_bus_batch(websocket: &mut TestWebSocket) -> Option<CurrentBusBatch> {
    let payload = read_text_message(websocket).await?;
    serde_json::from_str(&payload).ok()
}

pub(super) async fn read_server_request(
    websocket: &mut TestWebSocket,
) -> Option<(CurrentBusEnvelope, CurrentServerRequest)> {
    let batch = read_bus_batch(websocket).await?;
    let envelope = batch.first()?.clone();
    let request = serde_json::from_value::<CurrentServerRequest>(envelope.message.clone()).ok()?;
    Some((envelope, request))
}

pub(super) async fn acknowledge_transport_bootstrap(websocket: &mut TestWebSocket) -> Option<()> {
    acknowledge_transport_bootstrap_with_capabilities(websocket, test_client_rtp_capabilities())
        .await
}

pub(super) async fn acknowledge_transport_bootstrap_with_capabilities(
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

pub(super) async fn respond_to_server_request(
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

fn encode_native_auth(auth_payload: AuthPayload) -> Option<String> {
    let envelope = ClientEnvelope::Message(ClientMessage::Auth(auth_payload))
        .into_envelope()
        .ok()?;
    serde_json::to_string(&vec![envelope]).ok()
}

pub(super) fn test_client_rtp_capabilities() -> serde_json::Value {
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

pub(super) fn test_client_rtp_capabilities_without_video_rtx() -> serde_json::Value {
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

pub(super) async fn send_bus_request_and_read_response(
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

pub(super) async fn send_bus_message(
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

pub(super) async fn read_server_message(
    websocket: &mut TestWebSocket,
) -> Option<CurrentServerMessage> {
    let batch = read_bus_batch(websocket).await?;
    let envelope = batch.first()?;
    serde_json::from_value(envelope.message.clone()).ok()
}

pub(super) async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        if let tungstenite::Message::Close(frame) = message.ok()? {
            return frame.map(|frame| frame.code);
        }
    }
}

pub(super) async fn setup_authenticated_session(
    server: &TestServer,
    channel: &Arc<Channel>,
    session_id: SessionId,
) -> Option<TestWebSocket> {
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id)?;
    let (mut websocket, _welcome) = authenticate_and_read_welcome(server, &token).await?;
    acknowledge_transport_bootstrap(&mut websocket).await?;
    Some(websocket)
}

pub(super) fn sample_client_dtls_parameters() -> DtlsParameters {
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

pub(super) fn invalid_dtls_parameters_for_stub_rejection() -> DtlsParameters {
    DtlsParameters {
        role: String::new(),
        fingerprints: vec![],
    }
}
