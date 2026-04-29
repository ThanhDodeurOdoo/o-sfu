pub(super) use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

pub(super) use futures_util::{SinkExt, StreamExt};
pub(super) use o_sfu_protocol::{
    shared::{AvailableFeatures, RecordingState, StreamType, UserId, UserPermissions},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId,
        ServerEnvelope, ServerMessage, ServerRequest, SessionDescriptionPayload,
        StreamIntentPayload, WelcomePayload,
    },
};
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
    config::{
        AuthConfig, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TelemetryConfig, TransportConfig,
        UserConfig, VideoBitrateLimits,
    },
    runtime::{
        RuntimeState,
        auth::{RegisteredJwtClaims, WebSocketConnectClaims, sign},
        diagnostics::DiagnosticsStore,
        http_server::app,
        metrics::RuntimeMetrics,
        recording::MediaTap,
        room::{
            Room, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerConfig, RoomManagerDeps,
            RoomRuntimePolicy, rtp_capabilities,
        },
        testing::{build_test_runtime_state, decode_protocol_welcome_batch},
        transport_adapter::{
            MediaTransportDeps, RtcTransport, RtcTransportConfig, RuntimeTransportAdapter,
            SessionBitrateLimits,
            test_support::{FakeWebRtcAdapter, FakeWebRtcEvent},
        },
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub(super) const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
pub(super) type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
pub(super) type CreateRoomQuery = RoomConfig;

pub(super) struct TestServer {
    pub(super) addr: SocketAddr,
    pub(super) handle: JoinHandle<()>,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) transport_adapter: RuntimeTransportAdapter,
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
    user_timeout_ms: u64,
    ping_interval_ms: u64,
    room_size: usize,
) -> Config {
    Config {
        auth: AuthConfig {
            key: TEST_AUTH_KEY.to_owned(),
            authentication_timeout_ms,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            trust_proxy_headers: false,
        },
        user: UserConfig {
            room_size,
            timeout_ms: user_timeout_ms,
            ping_interval_ms,
        },
        transport: TransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            max_bitrate_in_bps: 8_000_000,
            max_bitrate_out_bps: 10_000_000,
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_media_worker_count: 1,
        },
        codecs: CodecConfig {
            flags: MediaCodecFlags::default(),
            preferences: CodecPreferences::default(),
        },
        features: RuntimeFeatureFlags::default(),
        telemetry: TelemetryConfig::default(),
        diagnostics: DiagnosticsConfig::default(),
    }
}

pub(super) async fn spawn_test_server(
    authentication_timeout_ms: u64,
    room_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        room_size,
        RuntimeTransportAdapter::fake_for_testing(),
    )
    .await
}

pub(super) async fn spawn_test_server_with_timeouts(
    authentication_timeout_ms: u64,
    user_timeout_ms: u64,
    ping_interval_ms: u64,
    room_size: usize,
    transport_adapter: RuntimeTransportAdapter,
) -> Option<TestServer> {
    spawn_test_server_impl(
        authentication_timeout_ms,
        user_timeout_ms,
        ping_interval_ms,
        room_size,
        transport_adapter,
        RuntimeFeatureFlags::default(),
    )
    .await
}

async fn spawn_test_server_impl(
    authentication_timeout_ms: u64,
    user_timeout_ms: u64,
    ping_interval_ms: u64,
    room_size: usize,
    transport_adapter: RuntimeTransportAdapter,
    feature_flags: RuntimeFeatureFlags,
) -> Option<TestServer> {
    let mut config = test_config(
        authentication_timeout_ms,
        user_timeout_ms,
        ping_interval_ms,
        room_size,
    );
    config.features = feature_flags;
    let diagnostics = Arc::new(DiagnosticsStore::default());
    let metrics = Arc::new(RuntimeMetrics::default());
    let room_manager = Arc::new(RoomManager::new(
        RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(config.user.room_size),
                feature_flags,
                rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        RoomManagerDeps {
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::clone(&diagnostics),
            metrics: Arc::clone(&metrics),
        },
    ));
    let bind_address = config.http.bind_address;
    let state = build_test_runtime_state(
        &config,
        Arc::clone(&room_manager),
        Arc::clone(&diagnostics),
        metrics,
        transport_adapter.clone(),
    );
    let state_for_server = state.clone();
    let listener = TcpListener::bind(bind_address).await.ok()?;
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
        room_manager,
        transport_adapter,
        state,
    })
}

pub(super) async fn spawn_test_server_with_adapter(
    authentication_timeout_ms: u64,
    room_size: usize,
    transport_adapter: RuntimeTransportAdapter,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        room_size,
        transport_adapter,
    )
    .await
}

pub(super) async fn spawn_protocol_test_server(
    authentication_timeout_ms: u64,
    room_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        room_size,
        RuntimeTransportAdapter::fake_for_testing(),
    )
    .await
}

pub(super) async fn spawn_test_server_with_feature_flags(
    authentication_timeout_ms: u64,
    room_size: usize,
    transport_adapter: RuntimeTransportAdapter,
    feature_flags: RuntimeFeatureFlags,
) -> Option<TestServer> {
    spawn_test_server_impl(
        authentication_timeout_ms,
        10_000,
        60_000,
        room_size,
        transport_adapter,
        feature_flags,
    )
    .await
}

#[allow(
    clippy::panic,
    reason = "the test fixture builds a constant valid RTC transport and failing here means the fixture itself is invalid"
)]
pub(super) fn build_real_rtc_transport_adapter() -> RuntimeTransportAdapter {
    match RtcTransport::builder()
        .transport_config(RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(8_000_000, 10_000_000),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(47_200, 47_299),
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: CodecPreferences::default(),
        })
        .deps(MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(MediaTap::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        })
        .worker_count(1)
        .build()
    {
        Ok(transport) => RuntimeTransportAdapter::from_rtc_transport(transport),
        Err(error) => panic!("constant RTC test transport config should be valid: {error}"),
    }
}

pub(super) async fn spawn_protocol_rtc_test_server(
    authentication_timeout_ms: u64,
    room_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        room_size,
        build_real_rtc_transport_adapter(),
    )
    .await
}

pub(super) async fn wait_for_fake_webrtc_events(
    adapter: &FakeWebRtcAdapter,
    event_count: usize,
) -> Option<Vec<FakeWebRtcEvent>> {
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

pub(super) fn signed_connect_claims(key: &str, room_id: &str, user_id: UserId) -> Option<String> {
    signed_connect_claims_with_permissions(key, room_id, user_id, None)
}

pub(super) fn signed_connect_claims_with_permissions(
    key: &str,
    room_id: &str,
    user_id: UserId,
    permissions: Option<UserPermissions>,
) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            room_id: room_id.to_owned(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions,
        },
        key,
    )
    .ok()
}

pub(super) fn signed_legacy_channel_scoped_connect_claims(
    key: &str,
    user_id: UserId,
    permissions: Option<UserPermissions>,
) -> Option<String> {
    #[derive(serde::Serialize)]
    struct LegacyClaims {
        #[serde(flatten)]
        registered: RegisteredJwtClaims,
        #[serde(rename = "session_id")]
        user_id: UserId,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        permissions: Option<UserPermissions>,
    }

    sign(
        &LegacyClaims {
            registered: RegisteredJwtClaims::default(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions,
        },
        key,
    )
    .ok()
}

pub(super) async fn create_room(
    server: &TestServer,
    issuer: &str,
    key: Option<&str>,
    config: RoomConfig,
) -> Arc<Room> {
    server
        .room_manager
        .serve_room(issuer, key, &config, None)
        .await
}

pub(super) async fn authenticate_with_jwt(
    server: &TestServer,
    token: &str,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_protocol_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: None,
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub(super) async fn authenticate_with_room(
    server: &TestServer,
    token: &str,
    room_id: Option<&str>,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_protocol_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: room_id.map(str::to_owned),
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

fn encode_protocol_auth(auth_payload: AuthPayload) -> Option<String> {
    let envelope = ClientEnvelope::Message(ClientMessage::Auth(auth_payload))
        .into_envelope()
        .ok()?;
    serde_json::to_string(&vec![envelope]).ok()
}

pub(super) async fn authenticate_and_read_welcome(
    server: &TestServer,
    token: &str,
) -> Option<(TestWebSocket, WelcomePayload)> {
    let mut websocket = authenticate_with_jwt(server, token).await?;
    let welcome = read_welcome(&mut websocket).await?;
    Some((websocket, welcome))
}

pub(super) async fn complete_initial_negotiation(
    websocket: &mut TestWebSocket,
    sdp: &str,
) -> Option<()> {
    let (request_id, request) = wait_for_protocol_server_request(websocket).await?;
    respond_to_protocol_negotiation_request(websocket, request_id, request, sdp).await
}

pub(super) async fn setup_negotiated_session(
    server: &TestServer,
    room: &Arc<Room>,
    user_id: UserId,
) -> Option<TestWebSocket> {
    let token = signed_connect_claims(TEST_AUTH_KEY, room.uuid(), user_id)?;
    let (mut websocket, _welcome) = authenticate_and_read_welcome(server, &token).await?;
    complete_initial_negotiation(&mut websocket, "v=0\r\ns=test-answer\r\n").await?;
    Some(websocket)
}

pub(super) async fn read_welcome(websocket: &mut TestWebSocket) -> Option<WelcomePayload> {
    let payload = read_text_message(websocket).await?;
    decode_protocol_welcome_batch(&payload)
}

pub(super) async fn read_protocol_server_batch(
    websocket: &mut TestWebSocket,
) -> Option<EnvelopeBatch> {
    let payload = read_text_message(websocket).await?;
    serde_json::from_str(&payload).ok()
}

pub(super) async fn wait_for_protocol_server_request(
    websocket: &mut TestWebSocket,
) -> Option<(RequestId, ServerRequest)> {
    loop {
        let batch = read_protocol_server_batch(websocket).await?;
        if let Some(request) = first_protocol_server_request(&batch) {
            return Some(request);
        }
    }
}

pub(super) fn first_protocol_server_request(
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

pub(super) fn protocol_server_messages(batch: &EnvelopeBatch) -> Option<Vec<ServerMessage>> {
    let mut messages = Vec::new();
    for envelope in batch.clone() {
        match ServerEnvelope::decode(envelope).ok()? {
            ServerEnvelope::Message(message) => messages.push(message),
            ServerEnvelope::Request { .. } | ServerEnvelope::Response { .. } => return None,
        }
    }
    Some(messages)
}

pub(super) async fn respond_to_protocol_negotiation_request(
    websocket: &mut TestWebSocket,
    response_to: RequestId,
    request: ServerRequest,
    sdp: &str,
) -> Option<()> {
    let response = match request {
        ServerRequest::Offer(_) => ClientResponse::Offer(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
            upload_slots: Vec::new(),
        }),
        ServerRequest::Renegotiate(_) => ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
            upload_slots: Vec::new(),
        }),
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

pub(super) async fn read_websocket_ping(websocket: &mut TestWebSocket) -> Option<Vec<u8>> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            tungstenite::Message::Ping(payload) => return Some(payload.to_vec()),
            tungstenite::Message::Pong(_) => {}
            tungstenite::Message::Text(_)
            | tungstenite::Message::Binary(_)
            | tungstenite::Message::Close(_)
            | tungstenite::Message::Frame(_) => return None,
        }
    }
}

pub(super) async fn send_websocket_pong(
    websocket: &mut TestWebSocket,
    payload: Vec<u8>,
) -> Option<()> {
    websocket
        .send(tungstenite::Message::Pong(payload.into()))
        .await
        .ok()?;
    Some(())
}

pub(super) async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            tungstenite::Message::Text(payload) => return Some(payload.to_string()),
            tungstenite::Message::Ping(payload) => {
                websocket
                    .send(tungstenite::Message::Pong(payload))
                    .await
                    .ok()?;
            }
            tungstenite::Message::Pong(_) => {}
            tungstenite::Message::Binary(_)
            | tungstenite::Message::Close(_)
            | tungstenite::Message::Frame(_) => return None,
        }
    }
}

pub(super) async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            tungstenite::Message::Close(frame) => return frame.map(|frame| frame.code),
            tungstenite::Message::Ping(payload) => {
                websocket
                    .send(tungstenite::Message::Pong(payload))
                    .await
                    .ok()?;
            }
            tungstenite::Message::Pong(_)
            | tungstenite::Message::Text(_)
            | tungstenite::Message::Binary(_)
            | tungstenite::Message::Frame(_) => {}
        }
    }
}

pub(super) async fn read_close_code_without_answering_ping(
    websocket: &mut TestWebSocket,
) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            tungstenite::Message::Close(frame) => return frame.map(|frame| frame.code),
            tungstenite::Message::Ping(_)
            | tungstenite::Message::Pong(_)
            | tungstenite::Message::Text(_)
            | tungstenite::Message::Binary(_)
            | tungstenite::Message::Frame(_) => {}
        }
    }
}
