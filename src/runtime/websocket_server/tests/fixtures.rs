pub(super) use std::net::{IpAddr, Ipv4Addr, SocketAddr};
pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;

pub(super) use futures_util::{SinkExt, StreamExt};
pub(super) use o_sfu_protocol::{
    shared::{AvailableFeatures, RecordingState, SessionId, SessionPermissions, StreamType},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, ClientResponse, EnvelopeBatch, RequestId,
        ServerEnvelope, ServerMessage, ServerRequest, SessionDescriptionPayload, WelcomePayload,
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
        Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
        TelemetryConfig,
    },
    runtime::{
        RuntimeState,
        auth::{RegisteredJwtClaims, WebSocketConnectClaims, sign},
        channel::Channel,
        channel::rtp_capabilities,
        channel::{
            ChannelAdmissionPolicy, ChannelConfig, ChannelManager, ChannelManagerConfig,
            ChannelRuntimePolicy,
        },
        diagnostics::DiagnosticsStore,
        http_server::app,
        metrics::RuntimeMetrics,
        recording::MediaTap,
        testing::decode_protocol_welcome_batch,
        transport_adapter::test_support::{FakeWebRtcAdapter, FakeWebRtcEvent},
        transport_adapter::{
            RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter, SessionBitrateLimits,
        },
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
    pub(super) channel_manager: Arc<ChannelManager>,
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
        trust_proxy_headers: false,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        diagnostics: DiagnosticsConfig::default(),
        telemetry: TelemetryConfig::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        max_bitrate_in_bps: 8_000_000,
        max_bitrate_out_bps: 10_000_000,
        rtc_media_worker_count: 1,
    }
}

pub(super) async fn spawn_test_server(
    authentication_timeout_ms: u64,
    channel_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
        RuntimeTransportAdapter::fake_for_testing(),
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
    spawn_test_server_impl(
        authentication_timeout_ms,
        session_timeout_ms,
        ping_interval_ms,
        channel_size,
        transport_adapter,
        RuntimeFeatureFlags::default(),
    )
    .await
}

async fn spawn_test_server_impl(
    authentication_timeout_ms: u64,
    session_timeout_ms: u64,
    ping_interval_ms: u64,
    channel_size: usize,
    transport_adapter: RuntimeTransportAdapter,
    feature_flags: RuntimeFeatureFlags,
) -> Option<TestServer> {
    let mut config = test_config(
        authentication_timeout_ms,
        session_timeout_ms,
        ping_interval_ms,
        channel_size,
    );
    config.feature_flags = feature_flags;
    let diagnostics = Arc::new(DiagnosticsStore::default());
    let metrics = Arc::new(RuntimeMetrics::default());
    let channel_manager = Arc::new(ChannelManager::new(
        ChannelManagerConfig::new(
            1,
            ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(config.channel_size),
                feature_flags,
                rtp_capabilities::router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::clone(&diagnostics),
        Arc::clone(&metrics),
    ));
    let state = RuntimeState {
        config,
        channel_manager: Arc::clone(&channel_manager),
        diagnostics,
        metrics,
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
        channel_manager,
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

pub(super) async fn spawn_protocol_test_server(
    authentication_timeout_ms: u64,
    channel_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
        RuntimeTransportAdapter::fake_for_testing(),
    )
    .await
}

pub(super) async fn spawn_test_server_with_feature_flags(
    authentication_timeout_ms: u64,
    channel_size: usize,
    transport_adapter: RuntimeTransportAdapter,
    feature_flags: RuntimeFeatureFlags,
) -> Option<TestServer> {
    spawn_test_server_impl(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
        transport_adapter,
        feature_flags,
    )
    .await
}

pub(super) fn build_real_rtc_transport_adapter() -> RuntimeTransportAdapter {
    RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        SessionBitrateLimits::new(8_000_000, 10_000_000),
        RtcPortRange::new(47_200, 47_299),
        1,
        MediaCodecFlags::default(),
        Arc::new(DiagnosticsStore::default()),
        Arc::new(MediaTap::default()),
        Arc::new(RuntimeMetrics::default()),
    ))
}

pub(super) async fn spawn_protocol_rtc_test_server(
    authentication_timeout_ms: u64,
    channel_size: usize,
) -> Option<TestServer> {
    spawn_test_server_with_timeouts(
        authentication_timeout_ms,
        10_000,
        60_000,
        channel_size,
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

pub(super) fn signed_connect_claims(
    key: &str,
    channel_uuid: &str,
    session_id: SessionId,
) -> Option<String> {
    signed_connect_claims_with_permissions(key, channel_uuid, session_id, None)
}

pub(super) fn signed_connect_claims_with_permissions(
    key: &str,
    channel_uuid: &str,
    session_id: SessionId,
    permissions: Option<SessionPermissions>,
) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            sfu_channel_uuid: channel_uuid.to_owned(),
            session_id,
            label: Some("Alice".to_owned()),
            permissions,
        },
        key,
    )
    .ok()
}

pub(super) fn signed_legacy_channel_scoped_connect_claims(
    key: &str,
    session_id: SessionId,
    permissions: Option<SessionPermissions>,
) -> Option<String> {
    #[derive(serde::Serialize)]
    struct LegacyClaims {
        #[serde(flatten)]
        registered: RegisteredJwtClaims,
        #[serde(rename = "session_id")]
        session_id: SessionId,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        permissions: Option<SessionPermissions>,
    }

    sign(
        &LegacyClaims {
            registered: RegisteredJwtClaims::default(),
            session_id,
            label: Some("Alice".to_owned()),
            permissions,
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
        .channel_manager
        .serve_channel(issuer, key, &config, None)
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

pub(super) async fn authenticate_with_channel(
    server: &TestServer,
    token: &str,
    channel_uuid: Option<&str>,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_protocol_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: channel_uuid.map(str::to_owned),
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
    channel: &Arc<Channel>,
    session_id: SessionId,
) -> Option<TestWebSocket> {
    let token = signed_connect_claims(TEST_AUTH_KEY, channel.uuid(), session_id)?;
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
        }),
        ServerRequest::Renegotiate(_) => ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp: sdp.to_owned(),
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
