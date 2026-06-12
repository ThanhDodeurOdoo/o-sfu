#![allow(
    dead_code,
    reason = "shared integration-test support is compiled by multiple test targets, each of which uses only a subset of the helpers"
)]

use std::{
    collections::BTreeMap,
    fmt::Display,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    result::Result as StdResult,
    time::Duration,
};

use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use o_sfu::{
    Runtime,
    auth::{
        HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims, WebSocketConnectClaims, sign,
    },
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend,
        RuntimeFeatureFlags, TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
    },
    core::server::room::{
        DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    },
    http::{
        CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DISCONNECT_PATH, METRICS_PATH,
        RoomResponse, STATS_PATH, StatsResponse,
    },
};
use o_sfu_core::server::transport::test_support::test_rtc_port_range;
use o_sfu_protocol::wire::{
    EnvelopeBatch, ServerEnvelope, ServerMessage, StreamType, UserId, UserPermissions,
    WelcomePayload,
};
use o_sfu_telemetry::diagnostics::{
    DiagnosticsActiveSpeaker, DiagnosticsActiveSpeakerReason, DiagnosticsActiveSpeakerState,
    DiagnosticsRoomDetail, DiagnosticsRouteState, DiagnosticsVideoLayoutRole,
};
use reqwest::StatusCode;
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, yield_now},
    time::timeout,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result as WebSocketResult, protocol::frame::coding::CloseCode},
};

pub type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

pub const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";

pub type TestResult<T = ()> = Result<T>;

/// Convert a required optional test fixture value into a contextual test error.
///
/// # Errors
///
/// Returns an error when the required value is absent.
pub fn require_some<T>(value: Option<T>, context: &'static str) -> Result<T> {
    value.ok_or_else(|| anyhow!(context))
}

/// Convert a required fallible test fixture value into a contextual test error.
///
/// # Errors
///
/// Returns an error when the wrapped result is an error.
pub fn require_ok<T, E>(value: StdResult<T, E>, context: &'static str) -> Result<T>
where
    E: Display,
{
    value.map_err(|error| anyhow!("{context}: {error}"))
}

/// Test-only server handle used by integration tests to exercise the real HTTP and WS entry points.
#[derive(Debug)]
pub struct TestServer {
    addr: SocketAddr,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
pub struct TestRoomServer {
    server: TestServer,
    room_id: String,
}

const TEST_POLL_DEADLINE: Duration = Duration::from_secs(5);
const FEATURED_POLICY_ROLE: &str = "featured";
const THUMBNAIL_POLICY_ROLE: &str = "thumbnail";

impl TestServer {
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}/", self.addr)
    }

    #[must_use]
    pub fn http_base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn wait_for_room_absence(&self, room_id: &str) -> bool {
        wait_for_test_predicate(|| async { self.room_absent(room_id).await.then_some(()) }).await
    }

    pub async fn wait_for_consumer_route_active(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            ExpectedRouteState::Active,
        )
        .await
    }

    pub async fn wait_for_consumer_route_inactive(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            ExpectedRouteState::Inactive,
        )
        .await
    }

    pub async fn wait_for_consumer_route_absence(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            ExpectedRouteState::Absent,
        )
        .await
    }

    pub async fn wait_for_audio_source_active_speaker(
        &self,
        room_id: &str,
        owner_user_id: &UserId,
        expected_state: DiagnosticsActiveSpeakerState,
        expected_reason: DiagnosticsActiveSpeakerReason,
        expected_last_audio_level_dbov: Option<i8>,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            let active_speaker = audio_source_active_speaker(&room, owner_user_id)?;
            (active_speaker.state == expected_state
                && active_speaker.reason == expected_reason
                && active_speaker.last_audio_level_dbov == expected_last_audio_level_dbov)
                .then_some(())
        })
        .await
    }

    pub async fn wait_for_video_subscription_selected_rid(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        expected_rid: &str,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            let selected_rid =
                video_subscription_selected_rid(&room, consumer_user_id, producer_user_id)?;
            (selected_rid == expected_rid).then_some(())
        })
        .await
    }

    pub async fn wait_for_user_media_worker(
        &self,
        room_id: &str,
        user_id: &UserId,
        expected_media_worker_id: usize,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            (user_media_worker_id(&room, user_id) == Some(expected_media_worker_id)).then_some(())
        })
        .await
    }

    async fn wait_for_consumer_route_state(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
        expected_state: ExpectedRouteState,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_detail(room_id).await?;
            expected_state
                .matches(route_state(
                    &room,
                    consumer_user_id,
                    producer_user_id,
                    stream_type,
                ))
                .then_some(())
        })
        .await
    }

    async fn room_absent(&self, room_id: &str) -> bool {
        reqwest::Client::new()
            .get(format!(
                "{}{DIAGNOSTICS_ROOMS_PATH}/{room_id}",
                self.http_base_url()
            ))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::NOT_FOUND)
    }

    async fn room_detail(&self, room_id: &str) -> Option<DiagnosticsRoomDetail> {
        let response = reqwest::Client::new()
            .get(format!(
                "{}{DIAGNOSTICS_ROOMS_PATH}/{room_id}",
                self.http_base_url()
            ))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json::<DiagnosticsRoomDetail>().await.ok()
    }
}

fn audio_source_active_speaker<'room>(
    room: &'room DiagnosticsRoomDetail,
    owner_user_id: &UserId,
) -> Option<&'room DiagnosticsActiveSpeaker> {
    room.sources
        .iter()
        .find(|source| {
            source.owner_user_id == *owner_user_id
                && source.stream_id == stream_id_for_stream_type(StreamType::Audio)
        })?
        .active_speaker
        .as_ref()
}

fn user_media_worker_id(room: &DiagnosticsRoomDetail, user_id: &UserId) -> Option<usize> {
    room.users
        .iter()
        .find(|user| user.user_id == *user_id)
        .map(|user| user.transport.media_worker_id)
}

fn video_subscription_selected_rid<'room>(
    room: &'room DiagnosticsRoomDetail,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
) -> Option<&'room str> {
    let subscription = room
        .users
        .iter()
        .find(|user| user.user_id == *consumer_user_id)?
        .subscriptions
        .iter()
        .find(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_stream_type(StreamType::Camera)
        })?;

    if let Some(selected_rid) = subscription.selection.selected_rid.as_deref() {
        return Some(selected_rid);
    }

    let policy_role = policy_role_for_layout_role(subscription.layout_role?)?;
    room.sources
        .iter()
        .find(|source| source.source_id == subscription.source_id)?
        .encodings
        .iter()
        .find(|encoding| encoding.policy_role.as_deref() == Some(policy_role))?
        .rid
        .as_deref()
}

fn policy_role_for_layout_role(layout_role: DiagnosticsVideoLayoutRole) -> Option<&'static str> {
    match layout_role {
        DiagnosticsVideoLayoutRole::Pinned
        | DiagnosticsVideoLayoutRole::Featured
        | DiagnosticsVideoLayoutRole::ReadableDetail
        | DiagnosticsVideoLayoutRole::ActiveSpeaker => Some(FEATURED_POLICY_ROLE),
        DiagnosticsVideoLayoutRole::VisibleThumbnail => Some(THUMBNAIL_POLICY_ROLE),
        DiagnosticsVideoLayoutRole::Hidden | DiagnosticsVideoLayoutRole::Overflow => None,
    }
}

impl TestRoomServer {
    #[must_use]
    pub fn into_parts(self) -> (TestServer, String) {
        (self.server, self.room_id)
    }
}

#[derive(Debug, Clone, Copy)]
enum ExpectedRouteState {
    Active,
    Inactive,
    Absent,
}

impl ExpectedRouteState {
    fn matches(self, actual: Option<&DiagnosticsRouteState>) -> bool {
        match self {
            Self::Active => actual == Some(&DiagnosticsRouteState::Active),
            Self::Inactive => actual == Some(&DiagnosticsRouteState::Inactive),
            Self::Absent => actual.is_none(),
        }
    }
}

fn route_state<'room>(
    room: &'room DiagnosticsRoomDetail,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: StreamType,
) -> Option<&'room DiagnosticsRouteState> {
    room.users
        .iter()
        .find(|user| user.user_id == *consumer_user_id)?
        .subscriptions
        .iter()
        .find(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_stream_type(stream_type)
        })
        .map(|subscription| &subscription.state)
}

fn stream_id_for_stream_type(stream_type: StreamType) -> &'static str {
    match stream_type {
        StreamType::Audio => "audio",
        StreamType::Camera => "camera",
        StreamType::Screen => "screen",
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spawn the real axum server on an ephemeral port for integration tests.
///
/// # Errors
///
/// Returns an error when the runtime cannot be constructed, the test listener
/// cannot bind, or the local socket address cannot be read.
///
/// # Panics
///
/// Panics in the spawned server task if the production listener exits with an
/// error instead of being aborted by test cleanup.
pub async fn spawn_test_server(config: Config) -> Result<TestServer> {
    let bind_address = config.http.bind_address;
    let runtime = Runtime::new(&config)?;
    let listener = TcpListener::bind(bind_address).await?;
    let addr = listener
        .local_addr()
        .map_err(|error| anyhow!("failed to read test listener address: {error}"))?;
    let handle = tokio::spawn(async move {
        let result = runtime.serve_listener(listener).await;
        assert!(
            result.is_ok(),
            "test server should stop cleanly: {result:?}"
        );
    });
    Ok(TestServer { addr, handle })
}

pub async fn spawn_room_server(issuer: &str) -> Option<TestRoomServer> {
    spawn_room_server_with_config(test_config(1_000, 10), issuer, TEST_ROOM_KEY).await
}

pub async fn spawn_room_server_with_config(
    config: Config,
    issuer: &str,
    key: &str,
) -> Option<TestRoomServer> {
    let server = spawn_test_server(config).await.ok()?;
    let room_id = create_room(&server, issuer, key).await?;
    Some(TestRoomServer { server, room_id })
}

#[must_use]
pub fn test_config(authentication_timeout_ms: u64, room_size: usize) -> Config {
    Config {
        auth: AuthConfig {
            key: TEST_AUTH_KEY.to_owned(),
            authentication_timeout_ms,
            max_pre_auth_websocket_sessions: 512,
            max_pre_auth_websocket_sessions_per_origin: 16,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            trust_proxy_headers: true,
        },
        user: UserConfig {
            room_size,
            timeout_ms: 10_000,
            ping_interval_ms: 60_000,
            outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        },
        transport: TransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: unique_rtc_port_range(1),
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
            rtc_media_worker_count: 1,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            room_media_limits: RoomMediaLimits::default(),
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

pub fn set_rtc_media_worker_count(config: &mut Config, worker_count: usize) {
    config.transport.rtc_media_worker_count = worker_count;
    config.transport.rtc_port_range = unique_rtc_port_range(worker_count);
}

fn unique_rtc_port_range(worker_count: usize) -> RtcPortRange {
    #![allow(
        clippy::panic,
        reason = "test configs cannot return Result and must fail loudly when no RTC ports are available"
    )]

    test_rtc_port_range(worker_count)
        .unwrap_or_else(|| panic!("test RTC port allocator could not reserve a contiguous range"))
}

#[must_use]
pub fn signed_connect_claims(key: &str, room_id: &str, user_id: UserId) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            room_id: room_id.to_owned(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions: Some(UserPermissions::default()),
        },
        key,
    )
    .ok()
}

#[must_use]
pub fn signed_room_claims(issuer: &str, key: &str) -> Option<String> {
    sign(
        &HttpRoomClaims {
            registered: RegisteredJwtClaims {
                iss: Some(issuer.to_owned()),
                ..RegisteredJwtClaims::default()
            },
            key: Some(key.to_owned()),
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

#[must_use]
pub fn signed_disconnect_claims(user_ids_by_room: BTreeMap<String, Vec<UserId>>) -> Option<String> {
    sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            user_ids_by_room,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub async fn create_room(server: &TestServer, issuer: &str, key: &str) -> Option<String> {
    let token = signed_room_claims(issuer, key)?;
    let response = reqwest::Client::new()
        .get(format!("{}{CHANNEL_PATH}", server.http_base_url()))
        .bearer_auth(token)
        .header("x-forwarded-for", "127.0.0.1")
        .query(&CreateRoomQuery::default())
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload = response.json::<RoomResponse>().await.ok()?;
    Some(payload.uuid)
}

pub async fn disconnect_sessions_via_http(
    server: &TestServer,
    user_ids_by_room: BTreeMap<String, Vec<UserId>>,
) -> Option<StatusCode> {
    let token = signed_disconnect_claims(user_ids_by_room)?;
    let response = reqwest::Client::new()
        .post(format!("{}{DISCONNECT_PATH}", server.http_base_url()))
        .body(token)
        .send()
        .await
        .ok()?;
    Some(response.status())
}

pub async fn metrics_text(server: &TestServer) -> Option<String> {
    let response = reqwest::Client::new()
        .get(format!("{}{METRICS_PATH}", server.http_base_url()))
        .send()
        .await
        .ok()?;
    response.text().await.ok()
}

pub async fn stats(server: &TestServer) -> Option<StatsResponse> {
    let response = reqwest::Client::new()
        .get(format!("{}{STATS_PATH}", server.http_base_url()))
        .send()
        .await
        .ok()?;
    response.json::<StatsResponse>().await.ok()
}

pub async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.ws_url()).await.ok()?;
    Some(websocket.0)
}

pub async fn read_message(websocket: &mut TestWebSocket) -> Option<WebSocketResult<Message>> {
    websocket.next().await
}

pub async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            Message::Text(payload) => return Some(payload.to_string()),
            Message::Ping(payload) => {
                websocket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) => {}
            Message::Binary(_) | Message::Close(_) | Message::Frame(_) => return None,
        }
    }
}

pub async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            Message::Close(frame) => {
                let code = frame.map(|frame| frame.code);
                let _ = websocket.close(None).await;
                return code;
            }
            Message::Ping(payload) => {
                websocket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) | Message::Text(_) | Message::Binary(_) | Message::Frame(_) => {}
        }
    }
}

#[must_use]
pub fn decode_protocol_welcome_batch(payload: &str) -> Option<WelcomePayload> {
    let batch = serde_json::from_str::<EnvelopeBatch>(payload).ok()?;
    let envelope = batch.first()?.clone();
    match ServerEnvelope::decode(envelope).ok()? {
        ServerEnvelope::Message(ServerMessage::Welcome(welcome)) => Some(welcome),
        ServerEnvelope::Message(_)
        | ServerEnvelope::Request { .. }
        | ServerEnvelope::Response { .. } => None,
    }
}

async fn wait_for_test_predicate<F, Fut>(mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<()>>,
{
    timeout(TEST_POLL_DEADLINE, async {
        loop {
            if predicate().await.is_some() {
                return Some(());
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
    .is_some()
}
