pub(super) use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

pub(super) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header, request::Builder as HttpRequestBuilder},
    response::Response as AxumResponse,
};
pub(super) use o_sfu_protocol::shared::UserId;
pub(super) use serde::de::DeserializeOwned;
pub(super) use tokio::sync::mpsc;
pub(super) use tower::util::ServiceExt;

pub(super) use super::super::app;
use crate::config::RoomShardingPolicy;
pub(super) use crate::{
    config::{
        AuthConfig, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TelemetryConfig, TransportConfig,
        UserConfig, VideoBitrateLimits,
    },
    runtime::{
        ConnectionId, RuntimeState,
        auth::{self, HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims},
        diagnostics::{
            DiagnosticsStore,
            types::{
                DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSourceSelectionReason,
                DiagnosticsSummaryResponse, DiagnosticsUserDetail, DiagnosticsUserLookupConflict,
                DiagnosticsUserSummary,
            },
        },
        http_server::contract::{
            CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
            DISCONNECT_PATH, METRICS_PATH, NOOP_PATH, NoopResponse, RoomResponse, STATS_PATH,
            StatsResponse,
        },
        media_transport::MediaTransport,
        metrics::RuntimeMetrics,
        recording::MediaTap,
        room::{
            JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerConfig,
            RoomManagerDeps, RoomRuntimePolicy, rtp_capabilities,
        },
        testing::build_test_runtime_state,
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

pub(super) struct TestRuntimeState {
    pub(super) state: RuntimeState,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
}

pub(super) fn test_config() -> Config {
    Config {
        auth: AuthConfig {
            key: TEST_AUTH_KEY.to_owned(),
            authentication_timeout_ms: 10_000,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8070)),
            trust_proxy_headers: false,
        },
        user: UserConfig {
            room_size: 100,
            timeout_ms: 10_000,
            ping_interval_ms: 60_000,
        },
        transport: TransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            max_bitrate_in_bps: 8_000_000,
            max_bitrate_out_bps: 10_000_000,
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_media_worker_count: 1,
            room_sharding_policy: RoomShardingPolicy::strict_single_router(),
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

pub(super) fn test_state() -> RuntimeState {
    test_state_with_handles().state
}

pub(super) fn test_state_with_handles() -> TestRuntimeState {
    let config = test_config();
    let diagnostics = Arc::new(DiagnosticsStore::default());
    let metrics = Arc::new(RuntimeMetrics::default());
    let room_manager = Arc::new(RoomManager::new(
        RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(config.user.room_size),
                config.features,
                rtp_capabilities::router_rtp_capabilities_with_preferences(
                    config.codecs.flags,
                    config.codecs.preferences,
                ),
            )
            .with_room_sharding_policy(config.transport.room_sharding_policy),
        ),
        RoomManagerDeps {
            recording_media_tap: Arc::new(MediaTap::default()),
            diagnostics: Arc::clone(&diagnostics),
            metrics: Arc::clone(&metrics),
        },
    ));
    let media_transport = MediaTransport::fake_for_testing();
    let state = build_test_runtime_state(
        &config,
        Arc::clone(&room_manager),
        diagnostics,
        metrics,
        media_transport.clone(),
    );
    TestRuntimeState {
        state,
        room_manager,
        media_transport,
    }
}

pub(super) fn signed_room_claims(issuer: Option<&str>, key: Option<&str>) -> Option<String> {
    auth::sign(
        &HttpRoomClaims {
            registered: RegisteredJwtClaims {
                iss: issuer.map(str::to_owned),
                ..RegisteredJwtClaims::default()
            },
            key: key.map(str::to_owned),
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub(super) fn signed_disconnect_claims(
    user_ids_by_room: BTreeMap<String, Vec<UserId>>,
) -> Option<String> {
    auth::sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            user_ids_by_room,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub(super) fn build_request(builder: HttpRequestBuilder, body: Body) -> Option<Request<Body>> {
    builder.body(body).ok()
}

pub(super) async fn parse_json<T>(response: AxumResponse) -> Option<T>
where
    T: DeserializeOwned,
{
    let bytes = to_bytes(response.into_body(), usize::MAX).await.ok()?;
    serde_json::from_slice::<T>(&bytes).ok()
}

pub(super) async fn parse_text(response: AxumResponse) -> Option<String> {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.ok()?;
    String::from_utf8(bytes.to_vec()).ok()
}
