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
pub(super) use o_sfu_protocol::shared::{UserId, UserPermissions};
pub(super) use serde::de::DeserializeOwned;
pub(super) use tokio::sync::mpsc;
pub(super) use tower::util::ServiceExt;

pub(super) use super::super::app;
pub(super) use crate::{
    application::rooms::Room as ApplicationRoom,
    config::{
        Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
        TelemetryConfig,
    },
    runtime::{
        ConnectionId, RuntimeState,
        auth::{self, HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims},
        diagnostics::{
            DiagnosticsStore,
            types::{
                DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSourceSelectionReason,
                DiagnosticsSummaryResponse, DiagnosticsUserDetail, DiagnosticsUserLookupConflict,
            },
        },
        http_server::contract::{
            CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
            DISCONNECT_PATH, METRICS_PATH, NOOP_PATH, NoopResponse, RoomResponse, STATS_PATH,
            StatsResponse,
        },
        metrics::RuntimeMetrics,
        recording::MediaTap,
        room::{
            RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerConfig, RoomRuntimePolicy,
            rtp_capabilities,
        },
        transport_adapter::RuntimeTransportAdapter,
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

pub(super) fn test_config() -> Config {
    Config {
        auth_key: TEST_AUTH_KEY.to_owned(),
        bind_address: SocketAddr::from(([127, 0, 0, 1], 8070)),
        authentication_timeout_ms: 10_000,
        room_size: 100,
        diagnostics: DiagnosticsConfig::default(),
        user_timeout_ms: 10_000,
        ping_interval_ms: 60_000,
        trust_proxy_headers: false,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        telemetry: TelemetryConfig::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        max_bitrate_in_bps: 8_000_000,
        max_bitrate_out_bps: 10_000_000,
        rtc_media_worker_count: 1,
    }
}

pub(super) fn test_state() -> RuntimeState {
    let config = test_config();
    let diagnostics = Arc::new(DiagnosticsStore::default());
    let metrics = Arc::new(RuntimeMetrics::default());
    let room_manager = Arc::new(RoomManager::new(
        RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(config.room_size),
                config.feature_flags,
                rtp_capabilities::router_rtp_capabilities(config.codec_flags),
            ),
        ),
        Arc::new(MediaTap::default()),
        Arc::clone(&diagnostics),
        Arc::clone(&metrics),
    ));
    let transport_adapter = RuntimeTransportAdapter::fake_for_testing();
    RuntimeState {
        rooms: ApplicationRoom::new(
            Arc::clone(&room_manager),
            Arc::clone(&diagnostics),
            transport_adapter.clone(),
        ),
        room_manager,
        config,
        metrics,
        transport_adapter,
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
