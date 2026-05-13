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
pub(super) use tower::util::ServiceExt;

pub(super) use super::super::app;
use crate::config::RoomShardingPolicy;
pub(super) use crate::{
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TelemetryConfig, TransportConfig,
        UserConfig, VideoBitrateLimits,
    },
    runtime::{
        ConnectionId, RoomPacketSinkRegistry, RuntimeState,
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
        metrics::{
            MetricName, RuntimeMetrics, RuntimeMetricsSnapshot,
            test_support::RuntimeMetricsSnapshotLookup,
        },
        room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            JoinUserRequest, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomManagerConfig,
            RoomManagerDeps, RoomRuntimePolicy, UserOutboundReceiver, UserOutboundSender,
            rtp_capabilities,
        },
    },
};

#[derive(Default)]
pub(super) struct DurationHistogramSnapshot {
    pub(super) count: u64,
}

pub(super) struct HttpInflightSnapshot {
    pub(super) metrics: i64,
}

pub(super) struct HttpRequestDurationSnapshot {
    pub(super) noop: DurationHistogramSnapshot,
    pub(super) metrics: DurationHistogramSnapshot,
}

pub(super) trait RuntimeMetricsSnapshotTestExt: RuntimeMetricsSnapshotLookup {
    fn http_noop_requests(&self) -> u64 {
        self.counter_value(MetricName::HttpNoopRequestsTotal, &[])
    }

    fn http_room_requests(&self) -> u64 {
        self.counter_value(MetricName::HttpRoomRequestsTotal, &[])
    }

    fn http_room_unauthorized(&self) -> u64 {
        self.counter_value(
            MetricName::HttpRoomResponsesTotal,
            &[("status", "unauthorized")],
        )
    }

    fn http_room_success(&self) -> u64 {
        self.counter_value(MetricName::HttpRoomResponsesTotal, &[("status", "success")])
    }

    fn http_disconnect_requests(&self) -> u64 {
        self.counter_value(MetricName::HttpDisconnectRequestsTotal, &[])
    }

    fn http_disconnect_success(&self) -> u64 {
        self.counter_value(
            MetricName::HttpDisconnectResponsesTotal,
            &[("status", "success")],
        )
    }

    fn http_disconnect_bad_request(&self) -> u64 {
        self.counter_value(
            MetricName::HttpDisconnectResponsesTotal,
            &[("status", "bad_request")],
        )
    }

    fn http_disconnect_unprocessable_entity(&self) -> u64 {
        self.counter_value(
            MetricName::HttpDisconnectResponsesTotal,
            &[("status", "unprocessable_entity")],
        )
    }

    fn http_metrics_requests(&self) -> u64 {
        self.counter_value(MetricName::HttpMetricsRequestsTotal, &[])
    }

    fn http_inflight(&self) -> HttpInflightSnapshot {
        HttpInflightSnapshot {
            metrics: self.gauge_value(MetricName::HttpInflightRequests, &[("route", "metrics")]),
        }
    }

    fn http_request_duration(&self) -> HttpRequestDurationSnapshot {
        HttpRequestDurationSnapshot {
            noop: DurationHistogramSnapshot {
                count: self.histogram_count_value(
                    MetricName::HttpRequestDurationSeconds,
                    &[("route", "noop")],
                ),
            },
            metrics: DurationHistogramSnapshot {
                count: self.histogram_count_value(
                    MetricName::HttpRequestDurationSeconds,
                    &[("route", "metrics")],
                ),
            },
        }
    }

    fn ws_handshake_duration(&self) -> DurationHistogramSnapshot {
        DurationHistogramSnapshot {
            count: self.histogram_count_value(MetricName::WsHandshakeDurationSeconds, &[]),
        }
    }

    fn active_rooms(&self) -> i64 {
        self.gauge_value(MetricName::RoomsActive, &[])
    }

    fn active_users(&self) -> i64 {
        self.gauge_value(MetricName::UsersActive, &[])
    }

    fn active_publications(&self) -> i64 {
        self.gauge_value(MetricName::PublicationsActive, &[])
    }

    fn active_subscriptions(&self) -> i64 {
        self.gauge_value(MetricName::SubscriptionsActive, &[])
    }

    fn active_recording_rooms(&self) -> i64 {
        self.gauge_value(MetricName::RecordingRoomsActive, &[])
    }

    fn active_transport_users(&self) -> i64 {
        self.gauge_value(MetricName::TransportUsersActive, &[])
    }

    fn connected_transport_users(&self) -> i64 {
        self.gauge_value(MetricName::TransportHealthUsers, &[("state", "connected")])
    }

    fn transport_health_transitions_unset_to_connected(&self) -> u64 {
        self.counter_value(
            MetricName::TransportHealthTransitionsTotal,
            &[("from", "unset"), ("to", "connected")],
        )
    }

    fn transport_ice_state_changes_checking(&self) -> u64 {
        self.counter_value(
            MetricName::TransportIceStateChangesTotal,
            &[("state", "checking")],
        )
    }

    fn transport_dtls_connected(&self) -> u64 {
        self.counter_value(MetricName::TransportDtlsConnectedTotal, &[])
    }

    fn transport_user_lifetime_count(&self) -> u64 {
        self.histogram_count_value(MetricName::TransportUserLifetimeSeconds, &[])
    }

    fn recording_start_accepted(&self) -> u64 {
        self.counter_value(
            MetricName::RecordingActionsTotal,
            &[("action", "start"), ("outcome", "accepted")],
        )
    }

    fn rtp_forwarded_packets_local_rtc(&self) -> u64 {
        self.counter_value(
            MetricName::RtpForwardedPacketsTotal,
            &[("destination", "local_rtc")],
        )
    }
}

impl RuntimeMetricsSnapshotTestExt for RuntimeMetricsSnapshot {}

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
            max_pre_auth_websocket_sessions: 512,
            max_pre_auth_websocket_sessions_per_origin: 16,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8070)),
            trust_proxy_headers: false,
        },
        user: UserConfig {
            room_size: 100,
            timeout_ms: 10_000,
            ping_interval_ms: 60_000,
            outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        },
        transport: TransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
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
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            diagnostics: Arc::clone(&diagnostics),
            metrics: Arc::clone(&metrics),
        },
    ));
    let media_transport = MediaTransport::fake_for_testing();
    let state = RuntimeState::for_config_parts(
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

pub(super) fn test_outbound_sender(
    state: &RuntimeState,
) -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(
        state.config.user.outbound_queue_capacity,
        Arc::clone(&state.metrics),
    )
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
