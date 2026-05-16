use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use crate::{
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RoomWorkerPolicy, RtcPortRange, RuntimeFeatureFlags, TelemetryConfig,
        TransportConfig, UserConfig, VideoBitrateLimits,
    },
    runtime::{
        DiagnosticsStore, MediaTransport, RoomPacketSinkRegistry, RuntimeMetrics, RuntimeState,
        metrics::{MetricName, RuntimeMetricsSnapshot, test_support::RuntimeMetricsSnapshotLookup},
        room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomManagerDeps,
            RoomRuntimePolicy, UserOutboundReceiver, UserOutboundSender, rtp_capabilities,
        },
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

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

    fn ws_connections_accepted(&self) -> u64 {
        self.counter_value(MetricName::WsConnectionsTotal, &[("stage", "accepted")])
    }

    fn ws_handshake_credentials_received(&self) -> u64 {
        self.counter_value(
            MetricName::WsConnectionsTotal,
            &[("stage", "credentials_received")],
        )
    }

    fn ws_users_joined(&self) -> u64 {
        self.counter_value(MetricName::WsConnectionsTotal, &[("stage", "joined")])
    }

    fn ws_handshake_rejected_timeout(&self) -> u64 {
        self.counter_value(
            MetricName::WsHandshakeRejectionsTotal,
            &[("close_code", "auth_timeout")],
        )
    }

    fn ws_handshake_rejected_protocol_error(&self) -> u64 {
        self.counter_value(
            MetricName::WsHandshakeRejectionsTotal,
            &[("close_code", "protocol_error")],
        )
    }

    fn ws_handshake_rejected_error(&self) -> u64 {
        self.counter_value(
            MetricName::WsHandshakeRejectionsTotal,
            &[("close_code", "error")],
        )
    }

    fn ws_user_loops_started(&self) -> u64 {
        self.counter_value(MetricName::WsUserLoopsStartedTotal, &[])
    }

    fn ws_user_loop_exits_ping_timeout(&self) -> u64 {
        self.counter_value(
            MetricName::WsUserLoopExitsTotal,
            &[("reason", "ping_timeout")],
        )
    }

    fn ws_bus_parse_failures(&self) -> u64 {
        self.counter_value(MetricName::WsBusParseFailuresTotal, &[])
    }

    fn ws_bus_invalid_input_failures(&self) -> u64 {
        self.counter_value(MetricName::WsBusFailuresTotal, &[("kind", "invalid_input")])
    }

    fn ws_bus_unsupported_feature_failures(&self) -> u64 {
        self.counter_value(
            MetricName::WsBusFailuresTotal,
            &[("kind", "unsupported_feature")],
        )
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

pub(super) struct RuntimeTestState {
    pub(super) state: RuntimeState,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
}

pub(super) struct RuntimeTestBuilder {
    config: Config,
    media_transport: MediaTransport,
}

impl RuntimeTestBuilder {
    pub(super) fn new() -> Self {
        Self {
            config: Config {
                auth: AuthConfig {
                    key: TEST_AUTH_KEY.to_owned(),
                    authentication_timeout_ms: 10_000,
                    max_pre_auth_websocket_sessions: 512,
                    max_pre_auth_websocket_sessions_per_origin: 16,
                },
                http: HttpConfig {
                    bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
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
                    room_worker_policy: RoomWorkerPolicy::strict_single_router(),
                },
                codecs: CodecConfig {
                    flags: MediaCodecFlags::default(),
                    preferences: CodecPreferences::default(),
                },
                features: RuntimeFeatureFlags::default(),
                telemetry: TelemetryConfig::default(),
                diagnostics: DiagnosticsConfig::default(),
            },
            media_transport: MediaTransport::fake_for_testing(),
        }
    }

    pub(super) const fn config(&self) -> &Config {
        &self.config
    }

    pub(super) fn authentication_timeout_ms(mut self, value: u64) -> Self {
        self.config.auth.authentication_timeout_ms = value;
        self
    }

    pub(super) fn user_timeout_ms(mut self, value: u64) -> Self {
        self.config.user.timeout_ms = value;
        self
    }

    pub(super) fn ping_interval_ms(mut self, value: u64) -> Self {
        self.config.user.ping_interval_ms = value;
        self
    }

    pub(super) fn room_size(mut self, value: usize) -> Self {
        self.config.user.room_size = value;
        self
    }

    pub(super) fn pre_auth_capacity(mut self, total: usize, per_origin: usize) -> Self {
        self.config.auth.max_pre_auth_websocket_sessions = total;
        self.config.auth.max_pre_auth_websocket_sessions_per_origin = per_origin;
        self
    }

    pub(super) fn trust_proxy_headers(mut self, value: bool) -> Self {
        self.config.http.trust_proxy_headers = value;
        self
    }

    pub(super) fn feature_flags(mut self, value: RuntimeFeatureFlags) -> Self {
        self.config.features = value;
        self
    }

    pub(super) fn media_transport(mut self, value: MediaTransport) -> Self {
        self.media_transport = value;
        self
    }

    pub(super) fn build_state(self) -> RuntimeTestState {
        let diagnostics = Arc::new(DiagnosticsStore::default());
        let metrics = Arc::new(RuntimeMetrics::default());
        let room_manager = Arc::new(RoomManager::new(
            RoomManagerConfig::new(
                self.config.transport.rtc_media_worker_count,
                RoomRuntimePolicy::new(
                    RoomAdmissionPolicy::new(self.config.user.room_size),
                    self.config.features,
                    rtp_capabilities::router_rtp_capabilities_with_preferences(
                        self.config.codecs.flags,
                        self.config.codecs.preferences,
                    ),
                )
                .with_room_worker_policy(self.config.transport.room_worker_policy),
            ),
            RoomManagerDeps {
                packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
                diagnostics: Arc::clone(&diagnostics),
                metrics: Arc::clone(&metrics),
            },
        ));
        let state = RuntimeState::for_config_parts(
            &self.config,
            Arc::clone(&room_manager),
            diagnostics,
            metrics,
            self.media_transport.clone(),
        );
        RuntimeTestState {
            state,
            room_manager,
            media_transport: self.media_transport,
        }
    }

    pub(super) fn build_runtime_state(self) -> RuntimeState {
        self.build_state().state
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
