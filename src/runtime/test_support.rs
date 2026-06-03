use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

pub(super) use crate::runtime::metrics::test_support::RuntimeMetricsSnapshotTestExt;
use crate::{
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RuntimeFeatureFlags,
        TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
    },
    runtime::{
        DiagnosticsStore, MediaTransport, RoomPacketSinkRegistry, RuntimeMetrics, RuntimeState,
        media_transport::{
            MediaTransportConfig, MediaTransportDeps, SessionBitrateLimits,
            test_support::test_rtc_port_range,
        },
        room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomManagerDeps,
            RoomRuntimePolicy, UserOutboundReceiver, UserOutboundSender, rtp_capabilities,
        },
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub(super) const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";

pub(super) struct RuntimeTestState {
    pub(super) state: RuntimeState,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) media_transport: MediaTransport,
}

pub(super) struct RuntimeTestBuilder {
    config: Config,
    media_transport: Option<MediaTransport>,
}

impl RuntimeTestBuilder {
    pub(super) fn new() -> Self {
        let rtc_port_range = next_runtime_test_rtc_port_range();
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
                    rtc_port_range,
                    max_bitrate_in: Bitrate::from_mbps(8),
                    max_bitrate_out: Bitrate::from_mbps(10),
                    video_bitrate_limits: VideoBitrateLimits::default(),
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
            },
            media_transport: None,
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
        self.media_transport = Some(value);
        self
    }

    pub(super) fn build_state(self) -> RuntimeTestState {
        let diagnostics = Arc::new(DiagnosticsStore::default());
        let metrics = Arc::new(RuntimeMetrics::default());
        let packet_sink_registry = Arc::new(RoomPacketSinkRegistry::default());
        let media_transport = self.media_transport.unwrap_or_else(|| {
            build_real_media_transport_for_test_config(
                &self.config,
                Arc::clone(&diagnostics),
                Arc::clone(&metrics),
                Arc::clone(&packet_sink_registry),
            )
        });
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
                packet_sink_registry,
                diagnostics: Arc::clone(&diagnostics),
                metrics: Arc::clone(&metrics),
            },
        ));
        let state = RuntimeState::for_config_parts(
            &self.config,
            Arc::clone(&room_manager),
            diagnostics,
            metrics,
            media_transport.clone(),
        );
        RuntimeTestState {
            state,
            room_manager,
            media_transport,
        }
    }

    pub(super) fn build_runtime_state(self) -> RuntimeState {
        self.build_state().state
    }
}

#[allow(
    clippy::panic,
    reason = "runtime test fixtures need a real UDP range and should fail loudly when the host cannot provide one"
)]
fn next_runtime_test_rtc_port_range() -> RtcPortRange {
    test_rtc_port_range(1).unwrap_or_else(|| panic!("runtime test RTC ports should be available"))
}

#[allow(
    clippy::panic,
    reason = "runtime tests use validated in-process RTC fixtures and should fail loudly if construction becomes invalid"
)]
fn build_real_media_transport_for_test_config(
    config: &Config,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
) -> MediaTransport {
    match MediaTransport::builder()
        .transport_config(MediaTransportConfig {
            public_ip: config.transport.public_ip,
            bitrate_limits: SessionBitrateLimits::new(
                config.transport.max_bitrate_in,
                config.transport.max_bitrate_out,
            ),
            video_bitrate_limits: config.transport.video_bitrate_limits,
            rtc_port_range: config.transport.rtc_port_range,
            codec_flags: config.codecs.flags,
            codec_preferences: config.codecs.preferences,
            media_quality_interval: config.telemetry.media_quality_interval,
        })
        .deps(MediaTransportDeps {
            diagnostics,
            packet_sink_registry,
            metrics,
        })
        .worker_count(config.transport.rtc_media_worker_count)
        .build()
    {
        Ok(transport) => transport,
        Err(error) => panic!("runtime test RTC transport config should be valid: {error}"),
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
