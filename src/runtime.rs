//! process-global orchestrator, wires subsystems together and manage server lifecycle
//!
//! runtime acts as the entry point for the server process, owning long-lived subsystems
//! like configuration, room management, metrics, and media transport. it ensures that
//! background tasks are synchronized with the control-plane server and provides guaranteed
//! cleanup through structured task management.
//!
//! ```text
//! Runtime
//! |- http_server          -> HTTP control-plane routes and server boot
//! |- websocket_server     -> WebSocket upgrade, auth handshake, and steady-state socket loop
//! |- core                 -> room engine, media transport, recording, metrics, and diagnostics
//! `- telemetry            -> tracing setup, schemas, diagnostics, metrics, and exporters
//! ```

use std::{future::Future, process, sync::Arc, time::Instant as StdInstant};

use anyhow::Result;
use tokio::{
    net::TcpListener,
    runtime::Builder,
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    config::Config,
    core::{CoreOptions, MediaCore, SfuCore},
};

pub(crate) mod auth;
pub(crate) mod diagnostics;
pub(crate) mod http_server;
pub(crate) mod options;
pub(crate) mod request_origin;
pub(crate) mod websocket_server;

pub(crate) use diagnostics::DiagnosticsStore;
use http_server::{serve_http, serve_http_on};
use media_transport::SourcePolicyPort;
pub(crate) use media_transport::{MediaTransport, MediaTransportDeps, ObservabilityPort};
pub(crate) use metrics::RuntimeMetrics;
pub(crate) use o_sfu_core::{
    ConnectionId, SessionBitrateLimits,
    server::{metrics, packet_sinks, room, transport as media_transport},
};
pub(crate) use o_sfu_telemetry as telemetry;
pub(crate) use o_sfu_telemetry::prometheus;
use options::{RuntimeConfig, RuntimeOptions};
pub(crate) use packet_sinks::RoomPacketSinkRegistry;
use room::{
    RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomManagerDeps, RoomRuntimePolicy,
    rtp_capabilities,
};
use telemetry::{init_tracing, schema::event as telemetry_event};

/// Process-global shell for the server process.
///
/// `Runtime` owns the long-lived subsystems that every request shares: configuration,
/// room allocation, metrics, and the media transport. Per-request entrypoints take
/// cheap clones of these dependencies through [`RuntimeState`].
#[derive(Debug)]
pub struct Runtime {
    config: RuntimeConfig,
    options: RuntimeOptions,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    media_transport: MediaTransport,
}

/// cheap-to-clone snapshot of runtime dependencies for per-request handlers
///
/// this is the standard shared state passed to axum handlers and websocket loops,
/// providing access to the room manager, diagnostics, and media core without
/// exposing the full runtime lifecycle.
#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    config: RuntimeConfig,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    media_transport: MediaTransport,
    media_core: MediaCore,
    metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeServices {
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
}

impl Default for RuntimeServices {
    fn default() -> Self {
        Self {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        }
    }
}

impl Runtime {
    /// Builds the process runtime from loaded configuration.
    ///
    /// this bootstraps the entire server instance, initializing telemetry,
    /// creating the room manager, and preparing the media transport workers.
    ///
    /// # Errors
    ///
    /// Returns an error when the media transport cannot be constructed from the
    /// configured RTC settings.
    pub fn new(config: &Config) -> Result<Self> {
        let runtime_config = RuntimeConfig::from_config(config);
        let options = RuntimeOptions::from_config(config);
        let services = RuntimeServices::default();
        let media_transport = build_media_transport(&options.core, &services)?;
        let room_runtime_policy = build_room_runtime_policy(&runtime_config, &options);
        info!("{}", config.log_view(process::id()));
        info!(
            event = telemetry_event::RUNTIME_BOOT,
            "runtime configuration loaded"
        );
        let room_manager = build_room_manager(&options, room_runtime_policy, &services);
        Ok(Self {
            config: runtime_config,
            options,
            room_manager,
            diagnostics: services.diagnostics,
            metrics: services.metrics,
            media_transport,
        })
    }

    /// Serves HTTP and WebSocket traffic on a caller-provided listener.
    ///
    /// This is the embedder-friendly sibling of [`run`]. It lets integration
    /// tests and external hosts bind an ephemeral port before handing the
    /// socket to the production runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when the Axum server fails while serving the supplied
    /// listener.
    pub async fn serve_listener(self, listener: TcpListener) -> Result<()> {
        let state = self.state();
        self.serve(|shutdown_token| serve_http_on(listener, state, shutdown_token))
            .await
    }

    /// default entrypoint for the production server loop
    async fn run_until_stopped(self) -> Result<()> {
        let state = self.state();
        self.serve(|shutdown_token| serve_http(state, shutdown_token))
            .await
    }

    /// core execution lifecycle manager
    ///
    /// orchestrates the relationship between the control plane (the http/websocket server)
    /// and the background workers. it ensures that background tasks are explicitly
    /// joined and cleaned up when the server stops,
    async fn serve<F, HttpServer>(self, http_server: F) -> Result<()>
    where
        F: FnOnce(CancellationToken) -> HttpServer,
        HttpServer: Future<Output = Result<()>>,
    {
        let tasks = RuntimeTasks::spawn(&self);
        let result = http_server(tasks.shutdown_token()).await;
        tasks.shutdown().await;
        result
    }

    fn state(&self) -> RuntimeState {
        RuntimeState::from_parts(
            &self.config,
            &self.options,
            Arc::clone(&self.room_manager),
            Arc::clone(&self.diagnostics),
            Arc::clone(&self.metrics),
            self.media_transport.clone(),
        )
    }
}

/// Owns runtime background tasks for the lifetime of one server future.
///
/// Normal shutdown asks tasks to exit through the shared cancellation token and
/// waits for them. Dropping the server future cancels the token and aborts any
/// remaining task so embedders cannot detach process-owned workers by cancelling
/// `Runtime::serve_listener`.
struct RuntimeTasks {
    shutdown_token: CancellationToken,
    source_packet_policy_sync: Option<JoinHandle<()>>,
}

impl RuntimeTasks {
    fn spawn(runtime: &Runtime) -> Self {
        let shutdown_token = CancellationToken::new();
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&runtime.room_manager),
            runtime.media_transport.clone(),
            runtime.media_transport.source_policy_subscription(),
            runtime.media_transport.clone(),
            shutdown_token.child_token(),
        );
        Self {
            shutdown_token,
            source_packet_policy_sync: Some(source_packet_policy_sync),
        }
    }

    /// provides a child token that will be cancelled when the runtime tasks stop
    fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.child_token()
    }

    /// signals background tasks to stop and waits for their completion
    async fn shutdown(mut self) {
        self.shutdown_token.cancel();
        if let Some(source_packet_policy_sync) = self.source_packet_policy_sync.take()
            && let Err(error) = source_packet_policy_sync.await
            && !error.is_cancelled()
        {
            warn!(
                ?error,
                "source packet policy update task stopped unexpectedly"
            );
        }
    }
}

impl Drop for RuntimeTasks {
    fn drop(&mut self) {
        // cancellation is idempotent, ensure tasks stop even if explicit shutdown was skipped
        self.shutdown_token.cancel();
        if let Some(source_packet_policy_sync) = self.source_packet_policy_sync.take() {
            source_packet_policy_sync.abort();
        }
    }
}

impl RuntimeState {
    fn from_parts(
        config: &RuntimeConfig,
        options: &RuntimeOptions,
        rooms: Arc<RoomManager>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
        media_transport: MediaTransport,
    ) -> Self {
        let media_core = SfuCore::new(options.core, media_transport.clone());
        Self {
            config: config.clone(),
            room_manager: rooms,
            diagnostics,
            media_transport,
            media_core,
            metrics,
        }
    }

    #[cfg(test)]
    fn for_config_parts(
        config: &Config,
        rooms: Arc<RoomManager>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
        media_transport: MediaTransport,
    ) -> Self {
        let runtime_config = RuntimeConfig::from_config(config);
        let options = RuntimeOptions::from_config(config);
        Self::from_parts(
            &runtime_config,
            &options,
            rooms,
            diagnostics,
            metrics,
            media_transport,
        )
    }
}

/// Room state decides which producer layers should remain routable from room-level
/// facts like membership and publication state, while the transport layer owns the
/// active-speaker observations that can change without any room mutation. This task
/// waits on explicit transport-side updates plus the current active-speaker expiry
/// deadline instead of polling the whole process on a fixed interval.
fn spawn_source_packet_policy_update_task(
    rooms: Arc<RoomManager>,
    observability_port: MediaTransport,
    updates: media_transport::SourcePolicyUpdateSubscription,
    media_port: MediaTransport,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    info!("booted source packet policy update task");
    tokio::spawn(async move {
        loop {
            let next_deadline = observability_port.next_active_speaker_deadline().await;
            let mut dirty_room_instance_ids = match next_deadline {
                Some(next_deadline) => {
                    tokio::select! {
                        biased;
                        () = shutdown_token.cancelled() => return,
                        dirty_room_instance_ids = updates.wait_for_update() => dirty_room_instance_ids,
                        () = time::sleep_until(Instant::from_std(next_deadline)) => {
                            observability_port
                                .expired_active_speaker_room_instance_ids(StdInstant::now())
                                .await
                        }
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        () = shutdown_token.cancelled() => return,
                        dirty_room_instance_ids = updates.wait_for_update() => dirty_room_instance_ids,
                    }
                }
            };
            dirty_room_instance_ids.extend(updates.take_pending_updates());
            if dirty_room_instance_ids.is_empty() {
                continue;
            }
            rooms
                .sync_source_packet_selection_policies_for_runtime_ids(
                    &dirty_room_instance_ids,
                    &observability_port,
                    &media_port,
                )
                .await;
        }
    })
}

fn build_media_transport(
    options: &CoreOptions,
    services: &RuntimeServices,
) -> Result<MediaTransport> {
    Ok(MediaTransport::from_core_options(
        options,
        MediaTransportDeps {
            diagnostics: Arc::clone(&services.diagnostics),
            packet_sink_registry: Arc::clone(&services.packet_sink_registry),
            metrics: Arc::clone(&services.metrics),
        },
    )?)
}

fn build_room_runtime_policy(
    config: &RuntimeConfig,
    options: &RuntimeOptions,
) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        RoomAdmissionPolicy::new(config.user.room_size),
        options.effective_feature_flags(),
        rtp_capabilities::router_rtp_capabilities_with_preferences(
            options.core.codecs.flags,
            options.core.codecs.preferences,
        ),
    )
    .with_room_sharding_policy(options.core.routing.room_sharding_policy)
}

fn build_room_manager(
    options: &RuntimeOptions,
    runtime_policy: RoomRuntimePolicy,
    services: &RuntimeServices,
) -> Arc<RoomManager> {
    Arc::new(RoomManager::new(
        RoomManagerConfig::new(options.core.routing.media_worker_count, runtime_policy),
        RoomManagerDeps {
            packet_sink_registry: Arc::clone(&services.packet_sink_registry),
            diagnostics: Arc::clone(&services.diagnostics),
            metrics: Arc::clone(&services.metrics),
        },
    ))
}

/// # Errors
///
/// Returns an error when tracing initialization fails, configuration loading fails,
/// the Tokio runtime cannot be built, or the HTTP/WebSocket listener exits with an
/// error.
pub fn run() -> Result<()> {
    let config = Config::from_env()?;
    let _telemetry = init_tracing(&config.telemetry, process::id())?;
    let runtime = Runtime::new(&config)?;
    Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(runtime.run_until_stopped())
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Weak},
        time::Duration,
    };

    use tokio::{
        task::yield_now,
        time::{sleep, timeout},
    };

    use super::{Result, RoomManager, Runtime};
    use crate::{
        config::{
            AuthConfig, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
            MediaCodecFlags, RoomShardingPolicy, RtcPortRange, RuntimeFeatureFlags,
            TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
        },
        core::server::room::DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    };

    #[tokio::test]
    async fn cancelling_serve_future_stops_source_packet_policy_task() {
        let runtime = Runtime::new(&test_config());
        assert!(runtime.is_ok());
        let Ok(runtime) = runtime else {
            return;
        };
        let rooms = Arc::downgrade(&runtime.room_manager);
        let server = tokio::spawn(runtime.serve(|_shutdown_token| pending::<Result<()>>()));

        let task_started =
            timeout(Duration::from_secs(1), wait_for_source_task_start(&rooms)).await;
        assert!(task_started.is_ok());

        server.abort();
        assert!(server.await.is_err());

        let room_manager_dropped =
            timeout(Duration::from_secs(1), wait_for_room_manager_drop(&rooms)).await;
        assert!(room_manager_dropped.is_ok());
    }

    async fn wait_for_source_task_start(rooms: &Weak<RoomManager>) {
        loop {
            if rooms.strong_count() > 1 {
                return;
            }
            yield_now().await;
        }
    }

    async fn wait_for_room_manager_drop(rooms: &Weak<RoomManager>) {
        loop {
            if rooms.upgrade().is_none() {
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    fn test_config() -> Config {
        Config {
            auth: AuthConfig {
                key: "dGVzdC1rZXk=".to_owned(),
                authentication_timeout_ms: 1_000,
            },
            http: HttpConfig {
                bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
                trust_proxy_headers: false,
            },
            user: UserConfig {
                room_size: 10,
                timeout_ms: 1_000,
                ping_interval_ms: 60_000,
                outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            },
            transport: TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                max_bitrate_in_bps: 8_000_000,
                max_bitrate_out_bps: 10_000_000,
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(41_000, 41_009),
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
}
