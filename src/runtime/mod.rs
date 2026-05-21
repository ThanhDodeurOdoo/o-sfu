//! process runtime shell that wires subsystems and owns server lifecycle
//!
//! `Runtime` is the process boundary for the media server. It is not the Tokio
//! executor and it is not the core media engine. It turns loaded configuration
//! into long-lived services, builds the media core, starts HTTP and WebSocket
//! serving, starts background policy work and makes shutdown cancel runtime-owned
//! tasks in one place.
//!
//! This type is useful because request handlers should not know how the process
//! was booted. They receive cheap clones through [`RuntimeState`] while the full
//! [`Runtime`] keeps ownership of services that must live for the whole process:
//! room management, diagnostics, metrics, media transport plus websocket
//! admission state.
//!
//! ```text
//! Runtime
//! |- http_server          -> HTTP control-plane routes and server boot
//! |- websocket_server     -> WebSocket upgrade, auth handshake, and steady-state socket loop
//! `- telemetry            -> tracing setup, schemas, diagnostics, metrics, and exporters
//! ```

use std::{
    future::Future,
    process,
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

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
    core::prelude::{CoreOptions, SfuCore},
};

pub(crate) mod auth;
pub(crate) mod diagnostics;
pub(crate) mod http_server;
pub(crate) mod options;
pub(crate) mod request_origin;
#[cfg(test)]
pub(super) mod test_support;
pub(crate) mod websocket_server;

use http_server::{serve_http, serve_http_on};
pub(crate) use o_sfu_core::{
    prelude::{ConnectionId, SessionBitrateLimits},
    server::{metrics, packet_sinks, room, transport as media_transport},
};
pub(crate) use o_sfu_telemetry::{self as telemetry, prometheus};
use options::{RuntimeConfig, RuntimeOptions};
use room::{
    RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomManagerDeps, RoomRuntimePolicy,
    rtp_capabilities,
};
use telemetry::{init_tracing, schema::event as telemetry_event};

pub(crate) use self::{
    diagnostics::DiagnosticsStore,
    media_transport::{MediaTransport, MediaTransportDeps},
    metrics::RuntimeMetrics,
    packet_sinks::RoomPacketSinkRegistry,
};

/// retry sweep cadence for retained rooms with pending transport cleanup
///
/// one second gives recoverable transport cleanup failures prompt progress
/// without turning room teardown recovery into a busy poll
const CLEANUP_RETRY_DRAIN_INTERVAL: Duration = Duration::from_secs(1);

/// Process-global shell for the server process.
///
/// `Runtime` owns boot-time configuration plus the long-lived services shared
/// by every request. It exists to keep process lifecycle decisions together:
/// service construction, listener serving, background task supervision and
/// graceful shutdown.
///
/// Request handlers do not receive this full object. They receive
/// [`RuntimeState`], which carries only the cheap service handles needed while a
/// request or websocket connection is active.
#[derive(Debug)]
pub struct Runtime {
    config: RuntimeConfig,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    media_transport: MediaTransport,
}

/// cheap-to-clone snapshot of runtime dependencies for per-request handlers
///
/// this is the standard shared state passed to axum handlers and websocket
/// loops. it provides access to room management, diagnostics, media transport
/// plus media core operations without exposing the full process lifecycle.
#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    config: RuntimeConfig,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    media_transport: MediaTransport,
    sfu_core: SfuCore,
    metrics: Arc<RuntimeMetrics>,
    pre_auth_websocket_admission: websocket_server::PreAuthWebSocketAdmission,
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
    cleanup_retry_drain: Option<JoinHandle<()>>,
}

impl RuntimeTasks {
    fn spawn(runtime: &Runtime) -> Self {
        let shutdown_token = CancellationToken::new();
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&runtime.room_manager),
            runtime.media_transport.clone(),
            runtime.media_transport.source_policy_subscription(),
            shutdown_token.child_token(),
        );
        let cleanup_retry_drain = spawn_cleanup_retry_drain_task(
            Arc::clone(&runtime.room_manager),
            runtime.media_transport.clone(),
            shutdown_token.child_token(),
        );
        Self {
            shutdown_token,
            source_packet_policy_sync: Some(source_packet_policy_sync),
            cleanup_retry_drain: Some(cleanup_retry_drain),
        }
    }

    /// provides a child token that will be cancelled when the runtime tasks stop
    fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.child_token()
    }

    /// signals background tasks to stop and waits for their completion
    async fn shutdown(mut self) {
        self.shutdown_token.cancel();
        if let Some(source_packet_policy_sync) = self.source_packet_policy_sync.take() {
            wait_for_runtime_task(source_packet_policy_sync, "source packet policy update").await;
        }
        if let Some(cleanup_retry_drain) = self.cleanup_retry_drain.take() {
            wait_for_runtime_task(cleanup_retry_drain, "cleanup retry drain").await;
        }
    }
}

async fn wait_for_runtime_task(task: JoinHandle<()>, name: &'static str) {
    if let Err(error) = task.await
        && !error.is_cancelled()
    {
        warn!(
            ?error,
            task = name,
            "runtime background task stopped unexpectedly"
        );
    }
}

impl Drop for RuntimeTasks {
    fn drop(&mut self) {
        // cancellation is idempotent, ensure tasks stop even if explicit shutdown was skipped
        self.shutdown_token.cancel();
        if let Some(source_packet_policy_sync) = self.source_packet_policy_sync.take() {
            source_packet_policy_sync.abort();
        }
        if let Some(cleanup_retry_drain) = self.cleanup_retry_drain.take() {
            cleanup_retry_drain.abort();
        }
    }
}

impl RuntimeState {
    fn from_parts(
        config: &RuntimeConfig,
        rooms: Arc<RoomManager>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
        media_transport: MediaTransport,
    ) -> Self {
        let sfu_core = SfuCore::new(media_transport.clone());
        let pre_auth_websocket_admission = websocket_server::PreAuthWebSocketAdmission::new(
            config.auth.max_pre_auth_websocket_sessions,
            config.auth.max_pre_auth_websocket_sessions_per_origin,
        );
        Self {
            config: config.clone(),
            room_manager: rooms,
            diagnostics,
            media_transport,
            sfu_core,
            metrics,
            pre_auth_websocket_admission,
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
        Self::from_parts(
            &runtime_config,
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
    media_transport: MediaTransport,
    updates: media_transport::SourcePolicyUpdateSubscription,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    info!("booted source packet policy update task");
    tokio::spawn(async move {
        loop {
            let next_deadline = media_transport.next_active_speaker_deadline().await;
            let mut dirty_room_instance_ids = match next_deadline {
                Some(next_deadline) => {
                    tokio::select! {
                        biased;
                        () = shutdown_token.cancelled() => return,
                        dirty_room_instance_ids = updates.wait_for_update() => dirty_room_instance_ids,
                        () = time::sleep_until(Instant::from_std(next_deadline)) => {
                            media_transport
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
                    &media_transport,
                )
                .await;
        }
    })
}

/// starts the process-owned driver for room cleanup retry progress
///
/// room cleanup retry state deliberately has no timer. this task supplies the
/// wall-clock poll from the runtime shell, then exits through the shared
/// shutdown token so retained rooms cannot keep the server future alive after
/// cancellation
///
/// missed ticks are skipped because cleanup retry draining is recovery work
/// rather than a backlog that should catch up under load
fn spawn_cleanup_retry_drain_task(
    rooms: Arc<RoomManager>,
    media_transport: MediaTransport,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    info!("booted cleanup retry drain task");
    tokio::spawn(async move {
        let mut retry_interval = time::interval(CLEANUP_RETRY_DRAIN_INTERVAL);
        retry_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => return,
                _ = retry_interval.tick() => {
                    rooms.drain_cleanup_retries(&media_transport).await;
                }
            }
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
    .with_room_worker_policy(options.core.routing.room_worker_policy)
    .with_media_limits(options.room_media_limits)
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
            AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig,
            HttpConfig, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange,
            RuntimeFeatureFlags, TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
        },
        core::server::room::{
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        },
    };

    #[tokio::test]
    async fn cancelling_serve_future_stops_runtime_background_tasks() {
        let runtime = Runtime::new(&test_config());
        assert!(runtime.is_ok());
        let Ok(runtime) = runtime else {
            return;
        };
        let rooms = Arc::downgrade(&runtime.room_manager);
        let server = tokio::spawn(runtime.serve(|_shutdown_token| pending::<Result<()>>()));

        let task_started =
            timeout(Duration::from_secs(1), wait_for_runtime_task_start(&rooms)).await;
        assert!(task_started.is_ok());

        server.abort();
        assert!(server.await.is_err());

        let room_manager_dropped =
            timeout(Duration::from_secs(1), wait_for_room_manager_drop(&rooms)).await;
        assert!(room_manager_dropped.is_ok());
    }

    async fn wait_for_runtime_task_start(rooms: &Weak<RoomManager>) {
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
                max_pre_auth_websocket_sessions: 512,
                max_pre_auth_websocket_sessions_per_origin: 16,
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
                outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
            },
            transport: TransportConfig {
                public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                max_bitrate_in: Bitrate::from_mbps(8),
                max_bitrate_out: Bitrate::from_mbps(10),
                video_bitrate_limits: VideoBitrateLimits::default(),
                rtc_port_range: RtcPortRange::new(41_000, 41_009),
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
}
