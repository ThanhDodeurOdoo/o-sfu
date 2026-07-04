//! process runtime shell that wires subsystems and owns server lifecycle
//!
//! [`Runtime`] turns loaded configuration into process-owned services, starts
//! HTTP and websocket serving, runs background policy work and cancels runtime
//! tasks on shutdown
//! request handlers receive [`RuntimeState`] so they cannot depend on process
//! boot details or full lifecycle ownership

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
#[path = "TESTS/support.rs"]
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
const CLEANUP_RETRY_DRAIN_INTERVAL: Duration = Duration::from_secs(1);

/// state for shared runtime services
///
/// [`Runtime`] keeps boot-time configuration plus the long-lived services
/// shared by every request. It exists to keep process lifecycle decisions together:
/// service construction, listener serving, background task supervision and
/// graceful shutdown
///
/// request handlers do not receive this full object
/// they receive a runtime
/// state handle with only the cheap service handles needed while a request or
/// websocket connection is active
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
/// loops
/// it provides access to room management, diagnostics, media transport
/// plus media core operations without exposing the full process lifecycle
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
    /// build the process runtime from loaded configuration
    ///
    /// this bootstraps the entire server instance by initializing telemetry,
    /// creating the room manager and preparing the media transport workers
    ///
    /// # Errors
    ///
    /// returns an error when the media transport cannot be constructed from the
    /// configured RTC settings
    pub fn new(config: &Config) -> Result<Self> {
        let runtime_config = RuntimeConfig::from_config(config);
        let options = RuntimeOptions::from_config(config);
        let services = RuntimeServices::default();
        let media_transport = build_media_transport(&options.core, &services)?;
        let room_runtime_policy = build_room_runtime_policy(&runtime_config, &options);
        info!(
            event = telemetry_event::RUNTIME_BOOT,
            rtc_udp_io_backend = options.core.media.rtc_udp_io_backend.wire_name(),
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

    /// serve HTTP and websocket traffic on a caller-provided listener
    ///
    /// this is the embedder-friendly sibling of [`run`]
    /// it lets integration
    /// tests and external hosts bind an ephemeral port before handing the
    /// socket to the production runtime
    ///
    /// # Errors
    ///
    /// returns an error when the Axum server fails while serving the supplied
    /// listener
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
    /// coordinates the control plane with background workers
    ///
    /// background tasks are explicitly joined when the server stops
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

/// runtime background tasks for the lifetime of one server future
///
/// normal shutdown asks tasks to exit through the shared cancellation token and
/// waits for them
/// dropping the server future cancels the token and aborts any
/// remaining task so embedders cannot detach process workers by cancelling
/// [`Runtime::serve_listener`]
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
        let sfu_core = SfuCore::new(media_transport.clone(), Arc::clone(&rooms));
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
}

/// room state decides which producer layers should remain routable from room-level
/// facts like membership and publication state while transport owns the
/// active-speaker observations that can change without room mutation
/// this task
/// waits on explicit transport-side updates plus the current active-speaker expiry
/// deadline instead of polling the whole process on a fixed interval
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

/// starts the process driver for room cleanup retry progress
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
            diagnostics: Arc::clone(&services.diagnostics),
            metrics: Arc::clone(&services.metrics),
        },
    ))
}

/// # Errors
///
/// returns an error when tracing initialization fails, configuration loading fails,
/// the Tokio runtime cannot be built, or the HTTP/WebSocket listener exits with an
/// error
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
#[path = "TESTS/runtime.rs"]
mod tests;
