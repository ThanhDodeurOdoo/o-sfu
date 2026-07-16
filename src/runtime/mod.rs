//! process runtime shell that wires subsystems and owns server lifecycle
//!
//! [`Runtime`] turns loaded configuration into process-owned services, starts
//! HTTP and websocket serving, runs background policy work and cancels runtime
//! tasks on shutdown
//! request handlers receive [`RuntimeState`] so they cannot depend on process
//! boot details or full lifecycle ownership

use std::{future::Future, process, sync::Arc};

use anyhow::Result;
use tokio::{net::TcpListener, runtime::Builder, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{config::Config, core::prelude::SfuCore};

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
use options::{RuntimeConfig, effective_feature_flags};
use room::{
    RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomManagerDeps, RoomRuntimePolicy,
};
use telemetry::{init_tracing, schema::event as telemetry_event};

pub(crate) use self::{
    diagnostics::DiagnosticsStore,
    media_transport::{MediaTransport, MediaTransportConfig, MediaTransportDeps},
    metrics::RuntimeMetrics,
    packet_sinks::RoomPacketSinkRegistry,
};

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
        let services = RuntimeServices::default();
        let media_transport = build_media_transport(config, &services)?;
        let room_runtime_policy = build_room_runtime_policy(config, &media_transport);
        info!(
            event = telemetry_event::RUNTIME_BOOT,
            rtc_udp_io_backend = config.transport.rtc_udp_io_backend.wire_name(),
            "runtime configuration loaded"
        );
        let room_manager = build_room_manager(config, room_runtime_policy, &services);
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
}

impl RuntimeTasks {
    fn spawn(runtime: &Runtime) -> Self {
        let shutdown_token = CancellationToken::new();
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&runtime.room_manager),
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
        if let Some(source_packet_policy_sync) = self.source_packet_policy_sync.take() {
            wait_for_runtime_task(source_packet_policy_sync, "source packet policy update").await;
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

/// Recomputes room packet policy after transport observations change.
fn spawn_source_packet_policy_update_task(
    rooms: Arc<RoomManager>,
    media_transport: MediaTransport,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    info!("booted source packet policy update task");
    let updates = media_transport.source_policy_subscription();
    tokio::spawn(async move {
        loop {
            let mut dirty_rooms = tokio::select! {
                biased;
                () = shutdown_token.cancelled() => return,
                dirty_rooms = updates.wait_for_update() => dirty_rooms,
            };
            dirty_rooms.extend(updates.take_pending_updates());
            rooms
                .sync_source_packet_selection_policies_for_runtime_ids(
                    &dirty_rooms,
                    &media_transport,
                )
                .await;
        }
    })
}

fn build_media_transport(config: &Config, services: &RuntimeServices) -> Result<MediaTransport> {
    Ok(MediaTransport::build(
        MediaTransportConfig {
            worker_count: config.transport.rtc_media_worker_count,
            announced_ip: config.transport.announced_ip,
            bitrate_limits: SessionBitrateLimits::new(
                config.transport.max_bitrate_in,
                config.transport.max_bitrate_out,
            ),
            video_bitrate_limits: config.transport.video_bitrate_limits,
            rtc_port_range: config.transport.rtc_port_range,
            rtc_udp_io_backend: config.transport.rtc_udp_io_backend,
            codec_flags: config.codecs.flags,
            codec_preferences: config.codecs.preferences,
            media_quality_interval: config.telemetry.media_quality_interval,
        },
        MediaTransportDeps {
            diagnostics: Arc::clone(&services.diagnostics),
            packet_sink_registry: Arc::clone(&services.packet_sink_registry),
            metrics: Arc::clone(&services.metrics),
        },
    )?)
}

fn build_room_runtime_policy(
    config: &Config,
    media_transport: &MediaTransport,
) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        RoomAdmissionPolicy::new(config.user.room_size),
        effective_feature_flags(config.features),
        media_transport.router_rtp_capabilities(),
    )
    .with_room_worker_policy(config.transport.room_worker_policy)
    .with_media_limits(config.transport.room_media_limits)
}

fn build_room_manager(
    config: &Config,
    runtime_policy: RoomRuntimePolicy,
    services: &RuntimeServices,
) -> Arc<RoomManager> {
    Arc::new(RoomManager::new(
        RoomManagerConfig::new(config.transport.rtc_media_worker_count, runtime_policy),
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
