//! `runtime` decide which concerns stay process-global, wires those long-lived subsystems
//! together once, and then hands request-local work to the apporpriate child node instead of
//! mixing admission, room state, and media execution in one place.
//!
//! ```text
//! Runtime
//! |- http_server          -> HTTP control-plane routes and server boot
//! |- websocket_server     -> WebSocket upgrade, auth handshake, and steady-state socket loop
//! |- core                 -> room engine, media transport, recording, metrics, and diagnostics
//! `- telemetry crate      -> tracing setup, schemas, diagnostics, metrics, and exporters
//! ```

use std::{collections::BTreeSet, future::Future, process, sync::Arc, time::Instant as StdInstant};

use anyhow::Result;
use tokio::{
    net::TcpListener,
    runtime::Builder,
    task::JoinHandle,
    time::{self, Instant},
};
use tracing::info;

use crate::{
    config::Config,
    core::{CoreOptions, RuntimeSfuCore, SfuCore},
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
pub(crate) use media_transport::{
    MediaPort, MediaTransport, MediaTransportDeps, ObservabilityPort,
};
pub(crate) use metrics::RuntimeMetrics;
pub(crate) use o_sfu_core::{
    ConnectionId, RoomInstanceId, SessionBitrateLimits,
    server::{metrics, recording, room, transport as media_transport},
};
pub(crate) use o_sfu_telemetry as telemetry;
pub(crate) use o_sfu_telemetry::prometheus;
use options::{HttpOptions, RuntimeOptions, SocketOptions};
pub(crate) use recording::MediaTap;
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
    options: RuntimeOptions,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    media_transport: MediaTransport,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    http_options: HttpOptions,
    websocket_options: SocketOptions,
    rooms: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    media_transport: MediaTransport,
    media_core: RuntimeSfuCore,
    metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeServices {
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    recording_media_tap: Arc<MediaTap>,
}

impl Default for RuntimeServices {
    fn default() -> Self {
        Self {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
            recording_media_tap: Arc::new(MediaTap::default()),
        }
    }
}

impl Runtime {
    /// Builds the process runtime from loaded configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the media transport cannot be constructed from the
    /// configured RTC settings.
    pub fn new(config: &Config) -> Result<Self> {
        let options = RuntimeOptions::from_config(config);
        let services = RuntimeServices::default();
        let media_transport = build_media_transport(&options.core, &services)?;
        let room_runtime_policy = build_room_runtime_policy(&options);
        info!("{}", config.log_view(process::id()));
        info!(
            event = telemetry_event::RUNTIME_BOOT,
            "runtime configuration loaded"
        );
        let room_manager = build_room_manager(&options, room_runtime_policy, &services);
        Ok(Self {
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
        self.serve(serve_http_on(listener, state)).await
    }

    async fn run_until_stopped(self) -> Result<()> {
        let state = self.state();
        self.serve(serve_http(state)).await
    }

    async fn serve(self, http_server: impl Future<Output = Result<()>>) -> Result<()> {
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&self.room_manager),
            self.media_transport.clone(),
            subscribe_source_policy_updates(&self.media_transport),
            self.media_transport.clone(),
        );
        let result = http_server.await;
        source_packet_policy_sync.abort();
        let _ = source_packet_policy_sync.await;
        result
    }

    fn state(&self) -> RuntimeState {
        RuntimeState::from_parts(
            &self.options,
            Arc::clone(&self.room_manager),
            Arc::clone(&self.diagnostics),
            Arc::clone(&self.metrics),
            self.media_transport.clone(),
        )
    }
}

impl RuntimeState {
    fn from_parts(
        options: &RuntimeOptions,
        rooms: Arc<RoomManager>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
        media_transport: MediaTransport,
    ) -> Self {
        let media_core = SfuCore::new(options.core, media_transport.clone());
        Self {
            http_options: options.http.clone(),
            websocket_options: options.websocket.clone(),
            rooms,
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
        let options = RuntimeOptions::from_config(config);
        Self::from_parts(&options, rooms, diagnostics, metrics, media_transport)
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
) -> JoinHandle<()> {
    info!("booted source packet policy update task");
    tokio::spawn(async move {
        loop {
            let next_deadline = next_active_speaker_deadline(&observability_port).await;
            let mut dirty_room_instance_ids = match next_deadline {
                Some(next_deadline) => {
                    tokio::select! {
                        dirty_room_instance_ids = updates.wait_for_update() => dirty_room_instance_ids,
                        () = time::sleep_until(Instant::from_std(next_deadline)) => {
                            expired_active_speaker_room_instance_ids(
                                &observability_port,
                                StdInstant::now(),
                            )
                            .await
                        }
                    }
                }
                None => updates.wait_for_update().await,
            };
            dirty_room_instance_ids.extend(updates.take_pending_updates());
            if dirty_room_instance_ids.is_empty() {
                continue;
            }
            sync_source_packet_selection_policies(
                &rooms,
                &dirty_room_instance_ids,
                &observability_port,
                &media_port,
            )
            .await;
        }
    })
}

fn subscribe_source_policy_updates(
    source_policy_port: &impl SourcePolicyPort,
) -> media_transport::SourcePolicyUpdateSubscription {
    source_policy_port.source_policy_subscription()
}

async fn next_active_speaker_deadline(
    observability_port: &impl ObservabilityPort,
) -> Option<StdInstant> {
    observability_port.next_active_speaker_deadline().await
}

async fn expired_active_speaker_room_instance_ids(
    observability_port: &impl ObservabilityPort,
    now: StdInstant,
) -> BTreeSet<RoomInstanceId> {
    observability_port
        .expired_active_speaker_room_instance_ids(now)
        .await
}

async fn sync_source_packet_selection_policies(
    rooms: &RoomManager,
    room_instance_ids: &BTreeSet<RoomInstanceId>,
    observability_port: &impl ObservabilityPort,
    media_port: &impl MediaPort,
) {
    rooms
        .sync_source_packet_selection_policies_for_runtime_ids(
            room_instance_ids,
            observability_port,
            media_port,
        )
        .await;
}

fn build_media_transport(
    options: &CoreOptions,
    services: &RuntimeServices,
) -> Result<MediaTransport> {
    Ok(MediaTransport::from_core_options(
        options,
        MediaTransportDeps {
            diagnostics: Arc::clone(&services.diagnostics),
            packet_sink_registry: Arc::clone(&services.recording_media_tap),
            metrics: Arc::clone(&services.metrics),
        },
    )?)
}

fn build_room_runtime_policy(options: &RuntimeOptions) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        RoomAdmissionPolicy::new(options.room.max_users),
        options.feature_flags(),
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
            recording_media_tap: Arc::clone(&services.recording_media_tap),
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
