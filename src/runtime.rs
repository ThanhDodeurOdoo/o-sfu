//! `runtime` decide which concerns stay process-global, wires those long-lived subsystems
//! together once, and then hands request-local work to the apporpriate child node instead of
//! mixing admission, room state, and media execution in one place.
//!
//! ```text
//! Runtime
//! |- http_server          -> HTTP control-plane routes and server boot
//! |- websocket_server     -> WebSocket upgrade, auth handshake, and steady-state socket loop
//! |- telemetry            -> runtime-owned tracing config and event-name conventions
//! |- core                 -> room engine, transport adapter, recording, metrics, and diagnostics
//! `- metrics_export       -> Prometheus export snapshot
//! ```

use std::{collections::BTreeSet, process, sync::Arc, time::Instant as StdInstant};

use anyhow::Result;
use tokio::{
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
mod metrics_export;
pub(crate) mod options;
mod request_origin;
pub(crate) mod telemetry;
#[doc(hidden)]
pub mod testing;
pub(crate) mod websocket_server;

pub(crate) use diagnostics::DiagnosticsStore;
use http_server::serve_http;
pub(crate) use metrics::RuntimeMetrics;
pub(crate) use o_sfu_core::{
    ConnectionId, RoomInstanceId, SessionBitrateLimits,
    server::{metrics, recording, room, transport as transport_adapter},
};
use options::{HttpOptions, RuntimeOptions, SocketOptions};
pub(crate) use recording::MediaTap;
pub(crate) use request_origin::resolve_remote_address;
use room::{
    RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomRuntimePolicy, rtp_capabilities,
};
use telemetry::init_tracing;
use transport_adapter::SourcePolicyPort;
#[cfg(any(test, feature = "testing-transport"))]
pub use transport_adapter::test_support::test_transport_session_key;
pub(crate) use transport_adapter::{
    MediaPort, MediaTransport, MediaTransportDeps, ObservabilityPort, RuntimeTransportAdapter,
    client_rtp_capabilities_from_answer,
};
pub use transport_adapter::{RemoteAddrDemux, TransportSessionKey};

/// Process-global shell for the server process.
///
/// `Runtime` owns the long-lived subsystems that every request shares: configuration,
/// room allocation, metrics,and the transport backend. Per-requets entrypoints take
/// cheap clones of these dependencies through [`RuntimeState`].
#[derive(Debug)]
pub struct Runtime {
    options: RuntimeOptions,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    http_options: HttpOptions,
    websocket_options: SocketOptions,
    rooms: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    transport_adapter: RuntimeTransportAdapter,
    media_core: RuntimeSfuCore,
    metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Clone)]
struct RuntimeDeps {
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    recording_media_tap: Arc<MediaTap>,
}

impl Default for RuntimeDeps {
    fn default() -> Self {
        Self {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
            recording_media_tap: Arc::new(MediaTap::default()),
        }
    }
}

impl Runtime {
    pub fn new(config: &Config) -> Result<Self> {
        let options = RuntimeOptions::from_config(config);
        let deps = RuntimeDeps::default();
        let transport_adapter = build_transport_adapter(&options.core, &deps)?;
        let room_runtime_policy = build_room_runtime_policy(&options);
        info!("{}", config.log_view(process::id()));
        info!(
            event = telemetry::schema::event::RUNTIME_BOOT,
            "runtime configuration loaded"
        );
        let room_manager = build_room_manager(&options, room_runtime_policy, &deps);
        Ok(Self {
            options,
            room_manager,
            diagnostics: deps.diagnostics,
            metrics: deps.metrics,
            transport_adapter,
        })
    }

    async fn run_until_stopped(self) -> Result<()> {
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&self.room_manager),
            self.transport_adapter.clone(),
            subscribe_source_policy_updates(&self.transport_adapter),
            self.transport_adapter.clone(),
        );
        let options = self.options.clone();
        let media_core = SfuCore::new(options.core, self.transport_adapter.clone());
        let result = serve_http(RuntimeState {
            http_options: options.http.clone(),
            websocket_options: options.websocket.clone(),
            rooms: Arc::clone(&self.room_manager),
            diagnostics: Arc::clone(&self.diagnostics),
            transport_adapter: self.transport_adapter.clone(),
            media_core,
            metrics: self.metrics,
        })
        .await;
        source_packet_policy_sync.abort();
        let _ = source_packet_policy_sync.await;
        result
    }
}

/// Room state decides which producer layers should remain routable from room-level
/// facts like membership and publication state, while the transport layer owns the
/// active-speaker observations that can change without any room mutation. This task
/// waits on explicit transport-side updates plus the current active-speaker expiry
/// deadline instead of polling the whole process on a fixed interval.
fn spawn_source_packet_policy_update_task(
    rooms: Arc<RoomManager>,
    observability_port: RuntimeTransportAdapter,
    updates: transport_adapter::SourcePolicyUpdateSubscription,
    media_port: RuntimeTransportAdapter,
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
) -> transport_adapter::SourcePolicyUpdateSubscription {
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

fn build_transport_adapter(
    options: &CoreOptions,
    deps: &RuntimeDeps,
) -> Result<RuntimeTransportAdapter> {
    Ok(MediaTransport::from_core_options(
        options,
        MediaTransportDeps {
            diagnostics: Arc::clone(&deps.diagnostics),
            packet_sink_registry: Arc::clone(&deps.recording_media_tap),
            metrics: Arc::clone(&deps.metrics),
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
}

fn build_room_manager(
    options: &RuntimeOptions,
    runtime_policy: RoomRuntimePolicy,
    deps: &RuntimeDeps,
) -> Arc<RoomManager> {
    Arc::new(RoomManager::new(
        RoomManagerConfig::new(options.core.routing.media_worker_count, runtime_policy),
        Arc::clone(&deps.recording_media_tap),
        Arc::clone(&deps.diagnostics),
        Arc::clone(&deps.metrics),
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
