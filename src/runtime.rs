//! `runtime` decide which concerns stay process-global, wires those long-lived subsystems
//! together once, and then hands request-local work to the apporpriate child node instead of
//! mixing admission, room state, and media execution in one place.
//!
//! ```text
//! Runtime
//! |- http_server          -> HTTP control-plane routes and server boot
//! |- websocket_server     -> WebSocket upgrade, auth handshake, and steady-state socket loop
//! |  `- session_protocol  -> authenticated signaling flow for one connected session
//! |- channel              -> room allocation, membership, negotiation, and recording policy
//! |- telemetry            -> runtime-owned tracing config and event-name conventions
//! |- transport_adapter    -> runtime-facing transport service boundary
//! |  `- rtc_adapter       -> WebRTC worker and packet execution engine
//! |- recording            -> shared media tap used by channel policy and transport execution
//! `- metrics              -> process-global metrics state and Prometheus export snapshot
//! ```

use std::{collections::BTreeSet, process, sync::Arc, time::Instant as StdInstant};

use anyhow::Result;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tracing::info;

use crate::config::Config;

pub(crate) mod auth;
#[cfg(feature = "internal-benchmarks")]
#[doc(hidden)]
pub mod benchmark_support;
pub(crate) mod channel;
pub(crate) mod diagnostics;
pub(crate) mod http_server;
mod ids;
mod metrics;
mod metrics_export;
mod recording;
mod request_origin;
mod rtc_adapter;
pub(crate) mod telemetry;
#[cfg(test)]
pub(crate) mod test_rtp_samples;
#[doc(hidden)]
pub mod testing;
mod transport_adapter;
pub(crate) mod websocket_server;

use channel::ChannelAdmissionPolicy;
use channel::ChannelManager;
use channel::ChannelManagerConfig;
use channel::ChannelRuntimePolicy;
use diagnostics::DiagnosticsStore;
use http_server::serve_http;
pub(crate) use ids::{ChannelRuntimeId, ConnectionId};
use metrics::RuntimeMetrics;
use recording::MediaTap;
use telemetry::init_tracing;
use transport_adapter::{
    MediaPort, ObservabilityPort, RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter,
    SessionBitrateLimits, SourcePolicyPort,
};

/// Process-global application shell for the server process.
///
/// `Runtime` owns the long-lived subsystems that every request shares: configuration,
/// channel allocation, metrics,and the transport backend. Per-requets entrypoints take
/// cheap clones of these dependencies through [`RuntimeState`].
#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    channels: Arc<ChannelManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    config: Config,
    channels: Arc<ChannelManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        let diagnostics = Arc::new(DiagnosticsStore::default());
        let metrics = Arc::new(RuntimeMetrics::default());
        let recording_media_tap = Arc::new(MediaTap::default());
        let transport_adapter = build_transport_adapter(
            &config,
            Arc::clone(&diagnostics),
            Arc::clone(&recording_media_tap),
            Arc::clone(&metrics),
        );
        let rtc_media_worker_count = config.rtc_media_worker_count;
        let channel_runtime_policy = ChannelRuntimePolicy::new(
            ChannelAdmissionPolicy::new(config.channel_size),
            config.feature_flags,
            channel::rtp_capabilities::router_rtp_capabilities(config.codec_flags),
        );
        info!("{}", config.log_view(process::id()));
        info!(
            event = telemetry::schema::event::RUNTIME_BOOT,
            "runtime configuration loaded"
        );
        Self {
            config,
            channels: Arc::new(ChannelManager::new(
                ChannelManagerConfig::new(rtc_media_worker_count, channel_runtime_policy),
                recording_media_tap,
                Arc::clone(&diagnostics),
                Arc::clone(&metrics),
            )),
            diagnostics,
            metrics,
            transport_adapter,
        }
    }

    async fn run_until_stopped(self) -> Result<()> {
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&self.channels),
            self.transport_adapter.clone(),
            subscribe_source_policy_updates(&self.transport_adapter),
            self.transport_adapter.clone(),
        );
        let result = serve_http(RuntimeState {
            config: self.config,
            channels: self.channels,
            diagnostics: self.diagnostics,
            metrics: self.metrics,
            transport_adapter: self.transport_adapter,
        })
        .await;
        source_packet_policy_sync.abort();
        let _ = source_packet_policy_sync.await;
        result
    }
}

/// Channel state decides which producer layers should remain routable from room-level
/// facts like membership and publication state, while the transport layer owns the
/// active-speaker observations that can change without any room mutation. This task
/// waits on explicit transport-side updates plus the current active-speaker expiry
/// deadline instead of polling the whole process on a fixed interval.
fn spawn_source_packet_policy_update_task(
    channels: Arc<ChannelManager>,
    observability_port: RuntimeTransportAdapter,
    updates: transport_adapter::SourcePolicyUpdateSubscription,
    media_port: RuntimeTransportAdapter,
) -> JoinHandle<()> {
    info!("booted source packet policy update task");
    tokio::spawn(async move {
        loop {
            let next_deadline = next_active_speaker_deadline(&observability_port).await;
            let mut dirty_channel_runtime_ids = match next_deadline {
                Some(next_deadline) => {
                    tokio::select! {
                        dirty_channel_runtime_ids = updates.wait_for_update() => dirty_channel_runtime_ids,
                        () = time::sleep_until(Instant::from_std(next_deadline)) => {
                            expired_active_speaker_channel_runtime_ids(
                                &observability_port,
                                StdInstant::now(),
                            )
                            .await
                        }
                    }
                }
                None => updates.wait_for_update().await,
            };
            dirty_channel_runtime_ids.extend(updates.take_pending_updates());
            if dirty_channel_runtime_ids.is_empty() {
                continue;
            }
            sync_source_packet_selection_policies(
                &channels,
                &dirty_channel_runtime_ids,
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

async fn expired_active_speaker_channel_runtime_ids(
    observability_port: &impl ObservabilityPort,
    now: StdInstant,
) -> BTreeSet<ChannelRuntimeId> {
    observability_port
        .expired_active_speaker_channel_runtime_ids(now)
        .await
}

async fn sync_source_packet_selection_policies(
    channels: &ChannelManager,
    channel_runtime_ids: &BTreeSet<ChannelRuntimeId>,
    observability_port: &impl ObservabilityPort,
    media_port: &impl MediaPort,
) {
    channels
        .sync_source_packet_selection_policies_for_runtime_ids(
            channel_runtime_ids,
            observability_port,
            media_port,
        )
        .await;
}

fn build_transport_adapter(
    config: &Config,
    diagnostics: Arc<DiagnosticsStore>,
    recording_media_tap: Arc<MediaTap>,
    metrics: Arc<RuntimeMetrics>,
) -> RuntimeTransportAdapter {
    RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        config.public_ip,
        SessionBitrateLimits::new(config.max_bitrate_in_bps, config.max_bitrate_out_bps),
        config.rtc_port_range,
        config.rtc_media_worker_count,
        config.codec_flags,
        diagnostics,
        recording_media_tap,
        metrics,
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
    let runtime = Runtime::new(config);
    Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(runtime.run_until_stopped())
}
