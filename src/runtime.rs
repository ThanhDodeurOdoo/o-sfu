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
//! |- transport_adapter    -> runtime-facing transport facade
//! |  `- rtc_adapter       -> WebRTC worker and packet execution engine
//! |- recording            -> shared media tap used by channel policy and transport execution
//! `- metrics              -> process-global metrics state and Prometheus export snapshot
//! ```

use std::{process, sync::Arc};

use anyhow::Result;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::info;

use crate::config::Config;

pub(crate) mod auth;
#[cfg(feature = "internal-benchmarks")]
#[doc(hidden)]
pub mod benchmark_support;
pub(crate) mod channel;
pub(crate) mod http_server;
mod metrics;
mod metrics_export;
mod recording;
mod rtc_adapter;
pub(crate) mod telemetry;
#[cfg(test)]
pub(crate) mod test_rtp_samples;
#[doc(hidden)]
pub mod testing;
mod transport_adapter;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod transport_bootstrap;
#[cfg(test)]
mod transport_connect;
pub(crate) mod websocket_server;

use channel::ChannelAdmissionPolicy;
use channel::ChannelManager;
use channel::ChannelManagerConfig;
use channel::ChannelRuntimePolicy;
use http_server::serve_http;
use metrics::RuntimeMetrics;
use recording::MediaTap;
use telemetry::init_tracing;
use transport_adapter::{RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter};

const SOURCE_PACKET_POLICY_SYNC_INTERVAL: Duration = Duration::from_millis(100);

/// Process-global application shell for the server process.
///
/// `Runtime` owns the long-lived subsystems that every request shares: configuration,
/// channel allocation, metrics,and the transport backend. Per-requets entrypoints take
/// cheap clones of these dependencies through [`RuntimeState`].
#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    channels: Arc<ChannelManager>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    config: Config,
    channels: Arc<ChannelManager>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        let metrics = Arc::new(RuntimeMetrics::default());
        let recording_media_tap = Arc::new(MediaTap::default());
        let transport_adapter = build_transport_adapter(
            &config,
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
                Arc::clone(&metrics),
            )),
            metrics,
            transport_adapter,
        }
    }

    async fn run_until_stopped(self) -> Result<()> {
        let source_packet_policy_sync = spawn_source_packet_policy_sync_task(
            Arc::clone(&self.channels),
            self.transport_adapter.clone(),
        );
        let result = serve_http(RuntimeState {
            config: self.config,
            channels: self.channels,
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
/// facts like membership and publication state, while the transport layer own the
/// live packet gates that enforce those deciisons on RTP fanout. Keeping the sync in one
/// background task avoids forcing every channel mutation to await transport updates while
/// still ensuring the steady-state packet policy converges quickly.
fn spawn_source_packet_policy_sync_task(
    channels: Arc<ChannelManager>,
    transport_adapter: RuntimeTransportAdapter,
) -> JoinHandle<()> {
    info!(
        interval_ms = SOURCE_PACKET_POLICY_SYNC_INTERVAL.as_millis(),
        "booted source packet policy sync loop"
    );
    tokio::spawn(async move {
        let mut interval = time::interval(SOURCE_PACKET_POLICY_SYNC_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            channels
                .sync_source_packet_selection_policies(&transport_adapter)
                .await;
        }
    })
}

fn build_transport_adapter(
    config: &Config,
    recording_media_tap: Arc<MediaTap>,
    metrics: Arc<RuntimeMetrics>,
) -> RuntimeTransportAdapter {
    RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        config.public_ip,
        config.rtc_port_range,
        config.rtc_media_worker_count,
        config.codec_flags,
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
    init_tracing(&config.telemetry, process::id())?;
    let runtime = Runtime::new(config);
    Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(runtime.run_until_stopped())
}
