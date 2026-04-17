use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[cfg(feature = "internal-benchmarks")]
#[doc(hidden)]
pub mod benchmark_support;
#[allow(
    dead_code,
    reason = "protocol session establishment does not yet exercise the remaining publish and transport-readiness channel paths that stay scheduled for the next implementation phase"
)]
pub(crate) mod channel;
mod http_server;
mod metrics;
mod metrics_export;
mod recording;
mod rtc_adapter;
#[cfg(test)]
pub(crate) mod test_rtp_samples;
#[doc(hidden)]
pub mod testing;
mod transport_adapter;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod transport_bootstrap;
#[cfg(test)]
mod transport_connect;
mod websocket_server;

use channel::ChannelAdmissionPolicy;
use channel::ChannelManager;
use channel::ChannelManagerConfig;
use channel::ChannelRuntimePolicy;
use channel::SessionCleanupPolicy;
use http_server::serve_http;
use metrics::RuntimeMetrics;
use recording::MediaTap;
use transport_adapter::{RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter};

const SOURCE_PACKET_POLICY_SYNC_INTERVAL: Duration = Duration::from_millis(100);

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

impl RuntimeState {
    #[must_use]
    pub(super) const fn session_cleanup_policy() -> SessionCleanupPolicy {
        SessionCleanupPolicy::StateAndTransportMedia
    }
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

fn spawn_source_packet_policy_sync_task(
    channels: Arc<ChannelManager>,
    transport_adapter: RuntimeTransportAdapter,
) -> JoinHandle<()> {
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

fn init_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_error| EnvFilter::new("o_sfu=info,o_sfu_router=info")),
        )
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|error| anyhow!(error.to_string()))?;
    Ok(())
}

/// # Errors
///
/// Returns an error when configuration loading fails or the HTTP server cannot bind.
pub fn run() -> Result<()> {
    init_tracing()?;
    let runtime = Runtime::new(Config::from_env()?);
    Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(runtime.run_until_stopped())
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
