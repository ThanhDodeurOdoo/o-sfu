use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use tokio::runtime::Builder;
use tracing_subscriber::EnvFilter;

use crate::{
    config::{Config, TransportBackend},
    signaling::CURRENT_WIRE_PROTOCOL_VERSION,
};

#[cfg(feature = "internal-benchmarks")]
#[doc(hidden)]
pub mod benchmark_support;
pub(crate) mod channel;
mod http_server;
mod metrics;
mod recording;
mod rtc_adapter;
mod stub_bus;
#[doc(hidden)]
pub mod testing;
mod transport_adapter;
mod transport_bootstrap;
mod websocket_server;

use channel::ChannelManager;
use http_server::serve_http;
use metrics::RuntimeMetrics;
use recording::MediaTap;
use transport_adapter::RuntimeTransportAdapter;

#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    pub current_wire_protocol_version: u16,
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
        let recording_media_tap = Arc::new(MediaTap::default());
        let transport_adapter = build_transport_adapter(&config, Arc::clone(&recording_media_tap));
        let rtc_media_worker_count = config.rtc_media_worker_count;
        Self {
            config,
            current_wire_protocol_version: CURRENT_WIRE_PROTOCOL_VERSION,
            channels: Arc::new(ChannelManager::with_media_workers_and_recording_tap(
                rtc_media_worker_count,
                recording_media_tap,
            )),
            metrics: Arc::new(RuntimeMetrics::default()),
            transport_adapter,
        }
    }

    async fn run_until_stopped(self) -> Result<()> {
        serve_http(RuntimeState {
            config: self.config,
            channels: self.channels,
            metrics: self.metrics,
            transport_adapter: self.transport_adapter,
        })
        .await
    }
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
) -> RuntimeTransportAdapter {
    match config.transport_backend {
        TransportBackend::Stub => RuntimeTransportAdapter::stub(),
        TransportBackend::Rtc => RuntimeTransportAdapter::rtc(
            config.public_ip,
            config.rtc_port_range,
            config.rtc_media_worker_count,
            recording_media_tap,
        ),
    }
}
