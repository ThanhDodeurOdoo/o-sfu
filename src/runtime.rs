use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use tokio::runtime::Builder;
use tracing_subscriber::EnvFilter;

use crate::{config::Config, signaling::CURRENT_WIRE_PROTOCOL_VERSION};

pub(crate) mod channel;
mod http_server;
mod stub_bus;
#[doc(hidden)]
pub mod testing;
mod websocket_server;

use channel::ChannelManager;
use http_server::serve_http;

#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    pub current_wire_protocol_version: u16,
    channels: Arc<ChannelManager>,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    pub config: Config,
    pub channels: Arc<ChannelManager>,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            current_wire_protocol_version: CURRENT_WIRE_PROTOCOL_VERSION,
            channels: Arc::new(ChannelManager::new()),
        }
    }

    async fn run_until_stopped(self) -> Result<()> {
        serve_http(RuntimeState {
            config: self.config,
            channels: self.channels,
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
