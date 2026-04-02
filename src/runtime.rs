use std::sync::Arc;

use anyhow::Result;
use tokio::runtime::Builder;

use crate::{config::Config, signaling::CURRENT_WIRE_PROTOCOL_VERSION};

mod http_server;
mod stub_channels;
mod websocket_server;

use http_server::serve_http;
use stub_channels::StubChannelRegistry;

#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    pub current_wire_protocol_version: u16,
    stub_channels: Arc<StubChannelRegistry>,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    pub config: Config,
    pub stub_channels: Arc<StubChannelRegistry>,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            current_wire_protocol_version: CURRENT_WIRE_PROTOCOL_VERSION,
            stub_channels: Arc::new(StubChannelRegistry::new()),
        }
    }

    async fn run_until_stopped(self) -> Result<()> {
        serve_http(RuntimeState {
            config: self.config,
            stub_channels: self.stub_channels,
        })
        .await
    }
}

/// # Errors
///
/// Returns an error when configuration loading fails or the HTTP server cannot bind.
pub fn run() -> Result<()> {
    let runtime = Runtime::new(Config::from_env()?);
    Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(runtime.run_until_stopped())
}
