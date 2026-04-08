use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::{net::TcpListener, task::JoinHandle};

use super::{
    RuntimeState, build_transport_adapter, channel::ChannelManager, http_server::app,
    metrics::RuntimeMetrics,
};
use crate::config::Config;

/// Test-only server handle used by integration tests to exercise the real HTTP and WS entry points.
#[derive(Debug)]
pub struct TestServer {
    addr: SocketAddr,
    handle: JoinHandle<()>,
}

impl TestServer {
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}/", self.addr)
    }

    #[must_use]
    pub fn http_base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spawn the real axum server on an ephemeral port for integration tests.
///
/// # Errors
///
/// Returns an error when the test listener cannot bind or the local socket address cannot be read.
pub async fn spawn_test_server(config: Config) -> Result<TestServer> {
    let channels = Arc::new(ChannelManager::new());
    let transport_adapter = build_transport_adapter(&config);
    let state = RuntimeState {
        config,
        channels: Arc::clone(&channels),
        metrics: Arc::new(RuntimeMetrics::default()),
        transport_adapter,
    };
    let listener = TcpListener::bind(state.config.bind_address).await?;
    let addr = listener
        .local_addr()
        .map_err(|error| anyhow!("failed to read test listener address: {error}"))?;
    let handle = tokio::spawn(async move {
        let result = axum::serve(listener, app(state)).await;
        assert!(
            result.is_ok(),
            "test server should stop cleanly: {result:?}"
        );
    });
    Ok(TestServer { addr, handle })
}
