use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::{net::TcpListener, task::JoinHandle};

use super::{
    RuntimeState, build_transport_adapter,
    channel::{
        ChannelAdmissionPolicy, ChannelManager, ChannelManagerConfig, ChannelRuntimePolicy,
        rtp_capabilities::router_rtp_capabilities,
    },
    http_server::app,
    metrics::RuntimeMetrics,
    recording::MediaTap,
};
use crate::config::Config;
use crate::signaling::protocol::{EnvelopeBatch, ServerEnvelope, ServerMessage, WelcomePayload};

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
    let metrics = Arc::new(RuntimeMetrics::default());
    let recording_media_tap = Arc::new(MediaTap::default());
    let channels = Arc::new(ChannelManager::new(
        ChannelManagerConfig::new(
            config.rtc_media_worker_count,
            ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(config.channel_size),
                config.feature_flags,
                router_rtp_capabilities(config.codec_flags),
            ),
        ),
        Arc::clone(&recording_media_tap),
        Arc::clone(&metrics),
    ));
    let transport_adapter =
        build_transport_adapter(&config, recording_media_tap, Arc::clone(&metrics));
    let state = RuntimeState {
        config,
        channels: Arc::clone(&channels),
        metrics,
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

#[must_use]
pub fn decode_protocol_welcome_batch(payload: &str) -> Option<WelcomePayload> {
    let batch = serde_json::from_str::<EnvelopeBatch>(payload).ok()?;
    let envelope = batch.first()?.clone();
    match ServerEnvelope::decode(envelope).ok()? {
        ServerEnvelope::Message(ServerMessage::Welcome(welcome)) => Some(welcome),
        ServerEnvelope::Message(_)
        | ServerEnvelope::Request { .. }
        | ServerEnvelope::Response { .. } => None,
    }
}
