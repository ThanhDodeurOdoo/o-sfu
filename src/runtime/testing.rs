use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use o_sfu_protocol::{
    shared::{StreamType, UserId},
    signaling::{EnvelopeBatch, ServerEnvelope, ServerMessage, WelcomePayload},
};
use tokio::{
    net::TcpListener,
    task::{JoinHandle, yield_now},
    time::timeout,
};

use super::{
    RuntimeServices, RuntimeState, build_transport_adapter,
    diagnostics::DiagnosticsStore,
    http_server::app,
    metrics::RuntimeMetrics,
    options::RuntimeOptions,
    room::{
        ConsumerRouteState, RoomAdmissionPolicy, RoomManager, RoomManagerConfig, RoomManagerDeps,
        RoomRuntimePolicy, rtp_capabilities::router_rtp_capabilities,
    },
    transport_adapter::MediaTransport,
};
use crate::{config::Config, core::SfuCore};

#[derive(Debug, Default)]
pub struct SourcePolicyDirtyState(super::transport_adapter::SourcePolicyDirtyState);

impl SourcePolicyDirtyState {
    pub fn take_dirty(&self) -> bool {
        self.0.take_dirty()
    }

    pub fn mark_dirty(&self) -> bool {
        self.0.mark_dirty()
    }

    pub fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }
}

pub use super::{
    recording::ActiveRoomRegistry,
    transport_adapter::{RelayTargetRegistry, WorkerHandleSlot},
};

/// Test-only server handle used by integration tests to exercise the real HTTP and WS entry points.
#[derive(Debug)]
pub struct TestServer {
    addr: SocketAddr,
    room_manager: Arc<RoomManager>,
    handle: JoinHandle<()>,
}

const TEST_POLL_DEADLINE: Duration = Duration::from_secs(3);

impl TestServer {
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}/", self.addr)
    }

    #[must_use]
    pub fn http_base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn wait_for_room_absence(&self, room_id: &str) -> bool {
        wait_for_test_predicate(|| async {
            (self.room_manager.get_by_uuid(room_id).await.is_none()).then_some(())
        })
        .await
    }

    pub async fn wait_for_consumer_route_active(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            true,
        )
        .await
    }

    pub async fn wait_for_consumer_route_inactive(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.wait_for_consumer_route_state(
            room_id,
            consumer_user_id,
            producer_user_id,
            stream_type,
            false,
        )
        .await
    }

    pub async fn wait_for_consumer_route_absence(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_manager.get_by_uuid(room_id).await?;
            matches!(
                room.consumer_route_state(consumer_user_id, producer_user_id, stream_type)
                    .await,
                Some(ConsumerRouteState::Absent)
            )
            .then_some(())
        })
        .await
    }

    async fn wait_for_consumer_route_state(
        &self,
        room_id: &str,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
        expected_active: bool,
    ) -> bool {
        wait_for_test_predicate(|| async {
            let room = self.room_manager.get_by_uuid(room_id).await?;
            let expected_state = if expected_active {
                ConsumerRouteState::Active
            } else {
                ConsumerRouteState::Inactive
            };
            matches!(
                room
                    .consumer_route_state(
                        consumer_user_id,
                        producer_user_id,
                        stream_type
                    )
                    .await,
                Some(state) if state == expected_state
            )
            .then_some(())
        })
        .await
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
    let options = RuntimeOptions::from_config(&config);
    let services = RuntimeServices::default();
    let room_manager = Arc::new(RoomManager::new(
        RoomManagerConfig::new(
            options.core.routing.media_worker_count,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(options.room.max_users),
                options.feature_flags(),
                router_rtp_capabilities(options.core.codecs.flags),
            )
            .with_room_sharding_policy(options.core.routing.room_sharding_policy),
        ),
        RoomManagerDeps {
            recording_media_tap: Arc::clone(&services.recording_media_tap),
            diagnostics: Arc::clone(&services.diagnostics),
            metrics: Arc::clone(&services.metrics),
        },
    ));
    let transport_adapter = build_transport_adapter(&options.core, &services)?;
    let bind_address = config.http.bind_address;
    let state = build_test_runtime_state(
        &config,
        Arc::clone(&room_manager),
        Arc::clone(&services.diagnostics),
        Arc::clone(&services.metrics),
        transport_adapter,
    );
    let listener = TcpListener::bind(bind_address).await?;
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
    Ok(TestServer {
        addr,
        room_manager,
        handle,
    })
}

/// Builds runtime state for in-crate tests and the exported `o_sfu::testing` harness.
///
/// This cannot be `#[cfg(test)]`: integration tests and fuzz targets use
/// `o_sfu::testing` as a normal dependency, so Rust compiles this module without
/// unit-test cfgs in those callers.
pub(in crate::runtime) fn build_test_runtime_state(
    config: &Config,
    room_manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: MediaTransport,
) -> RuntimeState {
    let options = RuntimeOptions::from_config(config);
    let media_core = SfuCore::new(options.core, transport_adapter.clone());
    RuntimeState {
        http_options: options.http,
        websocket_options: options.websocket,
        rooms: room_manager,
        diagnostics,
        transport_adapter,
        media_core,
        metrics,
    }
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

async fn wait_for_test_predicate<F, Fut>(mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<()>>,
{
    timeout(TEST_POLL_DEADLINE, async {
        loop {
            if predicate().await.is_some() {
                return Some(());
            }
            yield_now().await;
        }
    })
    .await
    .ok()
    .flatten()
    .is_some()
}
