//! Channel runtime layer: membership, bootstrap orchestration, and channel-local state.
//!
//! Internal modules:
//! - `manager`: server-global channel lookup, creation, and cleanup coordination
//! - `membership`: join/leave, session-info fan-out, and transport readiness
//! - `media`: producer/consumer bootstrap plus upload/download activity transitions
//! - `outbound`: shared outbound fan-out helpers for session handlers
//! - `state`: channel-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: post-auth bridge from signaling session ids into the router core
//! - `topology`: channel-local routing placement boundary
//! - `rtp_capabilities`: default router RTP capability surface
//! - signaling edges own the protocol wire mapping; the channel boundary consumes
//!   browser codec baseline RTP capabilities, negotiated parameters, and track bootstrap data

use std::fmt;
use std::sync::Arc;

use o_sfu_router::RouterId;
use tokio::sync::{Mutex, RwLock};

use o_sfu_protocol::{
    shared::{AvailableFeatures, RecordingState, SessionId, StreamType},
    signaling::PeerSnapshot,
};

use crate::config::RuntimeFeatureFlags;
use crate::runtime::diagnostics::{
    DiagnosticsQualitySummary, DiagnosticsSessionTransport, DiagnosticsSessionView,
    DiagnosticsStore,
};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportSessionKey};
use crate::runtime::{ChannelRuntimeId, ConnectionId};

use super::{
    definition::ChannelDefinition,
    events::ChannelEventMessage,
    lifecycle::SessionCloseReason,
    media_transaction::PendingPublishTransactions,
    state::{ChannelState, ConsumerRouteState, RemoteTrackBootstrap},
};

#[derive(Debug, Clone)]
pub(crate) struct TrackBindingUpdate {
    pub(crate) session_id: SessionId,
    pub(crate) stream_type: StreamType,
    pub(crate) active: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum SessionOutbound {
    Message(ChannelEventMessage),
    Request(Box<ChannelEventRequest>),
    TrackBindingUpdate(TrackBindingUpdate),
    Close(SessionCloseReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChannelEventRequest {
    BootstrapRemoteTrack(RemoteTrackBootstrap),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelJoinError {
    ChannelFull,
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelManagerJoinError {
    MissingChannel,
    ChannelFull,
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChannelAdmissionPolicy {
    pub(crate) max_sessions: usize,
}

impl ChannelAdmissionPolicy {
    #[must_use]
    pub(crate) const fn new(max_sessions: usize) -> Self {
        Self { max_sessions }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChannelRuntimeContext {
    pub(crate) runtime: ChannelRuntimeId,
    pub(crate) media_worker: usize,
    pub(crate) router: RouterId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelRuntimePolicy {
    pub(crate) admission_policy: ChannelAdmissionPolicy,
    pub(crate) feature_flags: RuntimeFeatureFlags,
    pub(crate) router_rtp_capabilities: o_sfu_router::MediaCapabilities,
}

impl ChannelRuntimePolicy {
    #[must_use]
    pub(crate) fn new(
        admission_policy: ChannelAdmissionPolicy,
        feature_flags: RuntimeFeatureFlags,
        router_rtp_capabilities: o_sfu_router::MediaCapabilities,
    ) -> Self {
        Self {
            admission_policy,
            feature_flags,
            router_rtp_capabilities,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelConfig {
    pub(crate) web_rtc_enabled: bool,
    pub(crate) recording_address: Option<String>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            web_rtc_enabled: true,
            recording_address: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IncomingBitrateSnapshot {
    pub(crate) total: u64,
    pub(crate) audio: u64,
    pub(crate) camera: u64,
    pub(crate) screen: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChannelSessionStatsSnapshot {
    pub(crate) incoming_bitrate: IncomingBitrateSnapshot,
    pub(crate) count: u64,
    pub(crate) camera_count: u64,
    pub(crate) screen_count: u64,
}

/// Analogus to a odoo discuss channel
///
/// `Channel` owns immutable room definition plus the guarded mutable state needed to run
/// membership, routing, and recording for that room. Callers are expected to express
/// room-level intents through this facade, while process-level lookup and lifecycle
/// serialization stay in [`super::manager::ChannelManager`].
pub struct Channel {
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) definition: ChannelDefinition,
    #[allow(
        dead_code,
        reason = "recording control-plane wiring is intentionally deferred until the replacement baseline is validated"
    )]
    pub(super) recording_service: Arc<RecordingService>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) pending_publish_transactions: Mutex<PendingPublishTransactions>,
    pub(super) state: RwLock<ChannelState>,
}

impl Channel {
    #[allow(
        clippy::too_many_arguments,
        reason = "channel construction keeps runtime identity, policy, and shared services explicit at the boundary"
    )]
    pub(crate) fn new(
        runtime_context: ChannelRuntimeContext,
        runtime_policy: ChannelRuntimePolicy,
        issuer: String,
        key: Option<String>,
        config: ChannelConfig,
        diagnostics: Arc<DiagnosticsStore>,
        recording_media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let definition =
            ChannelDefinition::new(runtime_context, &runtime_policy, issuer, key, config);
        let recording_media_source: Arc<dyn MediaSource> = recording_media_tap;
        let recording_service = Arc::new(RecordingService::new(
            definition.runtime_id(),
            recording_media_source,
            Arc::clone(&metrics),
        ));
        Self {
            diagnostics,
            definition,
            recording_service: Arc::clone(&recording_service),
            metrics,
            pending_publish_transactions: Mutex::new(PendingPublishTransactions::default()),
            state: RwLock::new(ChannelState::new(
                runtime_context.router,
                runtime_policy.admission_policy,
                runtime_policy.router_rtp_capabilities,
                recording_service,
            )),
        }
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        self.definition.uuid()
    }

    #[must_use]
    pub(crate) fn transport_session_key(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.definition
            .transport_session_key(session_id, connection_id)
    }

    pub(crate) async fn consumer_route_state(
        &self,
        consumer_session_id: &SessionId,
        producer_session_id: &SessionId,
        stream_type: StreamType,
    ) -> Option<ConsumerRouteState> {
        self.state.read().await.consumer_route_state(
            consumer_session_id,
            producer_session_id,
            stream_type,
        )
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        self.definition.issuer()
    }

    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.definition.key()
    }

    #[must_use]
    pub fn available_features(&self) -> AvailableFeatures {
        self.definition.available_features()
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state()
    }

    pub(crate) async fn peer_snapshots_except(
        &self,
        excluded_session_id: &SessionId,
    ) -> Vec<PeerSnapshot> {
        self.state
            .read()
            .await
            .peer_snapshots_except(excluded_session_id)
    }

    pub async fn router_rtp_capabilities(&self) -> o_sfu_router::MediaCapabilities {
        self.state.read().await.router_rtp_capabilities()
    }

    pub(crate) async fn session_stats_snapshot(
        &self,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> ChannelSessionStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = state
            .transport_session_entries()
            .into_iter()
            .map(|(session_id, connection_id)| {
                self.transport_session_key(&session_id, connection_id)
            })
            .collect::<Vec<_>>();
        let transport_snapshot = transport_adapter.transport_bitrate_snapshot(&session_keys);
        let mut aggregated_bitrate = IncomingBitrateSnapshot {
            total: transport_snapshot.total,
            ..Default::default()
        };
        for (transport_media_id, bits) in transport_snapshot.per_media {
            let Some(stream_type) =
                state.producer_stream_type_for_transport_media_id(transport_media_id)
            else {
                continue;
            };
            match stream_type {
                StreamType::Audio => {
                    aggregated_bitrate.audio = aggregated_bitrate.audio.saturating_add(bits);
                }
                StreamType::Camera => {
                    aggregated_bitrate.camera = aggregated_bitrate.camera.saturating_add(bits);
                }
                StreamType::Screen => {
                    aggregated_bitrate.screen = aggregated_bitrate.screen.saturating_add(bits);
                }
            }
        }
        let (count, camera_count, screen_count) = state.session_stats_counts();
        drop(state);
        ChannelSessionStatsSnapshot {
            incoming_bitrate: aggregated_bitrate,
            count,
            camera_count,
            screen_count,
        }
    }

    #[must_use]
    pub(crate) fn web_rtc_enabled(&self) -> bool {
        self.definition.web_rtc_enabled()
    }

    #[must_use]
    pub(crate) fn recording_enabled(&self) -> bool {
        self.definition.recording_enabled()
    }

    #[must_use]
    pub(crate) fn media_worker_id(&self) -> usize {
        self.definition.media_worker_id()
    }

    #[must_use]
    pub(crate) fn runtime_id(&self) -> ChannelRuntimeId {
        self.definition.runtime_id()
    }

    #[must_use]
    pub(crate) fn feature_flags(&self) -> RuntimeFeatureFlags {
        self.definition.feature_flags()
    }

    pub(crate) async fn diagnostics_session_views(
        &self,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Vec<DiagnosticsSessionView> {
        let state = self.state.read().await;
        let session_entries = state.transport_session_entries();
        let session_keys = session_entries
            .iter()
            .map(|(session_id, connection_id)| {
                self.transport_session_key(session_id, *connection_id)
            })
            .collect::<Vec<_>>();
        let transport_snapshot = transport_adapter.transport_bitrate_snapshot(&session_keys);
        let incoming_bitrate_by_session =
            state.diagnostics_incoming_bitrate_by_session(&transport_snapshot.per_media);
        let transport_by_session = session_entries
            .into_iter()
            .map(|(session_id, connection_id)| {
                let transport = DiagnosticsSessionTransport {
                    connection_id: connection_id.as_u64(),
                    health: transport_adapter
                        .session_transport_health(
                            &self.transport_session_key(&session_id, connection_id),
                        )
                        .map(Into::into),
                    media_worker_id: self.definition.media_worker_id(),
                    quality_summary: DiagnosticsQualitySummary {
                        current_incoming_bitrate: incoming_bitrate_by_session
                            .get(&session_id)
                            .cloned()
                            .unwrap_or_default(),
                        sampled_metrics_available: false,
                    },
                };
                (session_id, transport)
            })
            .collect();
        state.diagnostics_session_views(self.definition.media_worker_id(), &transport_by_session)
    }

    pub(crate) async fn diagnostics_matching_session(
        &self,
        requested_session_id: &str,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<(DiagnosticsSessionView, SessionId)> {
        self.diagnostics_session_views(transport_adapter)
            .await
            .into_iter()
            .find(|session| session_id_matches(&session.session_id, requested_session_id))
            .map(|session| {
                let session_id = session.session_id.clone();
                (session, session_id)
            })
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Channel")
            .field("runtime_id", &self.definition.runtime_id())
            .field("media_worker_id", &self.definition.media_worker_id())
            .field("uuid", &self.definition.uuid())
            .field("issuer", &self.definition.issuer())
            .field("web_rtc_enabled", &self.definition.web_rtc_enabled())
            .finish_non_exhaustive()
    }
}

fn session_id_matches(session_id: &SessionId, requested_session_id: &str) -> bool {
    match session_id {
        SessionId::Integer(value) => value.to_string() == requested_session_id,
        SessionId::String(value) => value == requested_session_id,
    }
}
