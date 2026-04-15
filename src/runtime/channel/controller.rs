//! Channel runtime layer: membership, bootstrap orchestration, and channel-local state.
//!
//! Internal modules:
//! - `manager`: server-global channel lookup, creation, and cleanup coordination
//! - `membership`: join/leave, session-info fan-out, and transport readiness
//! - `media`: producer/consumer bootstrap plus upload/download activity transitions
//! - `outbound`: shared outbound fan-out helpers for session handlers
//! - `state`: channel-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: compatibility bridge from signaling session ids into the pure router
//! - `topology`: channel-local routing placement boundary
//! - `rtp_capabilities`: default router RTP capability surface
//! - signaling edges own any legacy RTP/ORTC wire mapping; the channel boundary consumes
//!   router-native RTP capabilities, negotiated parameters, and track bootstrap data

use std::fmt;
use std::sync::Arc;

use o_sfu_router::RouterId;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::config::RuntimeFeatureFlags;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportSessionKey};
use crate::signaling::{
    protocol::{PeerSnapshot, WebSocketCloseCode},
    shared::{AvailableFeatures, RecordingState, SessionId, StreamType},
};

use super::{
    events::ChannelEventMessage,
    state::{ChannelState, RemoteTrackBootstrap},
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
    Close(WebSocketCloseCode),
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
    pub(crate) runtime: u64,
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

/// A single discussion channel owning sessions, features, and recording state,
/// roughly same concepts as in odoo's sfu and odoo discuss
/// Identity fields (uuid, issuer, key, features) are imuttable after creation.
/// Mutable state (sessions, recording, routing) is behind an interior lock.
/// TODO: update docstring later
pub struct Channel {
    pub(super) runtime_id: u64,
    pub(super) media_worker_id: usize,
    pub(super) uuid: String,
    pub(super) issuer: String,
    pub(super) key: Option<String>,
    pub(super) web_rtc_enabled: bool,
    pub(super) feature_flags: RuntimeFeatureFlags,
    #[allow(dead_code, reason = "stored for future recording pipeline integration")]
    pub(super) recording_address: Option<String>,
    #[allow(
        dead_code,
        reason = "recording control-plane wiring is intentionally deferred until the replacement baseline is validated"
    )]
    pub(super) recording_service: Arc<RecordingService>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) state: RwLock<ChannelState>,
}

impl Channel {
    pub(crate) fn new(
        runtime_context: ChannelRuntimeContext,
        runtime_policy: ChannelRuntimePolicy,
        issuer: String,
        key: Option<String>,
        config: ChannelConfig,
        recording_media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let recording_media_source: Arc<dyn MediaSource> = recording_media_tap;
        let recording_service = Arc::new(RecordingService::new(
            runtime_context.runtime,
            recording_media_source,
            Arc::clone(&metrics),
        ));
        Self {
            runtime_id: runtime_context.runtime,
            media_worker_id: runtime_context.media_worker,
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
            web_rtc_enabled: config.web_rtc_enabled,
            feature_flags: runtime_policy.feature_flags,
            recording_address: config.recording_address,
            recording_service: Arc::clone(&recording_service),
            metrics,
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
        &self.uuid
    }

    #[must_use]
    pub(crate) fn transport_session_key(
        &self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> TransportSessionKey {
        TransportSessionKey::new(
            self.runtime_id,
            self.media_worker_id,
            connection_id,
            session_id.clone(),
        )
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    #[must_use]
    pub fn available_features(&self) -> AvailableFeatures {
        AvailableFeatures {
            rtc: self.web_rtc_enabled,
            transcription: self.feature_flags.transcription,
            audio_recording: self.feature_flags.audio_recording,
            video_recording: self.feature_flags.video_recording,
        }
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
        self.web_rtc_enabled
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn media_worker_id(&self) -> usize {
        self.media_worker_id
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Channel")
            .field("runtime_id", &self.runtime_id)
            .field("media_worker_id", &self.media_worker_id)
            .field("uuid", &self.uuid)
            .field("issuer", &self.issuer)
            .field("web_rtc_enabled", &self.web_rtc_enabled)
            .finish_non_exhaustive()
    }
}
