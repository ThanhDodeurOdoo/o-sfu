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
//! - `rtp_conversion`: translation between wire RTP JSON and router-native RTP types

use std::fmt;

use o_sfu_router::RouterId;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::transport_adapter::{RuntimeTransportAdapter, TransportSessionKey};
use crate::signaling::{
    current_protocol::{CurrentServerMessage, CurrentServerRequest, CurrentWebSocketCloseCode},
    shared::{AvailableFeatures, RecordingState, SessionId, StreamType},
};

mod manager;
mod media;
mod membership;
mod outbound;
mod router_state;
mod rtp_capabilities;
mod rtp_conversion;
mod state;
#[cfg(test)]
mod tests;
mod topology;

pub use manager::ChannelManager;
pub(crate) use manager::RuntimeChannelStatsSnapshot;
use state::ChannelState;

#[derive(Debug, Clone)]
pub enum SessionOutbound {
    Message(CurrentServerMessage),
    Request(Box<CurrentServerRequest>),
    Close(CurrentWebSocketCloseCode),
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
/// Identity fields (uuid, issuer, key, features) are immutable after creation.
/// Mutable state (sessions, recording, routing) is behind an interior lock.
/// TODO: update docstring later
pub struct Channel {
    runtime_id: u64,
    media_worker_id: usize,
    uuid: String,
    issuer: String,
    key: Option<String>,
    web_rtc_enabled: bool,
    #[allow(dead_code, reason = "stored for future recording pipeline integration")]
    recording_address: Option<String>,
    state: RwLock<ChannelState>,
}

impl Channel {
    pub(super) fn new(
        runtime_id: u64,
        media_worker_id: usize,
        router_id: RouterId,
        issuer: String,
        key: Option<String>,
        config: ChannelConfig,
    ) -> Self {
        Self {
            runtime_id,
            media_worker_id,
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
            web_rtc_enabled: config.web_rtc_enabled,
            recording_address: config.recording_address,
            state: RwLock::new(ChannelState::new(router_id)),
        }
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    #[must_use]
    pub(super) fn transport_session_key(
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
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state.clone()
    }

    pub async fn router_rtp_capabilities(&self) -> o_sfu_router::RtpCapabilities {
        self.state.read().await.topology.rtp_capabilities().clone()
    }

    pub(super) async fn session_stats_snapshot(
        &self,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> ChannelSessionStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = state
            .sessions
            .iter()
            .map(|(session_id, session)| {
                TransportSessionKey::new(
                    self.runtime_id,
                    self.media_worker_id,
                    session.connection_id,
                    session_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        let transport_snapshot = transport_adapter.transport_bitrate_snapshot(&session_keys);
        let mut aggregated_bitrate = IncomingBitrateSnapshot {
            total: transport_snapshot.total,
            ..Default::default()
        };
        for (transport_media_id, bits) in transport_snapshot.per_media {
            let Some(stream_type) = state.producers.values().find_map(|producer| {
                if producer.transport_media_id == Some(transport_media_id) {
                    Some(producer.stream_type)
                } else {
                    None
                }
            }) else {
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
        ChannelSessionStatsSnapshot {
            incoming_bitrate: aggregated_bitrate,
            count: state.topology.session_count(),
            camera_count: state.topology.camera_count(),
            screen_count: state.topology.screen_count(),
        }
    }

    #[must_use]
    pub(super) fn web_rtc_enabled(&self) -> bool {
        self.web_rtc_enabled
    }

    #[cfg(test)]
    #[must_use]
    pub(super) const fn media_worker_id(&self) -> usize {
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
