//! Channel runtime layer: membership, bootstrap orchestration, and channel-local state.
//!
//! Internal modules:
//! - `manager`: server-global channel lookup, creation, and cleanup coordination
//! - `membership`: join/leave, session-info fan-out, and transport readiness
//! - `media`: producer/consumer bootstrap plus upload/download activity transitions
//! - `outbound`: shared outbound fan-out helpers for session handlers
//! - `state`: channel-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: compatibility bridge from signaling session ids into the pure router
//! - `rtp_capabilities`: default router RTP capability surface
//! - `rtp_conversion`: translation between wire RTP JSON and router-native RTP types

use std::fmt;

use o_sfu_router::RouterId;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::transport_adapter::RuntimeTransportAdapter;
use crate::signaling::{
    current_protocol::{CurrentServerMessage, CurrentServerRequest, CurrentWebSocketCloseCode},
    http::{ChannelStats, CreateChannelQuery, SessionsStats},
    shared::{AvailableFeatures, RecordingState},
};
use crate::utils::rfc3339_now;

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

pub use manager::ChannelManager;
use state::ChannelState;

/// A message the server pushes to a connected session's WebSocket handler.
#[derive(Debug, Clone)]
pub enum SessionOutbound {
    /// A fire-and-forget server message wrapped in a Bus envelope by the handler.
    Message(CurrentServerMessage),
    /// A request-style server event wrapped in a Bus envelope by the handler.
    Request(Box<CurrentServerRequest>),
    /// Instruct the handler to close the WebSocket with the given code.
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

/// A single discussion channel owning sessions, features, and recording state.
///
/// Identity fields (uuid, issuer, key, features) are immutable after creation.
/// Mutable state (sessions, recording, routing) is behind an interior lock.
pub struct Channel {
    create_date: String,
    uuid: String,
    issuer: String,
    key: Option<String>,
    remote_address: String,
    web_rtc_enabled: bool,
    #[allow(dead_code, reason = "stored for future recording pipeline integration")]
    recording_address: Option<String>,
    state: RwLock<ChannelState>,
}

impl Channel {
    pub(super) fn new(
        router_id: RouterId,
        issuer: String,
        key: Option<String>,
        remote_address: String,
        query: &CreateChannelQuery,
    ) -> Self {
        Self {
            create_date: rfc3339_now(),
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
            remote_address,
            web_rtc_enabled: query.web_rtc_enabled(),
            recording_address: query.recording_address.clone(),
            state: RwLock::new(ChannelState::new(router_id)),
        }
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    #[cfg(test)]
    #[must_use]
    pub fn create_date(&self) -> &str {
        &self.create_date
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
        self.state.read().await.router.rtp_capabilities().clone()
    }

    pub async fn stats(&self, transport_adapter: &RuntimeTransportAdapter) -> ChannelStats {
        let state = self.state.read().await;
        let session_ids = state.sessions.keys().cloned().collect::<Vec<_>>();
        let incoming_bitrate = transport_adapter.incoming_bitrate_snapshot(&session_ids);
        ChannelStats {
            create_date: self.create_date.clone(),
            uuid: self.uuid.clone(),
            remote_address: self.remote_address.clone(),
            sessions_stats: SessionsStats {
                incoming_bit_rate: incoming_bitrate.to_stats(),
                count: state.router.session_count(),
                camera_count: state.router.camera_count(),
                screen_count: state.router.screen_count(),
            },
            web_rtc_enabled: self.web_rtc_enabled,
        }
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Channel")
            .field("create_date", &self.create_date)
            .field("uuid", &self.uuid)
            .field("issuer", &self.issuer)
            .field("remote_address", &self.remote_address)
            .field("web_rtc_enabled", &self.web_rtc_enabled)
            .finish_non_exhaustive()
    }
}
