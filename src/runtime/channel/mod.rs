//! Channel runtime layer: membership, bootstrap orchestration, and channel-local state.
//!
//! Internal modules:
//! - `controller`: channel identity, immutable configuration, and shared accessors
//! - `manager`: server-global channel lookup, creation, and cleanup coordination
//! - `membership`: join/leave, session-info fan-out, and publish/consume readiness
//! - `media`: producer/consumer bootstrap plus publication/subscription activity transitions
//! - `outbound`: shared outbound fan-out helpers for session handlers
//! - `session_negotiation`: explicit transport/bootstrap readiness state for one session
//! - `source_packet_policy`: room-owned source-layer policy orchestration for transport media
//! - `state`: channel-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: compatibility bridge from signaling session ids into the pure router
//! - `topology`: channel-local routing placement boundary
//! - `rtp_capabilities`: default router RTP capability surface
//! - signaling edges own any legacy RTP/ORTC wire mapping; the channel boundary consumes
//!   router-native RTP capabilities, negotiated parameters, and track bootstrap data

mod controller;
mod events;
mod manager;
mod media;
mod membership;
mod outbound;
mod recording;
mod router_state;
pub(crate) mod rtp_capabilities;
mod session_negotiation;
mod source_packet_policy;
mod state;
#[cfg(test)]
mod tests;
mod topology;

pub use controller::{Channel, ChannelJoinError, ChannelManagerJoinError, SessionOutbound};
pub(crate) use controller::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelEventRequest, ChannelRuntimeContext,
    ChannelRuntimePolicy, ChannelSessionStatsSnapshot, TrackBindingUpdate,
};
pub(crate) use events::ChannelEventMessage;
pub use manager::ChannelManager;
pub(crate) use manager::ChannelManagerConfig;
pub(crate) use manager::JoinSessionRequest;
pub(crate) use manager::RuntimeChannelStatsSnapshot;
pub(crate) use media::NegotiatedPublish;
pub(crate) use membership::SessionCleanupPolicy;
pub(crate) use state::RemoteTrackBootstrap;
