//! Channel runtime layer: membership, bootstrap orchestration, and channel-local state.
//!
//! Internal modules:
//! - `controller`: channel identity, immutable configuration, and shared accessors
//! - `manager`: server-global channel lookup, creation, and cleanup coordination
//! - `membership`: join/leave, session-info fan-out, and transport readiness
//! - `media`: producer/consumer bootstrap plus upload/download activity transitions
//! - `outbound`: shared outbound fan-out helpers for session handlers
//! - `session_negotiation`: explicit transport/bootstrap readiness state for one session
//! - `state`: channel-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: compatibility bridge from signaling session ids into the pure router
//! - `topology`: channel-local routing placement boundary
//! - `rtp_capabilities`: default router RTP capability surface
//! - the legacy signaling edge owns RTP/ORTC wire mapping through `crate::signaling::ortc_mapper`
//!   until the native Phase 9 negotiation path replaces the current websocket protocol

mod controller;
mod manager;
mod media;
mod membership;
mod outbound;
mod router_state;
mod rtp_capabilities;
mod session_negotiation;
mod state;
#[cfg(test)]
mod tests;
mod topology;

pub use controller::{Channel, ChannelJoinError, ChannelManagerJoinError, SessionOutbound};
pub(crate) use controller::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelRuntimeContext, ChannelSessionStatsSnapshot,
};
pub use manager::ChannelManager;
pub(crate) use manager::ChannelManagerConfig;
pub(crate) use manager::JoinSessionRequest;
pub(crate) use manager::RuntimeChannelStatsSnapshot;
