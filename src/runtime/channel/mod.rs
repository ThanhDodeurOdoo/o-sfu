//! Channel runtime layer: membership, bootstrap orchestration, and channel-local state.
//!
//! Internal modules:
//! - `controller`: channel identity, immutable configuration, and shared accessors
//! - `manager`: server-global channel lookup, creation, and cleanup coordination
//! - `membership`: join/leave, session-info fan-out, and transport readiness
//! - `media`: producer/consumer bootstrap plus upload/download activity transitions
//! - `outbound`: shared outbound fan-out helpers for session handlers
//! - `state`: channel-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: compatibility bridge from signaling session ids into the pure router
//! - `topology`: channel-local routing placement boundary
//! - `rtp_capabilities`: default router RTP capability surface
//! - signaling edge owns RTP/ORTC wire mapping through `crate::signaling::ortc_mapper`

mod controller;
mod manager;
mod media;
mod membership;
mod outbound;
mod router_state;
mod rtp_capabilities;
mod state;
#[cfg(test)]
mod tests;
mod topology;

pub use controller::{Channel, ChannelJoinError, ChannelManagerJoinError, SessionOutbound};
pub(crate) use controller::{ChannelConfig, ChannelSessionStatsSnapshot};
pub use manager::ChannelManager;
pub(crate) use manager::RuntimeChannelStatsSnapshot;
