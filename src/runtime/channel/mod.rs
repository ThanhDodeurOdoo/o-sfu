//! `channel` is the main business-flow layer of the SFU.
//!
//! ```text
//! ChannelManager
//! |- directory / definition -> process-global lookup and immutable identity
//! `- Channel
//!    |- controller          -> room-facing facade and immutable accessors
//!    |- lifecycle           -> single owner of join, leave, and cleanup sequencing
//!    |- effects             -> explicit side-effect plans for transport, fanout, and diagnostics
//!    |- state               -> locked mutable room model
//!    |- membership          -> session presence, permissions, and fanout
//!    |- session_negotiation -> per-session transport readiness
//!    |- media               -> publish and subscribe activity transitions
//!    |- recording           -> channel-scoped recording policy
//!    |- router_state        -> bridge into the router core
//!    |- topology            -> routing placement boundary
//!    |- outbound            -> shared server-to-client fanout helpers
//!    `- source_policy_sync -> room-owned video policy refresh bridge
//! ```

mod controller;
mod definition;
mod directory;
mod effects;
mod events;
mod factory;
mod lifecycle;
mod manager;
mod media;
mod media_transaction;
mod membership;
mod outbound;
mod recording;
mod router_state;
pub(crate) mod rtp_capabilities;
mod session_negotiation;
mod source_policy_sync;
mod state;
#[cfg(test)]
mod tests;
mod topology;

pub use controller::{Channel, ChannelJoinError, ChannelManagerJoinError, SessionOutbound};
pub(crate) use controller::{
    ChannelAdmissionPolicy, ChannelConfig, ChannelEventRequest, ChannelMediaCounts,
    ChannelRuntimeContext, ChannelRuntimePolicy, ChannelSessionStatsSnapshot, TrackBindingUpdate,
};
pub(crate) use events::ChannelEventMessage;
pub(crate) use lifecycle::{ChannelSessionPermissions, SessionCloseReason};
pub use manager::ChannelManager;
pub(crate) use manager::{
    ChannelManagerConfig, JoinSessionRequest, RuntimeChannelDirectorySnapshot,
    RuntimeChannelStatsSnapshot,
};
#[cfg(test)]
pub(in crate::runtime::channel) use membership::SessionCleanup;
pub(crate) use state::{ConsumerRouteState, RemoteTrackBootstrap};
