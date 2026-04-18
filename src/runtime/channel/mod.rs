//! `channel` is the main business-flow layer of the SFU. It receives authenticated
//! session intent from signaling, owns the mutable room model, cordinates lifecycle
//! transitions, and bridge room policy into the router and transport layers without
//! exposing those lower-level details back to the socket code.
//!
//! ```text
//! ChannelManager
//! |- directory / definition -> process-global lookup and immutable identity
//! `- Channel
//!    |- controller          -> room-facing facade and immutable accessors
//!    |- lifecycle           -> single owner of join, leave, and cleanup sequencing
//!    |- state               -> locked mutable room model
//!    |- membership          -> session presence, permissions, and fanout
//!    |- session_negotiation -> per-session transport readiness
//!    |- media               -> publish and subscribe activity transitions
//!    |- recording           -> channel-scoped recording policy
//!    |- router_state        -> bridge into the router core
//!    |- topology            -> routing placement boundary
//!    |- outbound            -> shared server-to-client fanout helpers
//!    `- source_packet_policy-> room-owned packet gate intent for transport execution
//! ```
//!
//! Supporting modules such as `events`, `factory`, `media_transaction`, and
//! `rtp_capabilities` exist to keep these ownership edges small rather than to define
//! separate business roots of their own.

mod controller;
mod definition;
mod directory;
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
pub(crate) use lifecycle::{ChannelSessionPermissions, SessionCloseReason};
pub use manager::ChannelManager;
pub(crate) use manager::ChannelManagerConfig;
pub(crate) use manager::JoinSessionRequest;
pub(crate) use manager::RuntimeChannelStatsSnapshot;
pub(crate) use membership::SessionCleanupPolicy;
pub(crate) use state::RemoteTrackBootstrap;
#[cfg(test)]
pub(crate) use tests::api::NegotiatedPublish;
