//! `room` is the main business-flow layer of the SFU.
//!
//! ```text
//! RoomManager
//! |- directory / definition -> process-global lookup and immutable identity
//! `- Room
//!    |- controller          -> room-facing facade and immutable accessors
//!    |- lifecycle           -> single owner of join, leave, and cleanup sequencing
//!    |- effects             -> explicit side-effect plans for transport, fanout, and diagnostics
//!    |- state               -> locked mutable room model
//!    |- membership          -> user presence, permissions, and fanout
//!    |- user_negotiation -> per-user transport readiness
//!    |- media               -> publish and subscribe activity transitions
//!    |- recording           -> room-scoped recording policy
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
mod source_policy_sync;
mod state;
mod stream_role;
#[cfg(test)]
mod tests;
mod topology;
mod user_negotiation;

pub use controller::{Room, RoomJoinError, RoomManagerJoinError, UserOutbound};
pub(crate) use controller::{
    RoomAdmissionPolicy, RoomConfig, RoomEventRequest, RoomMediaCounts, RoomRuntimeContext,
    RoomRuntimePolicy, RoomUserStatsSnapshot, TrackBindingUpdate,
};
pub(crate) use events::RoomEventMessage;
pub(crate) use lifecycle::{RoomUserPermissions, UserCloseReason};
pub use manager::RoomManager;
pub(crate) use manager::{
    JoinUserRequest, RoomManagerConfig, RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot,
};
#[cfg(test)]
pub(in crate::runtime::room) use membership::UserCleanup;
pub(crate) use state::{ConsumerRouteState, RemoteTrackBootstrap};
