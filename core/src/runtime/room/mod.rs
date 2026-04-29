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

mod cleanup;
mod controller;
mod definition;
mod directory;
mod effects;
mod events;
mod factory;
mod lifecycle;
mod manager;
mod media;
mod media_core;
mod media_transaction;
mod membership;
mod outbound;
mod recording;
mod router_state;
pub mod rtp_capabilities;
mod source_policy_sync;
mod state;
mod stream_role;
#[cfg(any(test, feature = "testing-transport"))]
mod tests;
mod topology;
mod user_negotiation;

pub use controller::{
    Room, RoomAdmissionPolicy, RoomConfig, RoomEventRequest, RoomJoinError, RoomManagerJoinError,
    RoomMediaCounts, RoomRuntimeContext, RoomRuntimePolicy, RoomUserStatsSnapshot,
    TrackBindingUpdate, UserOutbound,
};
pub use events::RoomEventMessage;
pub use lifecycle::{RoomUserPermissions, UserCloseReason};
pub use manager::{
    JoinUserRequest, RoomManager, RoomManagerConfig, RoomManagerDeps, RuntimeRoomDirectorySnapshot,
    RuntimeRoomStatsSnapshot,
};
#[cfg(any(test, feature = "testing-transport"))]
pub(in crate::runtime::room) use membership::UserCleanup;
pub use state::{ConsumerRouteState, RemoteTrackBootstrap};
