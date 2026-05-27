//! `room` is the main business-flow layer of the SFU.
//!
//! ```text
//! RoomManager
//! |- directory / definition -> process-global lookup and immutable identity
//! `- Room
//!    |- controller          -> room-facing facade and immutable accessors
//!    |- lifecycle           -> close reasons and permission translation
//!    |- membership          -> join, leave and disconnect transition orchestration
//!    |- operation           -> user-scoped membership, media and publish work
//!    |- cleanup             -> transport cleanup execution and retry reconciliation
//!    |- effects             -> explicit side-effect plans for transport, fanout, and diagnostics
//!    |- state               -> locked mutable room model
//!    |- user_negotiation    -> per-user transport readiness
//!    |- media               -> room-wide consumer bootstrap and publication lookup
//!    |- recording           -> room-scoped recording policy
//!    |- router_state        -> bridge into the router core
//!    |- source_policy       -> room-owned source selection and refresh bridge
//!    |- topology            -> routing placement boundary
//!    |- outbound            -> shared server-to-client fanout helpers
//! ```

mod cleanup;
mod controller;
mod definition;
mod directory;
mod effects;
mod events;
mod factory;
mod init;
mod lifecycle;
mod manager;
mod media;
mod media_transaction;
mod membership;
mod operation;
mod outbound;
mod placement;
mod recording;
mod router_state;
pub mod rtp_capabilities;
mod source_policy;
mod state;
#[cfg(any(test, feature = "testing-transport"))]
mod tests;
mod topology;
mod user_negotiation;

pub use controller::{
    IncomingBitrateSnapshot, Room, RoomJoinError, RoomManagerJoinError, RoomMediaCounts,
    RoomUserStatsSnapshot,
};
pub use events::{
    BroadcastPayload, BroadcastPayloadError, MAX_BROADCAST_PAYLOAD_BYTES, RoomEventMessage,
};
pub use init::{RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy};
pub use lifecycle::{RoomUserPermissions, UserCloseReason};
pub use manager::{
    JoinUserRequest, RoomManager, RoomManagerConfig, RoomManagerDeps, RuntimeRoomDirectorySnapshot,
    RuntimeRoomStatsSnapshot,
};
pub(crate) use operation::RoomUserOperation;
pub use outbound::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    RoomEventRequest, TrackBindingUpdate, UserOutbound, UserOutboundEvent, UserOutboundOverflow,
    UserOutboundOverflowKind, UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError,
    UserOutboundSender,
};
pub(in crate::runtime::room) use placement::ResolvedPlacement;
pub use placement::{
    LocalRoomRouterPlacements, LocalRoomRouterPlacementsError, LocalRouterRuntimeContext,
    RoomRuntimeContext,
};
pub(in crate::runtime::room) use source_policy::SourcePolicyEvent;
pub use state::{ConsumerRouteState, RemoteTrackBootstrap};
#[cfg(any(test, feature = "testing-transport"))]
pub use tests::api::{
    NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect, RoomTestLifecycle,
    RoomTestMedia,
};

#[cfg(any(test, feature = "testing-transport"))]
pub(in crate::runtime::room) use self::{
    effects::RoomEffectContext, membership::JoinSessionIntent, placement::JoinPlacementPlan,
};
