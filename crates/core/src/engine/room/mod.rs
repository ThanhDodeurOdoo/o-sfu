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
mod media_graph;
mod membership;
mod operation;
mod outbound;
mod placement;
mod recording;
mod routing;
pub mod rtp_capabilities;
mod source_policy;
mod state;
#[cfg(any(test, feature = "testing-transport"))]
mod tests;
mod transition;
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
    JoinUserRequest, RoomManager, RoomManagerConfig, RoomManagerDeps, RoomUserAdmission,
    RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot,
};
pub use media_graph::{ConsumerRouteState, RemoteTrackSetup};
pub(crate) use operation::RoomUserOperation;
pub use outbound::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    RoomEventRequest, TrackBindingUpdate, UserOutbound, UserOutboundEvent, UserOutboundOverflow,
    UserOutboundOverflowKind, UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError,
    UserOutboundSender,
};
pub(in crate::engine::room) use placement::ResolvedPlacement;
pub use placement::{
    LocalRoomRouterPlacements, LocalRoomRouterPlacementsError, LocalRouterRuntimeContext,
    RoomRuntimeContext,
};
pub(in crate::engine::room) use source_policy::SourcePolicyEvent;
#[cfg(any(test, feature = "testing-transport"))]
pub use tests::api::{
    NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect, RoomTestLifecycle,
    RoomTestMedia,
};

#[cfg(any(test, feature = "testing-transport"))]
pub(in crate::engine::room) use self::{
    effects::RoomEffectContext, membership::JoinSessionIntent, placement::JoinPlacementPlan,
};
