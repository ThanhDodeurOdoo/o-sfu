#[cfg(any(test, feature = "testing-transport"))]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod cleanup;
mod definition;
mod directory;
mod effects;
mod events;
mod factory;
mod init;
mod instance;
mod lifecycle;
mod manager;
mod media;
mod media_graph;
mod membership;
mod operation;
mod outbound;
mod placement;
mod read_model;
mod recording;
mod routing;
pub mod rtp_capabilities;
mod source_policy;
mod state;
mod transition;
mod user_negotiation;

#[cfg(any(test, feature = "testing-transport"))]
pub use TESTS::api::{
    NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect, RoomTestLifecycle,
    RoomTestMedia,
};
pub use events::{
    BroadcastPayload, BroadcastPayloadError, MAX_BROADCAST_PAYLOAD_BYTES, RoomEventMessage,
};
pub use init::{RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy};
pub use instance::{Room, RoomJoinError, RoomManagerJoinError, RoomMediaCounts};
pub use lifecycle::{RoomUserPermissions, UserCloseReason};
pub use manager::{
    RoomManager, RoomManagerConfig, RoomManagerDeps, RoomUserAdmission,
    RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use media_graph::ConsumerRouteState;
pub use media_graph::RemoteTrackSetup;
pub use membership::JoinUserRequest;
pub(crate) use operation::RoomUserOperation;
pub use outbound::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    TrackBindingUpdate, UserOutbound, UserOutboundEvent, UserOutboundOverflow,
    UserOutboundOverflowKind, UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError,
    UserOutboundSender,
};
pub use placement::{
    LocalRoomRouterPlacements, LocalRoomRouterPlacementsError, LocalRouterRuntimeContext,
    RoomRuntimeContext,
};
pub use read_model::{IncomingBitrateSnapshot, RoomUserStatsSnapshot};
pub use source_policy::SourcePolicyEvent;

#[cfg(any(test, feature = "testing-transport"))]
pub use self::effects::batch::RoomEffectContext;
