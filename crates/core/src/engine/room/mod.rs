#[cfg(any(test, feature = "testing-transport"))]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod definition;
mod directory;
mod effects;
mod factory;
mod instance;
mod manager;
mod media_graph;
mod membership;
mod outbound;
mod placement;
mod read_model;
mod recording;
mod source_policy;
mod state;
mod transition;

#[cfg(any(test, feature = "testing-transport"))]
pub use TESTS::api::{
    NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect, RoomTestLifecycle,
    RoomTestMedia,
};
pub use factory::{RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy};
pub(crate) use instance::RoomUserOperation;
pub use instance::{Room, RoomJoinError, RoomManagerJoinError, RoomMediaCounts};
pub use manager::{
    RoomManager, RoomManagerConfig, RoomManagerDeps, RoomUserAdmission,
    RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use media_graph::ConsumerRouteState;
pub use membership::{JoinUserRequest, RoomUserPermissions, UserCloseReason};
pub use outbound::{
    BroadcastPayload, BroadcastPayloadError, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY, MAX_BROADCAST_PAYLOAD_BYTES, RemoteSourceProjection,
    RemoteSourceSnapshot, RoomEventMessage, UserOutbound, UserOutboundEvent, UserOutboundOverflow,
    UserOutboundOverflowKind, UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError,
    UserOutboundSender,
};
pub use placement::{RoomRuntimeContext, RouterPlacement, RouterPlacements, RouterPlacementsError};
pub use read_model::{IncomingBitrateSnapshot, RoomUserStatsSnapshot};
pub(crate) use transition::{PublishIntentOutcome, UnpublishIntentOutcome};

#[cfg(any(test, feature = "testing-transport"))]
pub use self::effects::batch::RoomEffectContext;
