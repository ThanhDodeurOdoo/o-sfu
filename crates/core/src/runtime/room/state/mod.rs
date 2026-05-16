//! Pure room-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `fanout` owns outbound fan-out preparation from state snapshots.
//! `layout` owns server-driven user layout state.
//! `presence` owns client-driven user presence state.
//! `recording` owns room recording-control state updates and fan-out.
//! `user_info_projection` owns outward-facing user projection.
//! `video_policy` owns room-level source selection input, planning, projection, and commit.
//! `shared` owns the in-memory state and bookkeeping types.
//! `membership` owns user lifecycle, presence fan-out, and negotiation readiness.
//! `media` owns producer/consumer bootstrap and routing-side media bookkeeping.
//! `test_support` owns read-only state inspectors used only by tests.

mod diagnostics;
mod fanout;
mod ids;
mod layout;
mod media;
mod membership;
mod presence;
mod recording;
mod shared;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;
mod user_info_projection;
mod video_policy;

pub use self::media::{ConsumerRouteState, RemoteTrackBootstrap};
pub(in crate::runtime::room) use self::{
    media::{
        ConsumerBootstrapOrigin, ConsumerRouteUpdate, PendingConsumerBootstrap,
        PendingConsumerBootstrapTarget, PlannedConsumerBootstrap, PlannedSubscriptionChange,
        PreparedConsumerBootstrap, RelayRouteEffect, RelayRouteKey, ValidatedPublishDescriptor,
    },
    membership::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects},
    shared::{ConsumerRouteTransportRef, RoomState, TransportMediaRemoval},
    video_policy::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
};
