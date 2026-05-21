//! Pure room-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `fanout` owns outbound fan-out preparation from state snapshots.
//! `layout` owns server-driven user layout state.
//! `presence` owns client-driven user presence state.
//! `recording` owns room recording-control state updates and fan-out.
//! `user_info_projection` owns outward-facing user projection.
//! `source_policy` owns room-level source selection input, planning, projection, and commit.
//! `shared` owns the room state shell.
//! `membership` owns user lifecycle, presence fan-out, and negotiation readiness.
//! `media` owns producer/consumer workflows and the media graph indexes.
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
mod source_policy;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;
mod user_info_projection;

pub use self::media::{ConsumerRouteState, RemoteTrackBootstrap};
pub(in crate::runtime::room) use self::{
    media::{
        ConsumerBootstrapOrigin, ConsumerRouteTransportRef, ConsumerRouteUpdate,
        PendingConsumerBootstrap, PendingConsumerBootstrapTarget, PlannedConsumerBootstrap,
        PlannedSubscriptionChange, PreparedConsumerBootstrap, RelayRouteEffect, RelayRouteKey,
        TransportMediaRemoval, ValidatedPublishDescriptor,
    },
    membership::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects},
    shared::RoomState,
    source_policy::{ConsumerPacketSelectionUpdate, FeaturedUserUpdate},
};
