//! Pure room-state model split by responsibility.
//!
//! `ids` owns the typed runtime-only producer/consumer identifiers.
//! `fanout` owns outbound fan-out preparation from state snapshots.
//! `recording` owns room recording-control state updates and fan-out.
//! `shared` owns the room state shell and room-user presentation projection.
//! `membership` owns user lifecycle, presence fan-out, and negotiation readiness.
//! `media` owns producer/consumer workflows and the media graph indexes.
//! `test_support` owns read-only state inspectors used only by tests.

mod diagnostics;
mod fanout;
mod ids;
mod media;
mod membership;
mod recording;
mod shared;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

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
};
