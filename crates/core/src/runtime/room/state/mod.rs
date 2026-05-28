//! Pure room-state model split by responsibility.
//!
//! `fanout` owns outbound fan-out preparation from state snapshots.
//! `recording` owns room recording-control state updates and fan-out.
//! `shared` owns the room state shell, composes the room media graph and
//! projects room-user presentation state.
//! `membership` owns user lifecycle, presence fan-out, and negotiation readiness.
//! `test_support` owns read-only state inspectors used only by tests.

mod diagnostics;
mod fanout;
mod membership;
mod recording;
mod shared;
#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

pub(in crate::runtime::room) use self::{
    membership::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects},
    shared::RoomState,
};
