mod diagnostics;
mod membership;
mod shared;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

pub use self::{
    membership::{DisconnectUsersOutcome, JoinUserOutcome, LeaveUserOutcome, LifecycleEffects},
    shared::RoomState,
};
