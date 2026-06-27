mod diagnostics;
mod membership;
mod shared;
#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

pub use self::{
    membership::{
        ConnectionCloseCommit, DisconnectCommit, JoinCommit, LifecycleEffects, PresenceCommit,
        RemoteSourceRefresh,
    },
    shared::RoomState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UserJoinedFanout {
    Emit,
    #[cfg(any(test, feature = "testing-transport"))]
    Suppress,
}
