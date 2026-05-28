//! Session identity and lifecycle.

use super::SessionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// The session is admitted and may have transports and media.
    Active,
    /// The session has been closed and should no longer have router state.
    ///
    /// This is a terminal state. Once a session is closed, it is not expected
    /// to transition back to [`SessionState::Active`], callers should create a new session
    /// instead.
    Closed,
}

/// Router session record.
///
/// the id is the stable router identity and state tracks whether the session is
/// still live. Application-level permissions live above the router because they
/// do not change pure media routing invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    state: SessionState,
}

impl Session {
    #[must_use]
    pub fn new(id: SessionId) -> Self {
        Self {
            id,
            state: SessionState::Active,
        }
    }

    #[must_use]
    pub fn id(&self) -> SessionId {
        self.id
    }

    #[must_use]
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Mark the session as closed before outer state tears it down.
    pub fn close(&mut self) {
        self.state = SessionState::Closed;
    }
}
