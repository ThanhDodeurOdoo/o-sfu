//! Session identity, lifecycle, and room-level permissions.
//! analogus to odoo rtc session

use super::SessionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// The session is admitted and may own transports and media.
    Active,
    /// The session has been closed and should no longer own router state.
    ///
    /// This is a terminal state. Once a session is closed, it is not expected
    /// to transition back to `Active`; callers should create a new session
    /// instead.
    Closed,
}

/// Permissions that affect session-owned capabilities outside the router core.
///
/// The router stores these flags because outer layers need them to remain bound
/// to session identity, but the flags do not directly change routing behavior.
///
/// TODO: may clean that up later since business logic leaks in the router core,
/// not a "big" deal but should find an elegant solution at some point, probably
/// not too hard to refactor out of the router core.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionPermissions {
    transcription: bool,
    audio_recording: bool,
    video_recording: bool,
}

/// a public version of [`SessionPermissions`]
/// it's to keep the router core private, but may be overkill,
/// will maybe refactor later.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionPermissionFlags {
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}

impl SessionPermissions {
    #[must_use]
    pub const fn from_flags(flags: SessionPermissionFlags) -> Self {
        Self {
            transcription: flags.transcription,
            audio_recording: flags.audio_recording,
            video_recording: flags.video_recording,
        }
    }

    #[must_use]
    pub fn transcription(&self) -> bool {
        self.transcription
    }

    #[must_use]
    pub fn audio_recording(&self) -> bool {
        self.audio_recording
    }

    #[must_use]
    pub fn video_recording(&self) -> bool {
        self.video_recording
    }
}

/// Router-owned session record.
///
/// `id` is the stable router identity, `state` tracks whether the session is
/// still live, and `permissions` preserves the room-level capabilities that
/// outer orchestration layers need to query alongside session ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    state: SessionState,
    permissions: SessionPermissions,
}

impl Session {
    #[must_use]
    pub fn new(id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            id,
            state: SessionState::Active,
            permissions,
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

    #[must_use]
    pub fn permissions(&self) -> SessionPermissions {
        self.permissions
    }

    /// Replace the session's permission snapshot.
    pub fn set_permissions(&mut self, permissions: SessionPermissions) {
        self.permissions = permissions;
    }

    /// Mark the session as closed before outer state tears it down.
    pub fn close(&mut self) {
        self.state = SessionState::Closed;
    }
}
