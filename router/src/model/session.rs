use super::SessionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Active,
    Closed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionPermissions {
    transcription: bool,
    audio_recording: bool,
    video_recording: bool,
}

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

    pub fn set_permissions(&mut self, permissions: SessionPermissions) {
        self.permissions = permissions;
    }

    pub fn close(&mut self) {
        self.state = SessionState::Closed;
    }
}
