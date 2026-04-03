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

impl SessionPermissions {
    #[must_use]
    pub fn new(transcription: bool, audio_recording: bool, video_recording: bool) -> Self {
        Self {
            transcription,
            audio_recording,
            video_recording,
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

#[allow(
    clippy::struct_field_names,
    reason = "The session info fields intentionally mirror the established call-state vocabulary used across the signaling boundary."
)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionInfo {
    is_talking: Option<bool>,
    is_camera_on: Option<bool>,
    is_screen_sharing_on: Option<bool>,
    is_self_muted: Option<bool>,
    is_deaf: Option<bool>,
    is_raising_hand: Option<bool>,
}

impl SessionInfo {
    #[must_use]
    pub fn new(
        is_talking: Option<bool>,
        is_camera_on: Option<bool>,
        is_screen_sharing_on: Option<bool>,
        is_self_muted: Option<bool>,
        is_deaf: Option<bool>,
        is_raising_hand: Option<bool>,
    ) -> Self {
        Self {
            is_talking,
            is_camera_on,
            is_screen_sharing_on,
            is_self_muted,
            is_deaf,
            is_raising_hand,
        }
    }

    #[must_use]
    pub fn is_talking(&self) -> Option<bool> {
        self.is_talking
    }

    #[must_use]
    pub fn is_camera_on(&self) -> Option<bool> {
        self.is_camera_on
    }

    #[must_use]
    pub fn is_screen_sharing_on(&self) -> Option<bool> {
        self.is_screen_sharing_on
    }

    #[must_use]
    pub fn is_self_muted(&self) -> Option<bool> {
        self.is_self_muted
    }

    #[must_use]
    pub fn is_deaf(&self) -> Option<bool> {
        self.is_deaf
    }

    #[must_use]
    pub fn is_raising_hand(&self) -> Option<bool> {
        self.is_raising_hand
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    state: SessionState,
    permissions: SessionPermissions,
    info: SessionInfo,
}

impl Session {
    #[must_use]
    pub fn new(id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            id,
            state: SessionState::Active,
            permissions,
            info: SessionInfo::default(),
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

    #[must_use]
    pub fn info(&self) -> SessionInfo {
        self.info
    }

    pub fn set_permissions(&mut self, permissions: SessionPermissions) {
        self.permissions = permissions;
    }

    pub fn set_info(&mut self, info: SessionInfo) {
        self.info = info;
    }

    pub fn close(&mut self) {
        self.state = SessionState::Closed;
    }
}
