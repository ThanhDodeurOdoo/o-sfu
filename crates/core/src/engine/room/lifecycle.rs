use crate::engine::UserPermissions;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoomUserPermissions {
    transcription: bool,
    audio_recording: bool,
    video_recording: bool,
}

impl RoomUserPermissions {
    #[must_use]
    pub(crate) const fn transcription(self) -> bool {
        self.transcription
    }

    #[must_use]
    pub(crate) const fn audio_recording(self) -> bool {
        self.audio_recording
    }

    #[must_use]
    pub(crate) const fn video_recording(self) -> bool {
        self.video_recording
    }
}

impl From<UserPermissions> for RoomUserPermissions {
    fn from(value: UserPermissions) -> Self {
        Self {
            transcription: value.transcription.unwrap_or(false),
            audio_recording: value.audio_recording.unwrap_or(false),
            video_recording: value.video_recording.unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCloseReason {
    Replaced,
    RemovedByRuntime,
}
