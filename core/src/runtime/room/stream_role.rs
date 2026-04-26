use o_sfu_router::MediaKind;

use crate::runtime::StreamType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum UserStreamRole {
    Microphone,
    Camera,
    Screen,
}

impl UserStreamRole {
    pub(crate) const fn from_protocol_stream_type(stream_type: StreamType) -> Self {
        match stream_type {
            StreamType::Audio => Self::Microphone,
            StreamType::Camera => Self::Camera,
            StreamType::Screen => Self::Screen,
        }
    }

    const fn media_kind(self) -> MediaKind {
        match self {
            Self::Microphone => MediaKind::Audio,
            Self::Camera | Self::Screen => MediaKind::Video,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UserStreamIntent {
    role: UserStreamRole,
}

impl UserStreamIntent {
    pub(crate) const fn from_protocol_stream_type(stream_type: StreamType) -> Self {
        Self {
            role: UserStreamRole::from_protocol_stream_type(stream_type),
        }
    }

    pub(crate) const fn media_kind(self) -> MediaKind {
        self.role.media_kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_roles_map_to_application_roles_and_media_kinds() {
        let microphone = UserStreamIntent::from_protocol_stream_type(StreamType::Audio);
        let camera = UserStreamIntent::from_protocol_stream_type(StreamType::Camera);
        let screen = UserStreamIntent::from_protocol_stream_type(StreamType::Screen);

        assert_eq!(microphone.role, UserStreamRole::Microphone);
        assert_eq!(microphone.media_kind(), MediaKind::Audio);
        assert_eq!(camera.role, UserStreamRole::Camera);
        assert_eq!(camera.media_kind(), MediaKind::Video);
        assert_eq!(screen.role, UserStreamRole::Screen);
        assert_eq!(screen.media_kind(), MediaKind::Video);
    }
}
