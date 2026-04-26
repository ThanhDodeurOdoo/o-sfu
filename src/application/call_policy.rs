use o_sfu_protocol::shared::{StreamType, UserInfo};
use o_sfu_router::MediaKind;

/// Application-owned publication slot.
///
/// Core media routing only needs the technical [`MediaKind`]. Stream labels are
/// kept here as app policy so future call products can choose their publication
/// slots without adding core media identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CallPublicationSlot {
    stream_type: StreamType,
}

impl CallPublicationSlot {
    pub(crate) const fn from_stream_type(stream_type: StreamType) -> Self {
        Self { stream_type }
    }

    pub(crate) const fn media_kind(self) -> MediaKind {
        match self.stream_type {
            StreamType::Audio => MediaKind::Audio,
            StreamType::Camera | StreamType::Screen => MediaKind::Video,
        }
    }

    pub(crate) const fn stream_type(self) -> StreamType {
        self.stream_type
    }

    #[cfg(test)]
    pub(crate) const fn default_slots() -> [Self; 3] {
        [
            Self::from_stream_type(StreamType::Audio),
            Self::from_stream_type(StreamType::Camera),
            Self::from_stream_type(StreamType::Screen),
        ]
    }
}

#[allow(
    dead_code,
    reason = "presence ownership is established in the application layer before the remaining room-state migration consumes this type directly"
)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CallPresence {
    talking: Option<bool>,
    self_muted: Option<bool>,
    deaf: Option<bool>,
    raising_hand: Option<bool>,
}

impl CallPresence {
    pub(crate) const fn from_user_info(info: &UserInfo) -> Self {
        Self {
            talking: info.is_talking,
            self_muted: info.is_self_muted,
            deaf: info.is_deaf,
            raising_hand: info.is_raising_hand,
        }
    }

    #[allow(
        clippy::unused_self,
        reason = "the method documents that application presence currently has no implicit packet-routing effect"
    )]
    pub(crate) const fn affects_media_routing(self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_slots_remain_one_audio_camera_and_screen_slot() {
        let slots = CallPublicationSlot::default_slots();

        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].stream_type(), StreamType::Audio);
        assert_eq!(slots[0].media_kind(), MediaKind::Audio);
        assert_eq!(slots[1].stream_type(), StreamType::Camera);
        assert_eq!(slots[1].media_kind(), MediaKind::Video);
        assert_eq!(slots[2].stream_type(), StreamType::Screen);
        assert_eq!(slots[2].media_kind(), MediaKind::Video);
    }

    #[test]
    fn presence_flags_are_business_state_not_media_routing_state() {
        let presence = CallPresence::from_user_info(&UserInfo {
            is_talking: Some(true),
            is_raising_hand: Some(true),
            ..UserInfo::default()
        });

        assert_eq!(
            presence,
            CallPresence {
                talking: Some(true),
                self_muted: None,
                deaf: None,
                raising_hand: Some(true),
            }
        );
        assert!(!presence.affects_media_routing());
    }
}
