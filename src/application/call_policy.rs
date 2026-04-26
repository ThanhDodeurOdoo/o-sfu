use o_sfu_protocol::shared::{StreamType, UserInfo};
use o_sfu_router::MediaKind;

/// Application-owned publication slot.
///
/// Core media routing only needs the technical [`MediaKind`]. Compatibility
/// stream names are kept here so adding another application slot does not add
/// another core media identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CallPublicationSlot {
    Odoo(StreamType),
    #[allow(
        dead_code,
        reason = "custom app slots are the extension point that keeps new business media choices out of core stream enums"
    )]
    CustomAudio(&'static str),
}

impl CallPublicationSlot {
    pub(crate) const fn from_stream_type(stream_type: StreamType) -> Self {
        Self::Odoo(stream_type)
    }

    #[cfg(test)]
    pub(crate) const fn background_music() -> Self {
        Self::CustomAudio("background_music")
    }

    pub(crate) const fn media_kind(self) -> MediaKind {
        match self {
            Self::Odoo(StreamType::Audio) | Self::CustomAudio(_) => MediaKind::Audio,
            Self::Odoo(StreamType::Camera | StreamType::Screen) => MediaKind::Video,
        }
    }

    pub(crate) const fn compatibility_stream_type(self) -> Option<StreamType> {
        match self {
            Self::Odoo(stream_type) => Some(stream_type),
            Self::CustomAudio(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn default_odoo_slots() -> [Self; 3] {
        [
            Self::Odoo(StreamType::Audio),
            Self::Odoo(StreamType::Camera),
            Self::Odoo(StreamType::Screen),
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
    fn odoo_default_slots_remain_one_audio_camera_and_screen_slot() {
        let slots = CallPublicationSlot::default_odoo_slots();

        assert_eq!(slots.len(), 3);
        assert_eq!(
            slots[0].compatibility_stream_type(),
            Some(StreamType::Audio)
        );
        assert_eq!(slots[0].media_kind(), MediaKind::Audio);
        assert_eq!(
            slots[1].compatibility_stream_type(),
            Some(StreamType::Camera)
        );
        assert_eq!(slots[1].media_kind(), MediaKind::Video);
        assert_eq!(
            slots[2].compatibility_stream_type(),
            Some(StreamType::Screen)
        );
        assert_eq!(slots[2].media_kind(), MediaKind::Video);
    }

    #[test]
    fn extra_application_audio_slot_does_not_need_a_core_stream_type() {
        let slot = CallPublicationSlot::background_music();

        assert_eq!(slot.media_kind(), MediaKind::Audio);
        assert_eq!(slot.compatibility_stream_type(), None);
        assert_ne!(
            slot,
            CallPublicationSlot::from_stream_type(StreamType::Audio)
        );
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
