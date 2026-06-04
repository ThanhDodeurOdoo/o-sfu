//! Compatibility stream catalog for the current Odoo-facing protocol.
//!
//! This module translates discuss [`StreamType`] values into core source
//! intent. Application decisions such as which streams are bandwidth-scalable,
//! which streams may become active-speaker video and which streams should
//! preserve readable detail
//! belong here, not in core room state or the router.
//!
//! Core receives [`SourcePublishIntent`] and
//! [`crate::core::prelude::SourceSubscriptionIntent`] values with opaque [`UserStreamId`]
//! keys. It does not know whether a stream id came from Odoo's camera slot, a
//! screen-share slot or a custom stream.

use std::collections::BTreeMap;

use o_sfu_protocol::wire::{DownloadStates, StreamType, UserInfo};
use o_sfu_router::MediaKind;

use crate::core::prelude::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
    SourceLayoutPolicy, SourcePolicy, SourcePublishIntent, SourceRoomPolicySelector,
    SourceSubscriptionIntent, UserStreamId,
};

pub(crate) const AUDIO_STREAM_LABEL: &str = "audio";
pub(crate) const CAMERA_STREAM_LABEL: &str = "camera";
pub(crate) const SCREEN_STREAM_LABEL: &str = "screen";

const DISCUSS_STREAMS: [DiscussStream; 3] = [
    DiscussStream {
        stream_type: StreamType::Audio,
        label: AUDIO_STREAM_LABEL,
        media_kind: MediaKind::Audio,
        policy: SourcePolicy::new(
            None,
            SourceAdaptationPolicy::None,
            Some(ActiveSpeakerPolicy::new(
                ActiveSpeakerGroup::MAIN,
                ActiveSpeakerSourceRole::Detector,
            )),
        ),
    },
    DiscussStream {
        stream_type: StreamType::Camera,
        label: CAMERA_STREAM_LABEL,
        media_kind: MediaKind::Video,
        policy: SourcePolicy::new(
            Some(SourceLayoutPolicy::new(
                SourceRoomPolicySelector::VisibleThumbnail,
                Some(SourceRoomPolicySelector::ActiveSpeaker),
            )),
            SourceAdaptationPolicy::ScalableVideo,
            Some(ActiveSpeakerPolicy::new(
                ActiveSpeakerGroup::MAIN,
                ActiveSpeakerSourceRole::Promotable,
            )),
        ),
    },
    DiscussStream {
        stream_type: StreamType::Screen,
        label: SCREEN_STREAM_LABEL,
        media_kind: MediaKind::Video,
        policy: SourcePolicy::new(
            Some(SourceLayoutPolicy::new(
                SourceRoomPolicySelector::ReadableDetail,
                None,
            )),
            SourceAdaptationPolicy::ReadableDetail,
            None,
        ),
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiscussStream {
    stream_type: StreamType,
    label: &'static str,
    media_kind: MediaKind,
    policy: SourcePolicy,
}

impl DiscussStream {
    pub(crate) fn all() -> impl Iterator<Item = Self> {
        DISCUSS_STREAMS.into_iter()
    }

    pub(crate) const fn for_type(stream_type: StreamType) -> Self {
        match stream_type {
            StreamType::Audio => DISCUSS_STREAMS[0],
            StreamType::Camera => DISCUSS_STREAMS[1],
            StreamType::Screen => DISCUSS_STREAMS[2],
        }
    }

    pub(crate) fn for_stream_id(stream_id: &UserStreamId) -> Option<Self> {
        match stream_id.as_str() {
            AUDIO_STREAM_LABEL => Some(Self::for_type(StreamType::Audio)),
            CAMERA_STREAM_LABEL => Some(Self::for_type(StreamType::Camera)),
            SCREEN_STREAM_LABEL => Some(Self::for_type(StreamType::Screen)),
            _ => None,
        }
    }

    pub(crate) fn stream_id(self) -> UserStreamId {
        UserStreamId::new(self.label)
    }

    pub(crate) fn publish_intent(self) -> SourcePublishIntent {
        SourcePublishIntent::new(self.stream_id(), self.media_kind, self.policy)
    }

    pub(crate) fn subscription_intent_if_requested(
        self,
        states: &DownloadStates,
    ) -> Option<(UserStreamId, SourceSubscriptionIntent)> {
        let (active, layout) = match self.stream_type {
            StreamType::Audio => (states.audio, None),
            StreamType::Camera => (states.camera, states.camera_layout),
            StreamType::Screen => (states.screen, states.screen_layout),
        };
        let intent = SourceSubscriptionIntent::new(active, layout);
        (!intent.is_empty()).then(|| (self.stream_id(), intent))
    }

    pub(crate) fn publication_info(self, active: bool) -> Option<UserInfo> {
        match self.stream_type {
            StreamType::Audio => None,
            StreamType::Camera => Some(UserInfo {
                is_camera_on: Some(active),
                ..UserInfo::default()
            }),
            StreamType::Screen => Some(UserInfo {
                is_screen_sharing_on: Some(active),
                ..UserInfo::default()
            }),
        }
    }
}

/// Build the core publish intent for one discuss upload request.
///
/// [`User`](crate::application::user_session::User) calls this before
/// [`crate::core::prelude::MediaPublication::stage`].
/// Changing this mapping changes how new publications behave in core policy,
/// including media kind, layout role, receiver bandwidth adaptation and active
/// speaker participation.
pub(crate) fn source_publish_intent_for_stream_type(
    stream_type: StreamType,
) -> SourcePublishIntent {
    DiscussStream::for_type(stream_type).publish_intent()
}

/// Return the stable source id used to represent one discuss stream slot.
///
/// These strings are compatibility ids, not core policy names. Core stores
/// them only as opaque per-user stream keys. Any decision tied to a discuss slot
/// must be expressed through [`DiscussStream`].
pub(crate) fn stream_id_for_stream_type(stream_type: StreamType) -> UserStreamId {
    DiscussStream::for_type(stream_type).stream_id()
}

/// Project a core stream id back to the current Odoo wire shape.
///
/// Unknown stream ids are valid for core, but cannot be represented by the
/// discuss protocol. Callers use `None` as the signal to omit that source from
/// compatibility-only track snapshots.
pub(crate) fn stream_type_for_stream_id(stream_id: &UserStreamId) -> Option<StreamType> {
    DiscussStream::for_stream_id(stream_id).map(|stream| stream.stream_type)
}

/// Read a per-stream counter through a discuss stream label.
///
/// HTTP stats still expose compatibility-shaped `audio`, `camera` and `screen`
/// buckets. This helper keeps that projection at the application edge while
/// diagnostics and room state stay keyed by [`UserStreamId`].
pub(crate) fn counter_for_stream_type(
    by_stream: &BTreeMap<UserStreamId, u64>,
    stream_type: StreamType,
) -> u64 {
    by_stream
        .get(&stream_id_for_stream_type(stream_type))
        .copied()
        .unwrap_or(0)
}

/// Read a diagnostics bitrate bucket by core stream id.
///
/// Diagnostics may contain streams that do not map back to discuss
/// [`StreamType`] values, so this helper accepts raw stream ids.
pub(crate) fn diagnostics_bitrate_for_stream_id(
    by_stream: &BTreeMap<String, u64>,
    stream_id: &str,
) -> u64 {
    by_stream.get(stream_id).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::wire::VideoLayoutIntent;

    use super::*;

    #[test]
    fn maps_discuss_stream_labels_to_stable_core_ids() {
        assert_eq!(
            stream_id_for_stream_type(StreamType::Audio).as_str(),
            AUDIO_STREAM_LABEL
        );
        assert_eq!(
            stream_id_for_stream_type(StreamType::Camera).as_str(),
            CAMERA_STREAM_LABEL
        );
        assert_eq!(
            stream_id_for_stream_type(StreamType::Screen).as_str(),
            SCREEN_STREAM_LABEL
        );
        assert_eq!(
            stream_type_for_stream_id(&UserStreamId::new(AUDIO_STREAM_LABEL)),
            Some(StreamType::Audio)
        );
        assert_eq!(
            stream_type_for_stream_id(&UserStreamId::new(CAMERA_STREAM_LABEL)),
            Some(StreamType::Camera)
        );
        assert_eq!(
            stream_type_for_stream_id(&UserStreamId::new(SCREEN_STREAM_LABEL)),
            Some(StreamType::Screen)
        );
        assert_eq!(
            stream_type_for_stream_id(&UserStreamId::new("custom-source")),
            None
        );
    }

    #[test]
    fn maps_discuss_streams_to_core_source_policy() {
        assert_audio_policy(&source_publish_intent_for_stream_type(StreamType::Audio));
        assert_camera_policy(&source_publish_intent_for_stream_type(StreamType::Camera));
        assert_screen_policy(&source_publish_intent_for_stream_type(StreamType::Screen));
    }

    fn assert_audio_policy(intent: &SourcePublishIntent) {
        assert_eq!(intent.stream_id().as_str(), AUDIO_STREAM_LABEL);
        assert_eq!(intent.media_kind(), MediaKind::Audio);

        let policy = intent.policy();
        assert_eq!(policy.layout(), None);
        assert_eq!(policy.adaptation(), SourceAdaptationPolicy::None);
        assert_eq!(
            policy.active_speaker(),
            Some(ActiveSpeakerPolicy::new(
                ActiveSpeakerGroup::MAIN,
                ActiveSpeakerSourceRole::Detector,
            ))
        );
    }

    fn assert_camera_policy(intent: &SourcePublishIntent) {
        assert_eq!(intent.stream_id().as_str(), CAMERA_STREAM_LABEL);
        assert_eq!(intent.media_kind(), MediaKind::Video);

        let policy = intent.policy();
        assert_eq!(policy.adaptation(), SourceAdaptationPolicy::ScalableVideo);
        assert_eq!(
            policy.active_speaker(),
            Some(ActiveSpeakerPolicy::new(
                ActiveSpeakerGroup::MAIN,
                ActiveSpeakerSourceRole::Promotable,
            ))
        );

        assert_eq!(
            policy.layout().map(|layout| layout.resolve(None, false)),
            Some(SourceRoomPolicySelector::VisibleThumbnail)
        );
        assert_eq!(
            policy.layout().map(|layout| layout.resolve(None, true)),
            Some(SourceRoomPolicySelector::ActiveSpeaker)
        );
        assert_eq!(
            policy
                .layout()
                .map(|layout| layout.resolve(Some(VideoLayoutIntent::Pinned), true)),
            Some(SourceRoomPolicySelector::Pinned)
        );
    }

    fn assert_screen_policy(intent: &SourcePublishIntent) {
        assert_eq!(intent.stream_id().as_str(), SCREEN_STREAM_LABEL);
        assert_eq!(intent.media_kind(), MediaKind::Video);

        let policy = intent.policy();
        assert_eq!(policy.adaptation(), SourceAdaptationPolicy::ReadableDetail);
        assert_eq!(policy.active_speaker(), None);

        assert_eq!(
            policy.layout().map(|layout| layout.resolve(None, false)),
            Some(SourceRoomPolicySelector::ReadableDetail)
        );
        assert_eq!(
            policy.layout().map(|layout| layout.resolve(None, true)),
            Some(SourceRoomPolicySelector::ReadableDetail)
        );
    }
}
