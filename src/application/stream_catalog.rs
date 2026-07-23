use std::collections::BTreeMap;

use o_sfu_protocol::wire::{DownloadStates, StreamType, UserInfo};
use o_sfu_router::MediaKind;

use crate::core::prelude::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
    SourceDeactivateIntent, SourceLayoutPolicy, SourcePolicy, SourcePublishIntent,
    SourceRoomPolicySelector, SourceSubscriptionIntent, UserStreamId,
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
            .with_presence(self.publication_presence(true))
    }

    pub(crate) fn deactivate_intent(self) -> SourceDeactivateIntent {
        SourceDeactivateIntent::new(self.stream_id())
            .with_presence(self.publication_presence(false))
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

    fn publication_presence(self, active: bool) -> Option<UserInfo> {
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

pub(crate) fn source_publish_intent_for_stream_type(
    stream_type: StreamType,
) -> SourcePublishIntent {
    DiscussStream::for_type(stream_type).publish_intent()
}

pub(crate) fn stream_id_for_stream_type(stream_type: StreamType) -> UserStreamId {
    DiscussStream::for_type(stream_type).stream_id()
}

pub(crate) fn stream_type_for_stream_id(stream_id: &UserStreamId) -> Option<StreamType> {
    DiscussStream::for_stream_id(stream_id).map(|stream| stream.stream_type)
}

pub(crate) fn counter_for_stream_type(
    by_stream: &BTreeMap<UserStreamId, u64>,
    stream_type: StreamType,
) -> u64 {
    by_stream
        .get(&stream_id_for_stream_type(stream_type))
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "TESTS/stream_catalog.rs"]
mod tests;
