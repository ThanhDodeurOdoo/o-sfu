#![allow(
    dead_code,
    reason = "room test helpers are compiled in normal builds so downstream integration tests can use core test APIs"
)]

use std::collections::BTreeMap;

use o_sfu_router::MediaKind;

use super::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
    SourceLayoutPolicy, SourcePolicy, SourcePublishIntent, SourceRoomPolicySelector,
    SourceSubscriptionIntent, UserStreamId,
};
use crate::runtime::VideoLayoutIntent;

const AUDIO_DETECTOR_SOURCE_ID: &str = "test-audio-detector";
const SCALABLE_VIDEO_SOURCE_ID: &str = "test-scalable-video";
const READABLE_VIDEO_SOURCE_ID: &str = "test-readable-video";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestSourceKind {
    AudioDetector,
    ScalableVideo,
    ReadableVideo,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TestSubscriptionStates {
    pub audio_detector: Option<bool>,
    pub scalable_video: Option<bool>,
    pub readable_video: Option<bool>,
    pub scalable_video_layout: Option<VideoLayoutIntent>,
    pub readable_video_layout: Option<VideoLayoutIntent>,
}

#[must_use]
pub fn source_publish_intent_for_source(source: TestSourceKind) -> SourcePublishIntent {
    SourcePublishIntent::new(
        stream_id_for_source(source),
        media_kind_for_source(source),
        source_policy_for_source(source),
    )
}

#[must_use]
pub fn stream_id_for_source(source: TestSourceKind) -> UserStreamId {
    match source {
        TestSourceKind::AudioDetector => UserStreamId::new(AUDIO_DETECTOR_SOURCE_ID),
        TestSourceKind::ScalableVideo => UserStreamId::new(SCALABLE_VIDEO_SOURCE_ID),
        TestSourceKind::ReadableVideo => UserStreamId::new(READABLE_VIDEO_SOURCE_ID),
    }
}

#[must_use]
pub fn source_kind_for_stream_id(stream_id: &UserStreamId) -> Option<TestSourceKind> {
    match stream_id.as_str() {
        AUDIO_DETECTOR_SOURCE_ID => Some(TestSourceKind::AudioDetector),
        SCALABLE_VIDEO_SOURCE_ID => Some(TestSourceKind::ScalableVideo),
        READABLE_VIDEO_SOURCE_ID => Some(TestSourceKind::ReadableVideo),
        _ => None,
    }
}

#[must_use]
pub fn subscription_intents_from_test_states(
    states: &TestSubscriptionStates,
) -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    let mut intents = BTreeMap::new();
    if states.audio_detector.is_some() {
        intents.insert(
            stream_id_for_source(TestSourceKind::AudioDetector),
            SourceSubscriptionIntent::new(states.audio_detector, None),
        );
    }
    if states.scalable_video.is_some() || states.scalable_video_layout.is_some() {
        intents.insert(
            stream_id_for_source(TestSourceKind::ScalableVideo),
            SourceSubscriptionIntent::new(states.scalable_video, states.scalable_video_layout),
        );
    }
    if states.readable_video.is_some() || states.readable_video_layout.is_some() {
        intents.insert(
            stream_id_for_source(TestSourceKind::ReadableVideo),
            SourceSubscriptionIntent::new(states.readable_video, states.readable_video_layout),
        );
    }
    intents
}

const fn media_kind_for_source(source: TestSourceKind) -> MediaKind {
    match source {
        TestSourceKind::AudioDetector => MediaKind::Audio,
        TestSourceKind::ScalableVideo | TestSourceKind::ReadableVideo => MediaKind::Video,
    }
}

const fn source_policy_for_source(source: TestSourceKind) -> SourcePolicy {
    match source {
        TestSourceKind::AudioDetector => SourcePolicy::new(
            None,
            SourceAdaptationPolicy::None,
            Some(ActiveSpeakerPolicy::new(
                ActiveSpeakerGroup::MAIN,
                ActiveSpeakerSourceRole::Detector,
            )),
        ),
        TestSourceKind::ScalableVideo => SourcePolicy::new(
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
        TestSourceKind::ReadableVideo => SourcePolicy::new(
            Some(SourceLayoutPolicy::new(
                SourceRoomPolicySelector::ReadableDetail,
                None,
            )),
            SourceAdaptationPolicy::ReadableDetail,
            None,
        ),
    }
}
