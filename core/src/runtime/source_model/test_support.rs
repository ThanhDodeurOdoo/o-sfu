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
use crate::runtime::{DownloadStates, StreamType};

pub(crate) fn source_publish_intent_for_stream_type(
    stream_type: StreamType,
) -> SourcePublishIntent {
    SourcePublishIntent::new(
        stream_id_for_stream_type(stream_type),
        media_kind_for_stream_type(stream_type),
        source_policy_for_stream_type(stream_type),
    )
}

pub(crate) fn stream_id_for_stream_type(stream_type: StreamType) -> UserStreamId {
    match stream_type {
        StreamType::Audio => UserStreamId::new("audio"),
        StreamType::Camera => UserStreamId::new("camera"),
        StreamType::Screen => UserStreamId::new("screen"),
    }
}

pub(crate) fn stream_type_for_stream_id(stream_id: &UserStreamId) -> Option<StreamType> {
    match stream_id.as_str() {
        "audio" => Some(StreamType::Audio),
        "camera" => Some(StreamType::Camera),
        "screen" => Some(StreamType::Screen),
        _ => None,
    }
}

pub(crate) fn subscription_intents_from_download_states(
    states: &DownloadStates,
) -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    let mut intents = BTreeMap::new();
    if states.audio.is_some() {
        intents.insert(
            stream_id_for_stream_type(StreamType::Audio),
            SourceSubscriptionIntent::new(states.audio, None),
        );
    }
    if states.camera.is_some() || states.camera_layout.is_some() {
        intents.insert(
            stream_id_for_stream_type(StreamType::Camera),
            SourceSubscriptionIntent::new(states.camera, states.camera_layout),
        );
    }
    if states.screen.is_some() || states.screen_layout.is_some() {
        intents.insert(
            stream_id_for_stream_type(StreamType::Screen),
            SourceSubscriptionIntent::new(states.screen, states.screen_layout),
        );
    }
    intents
}

const fn media_kind_for_stream_type(stream_type: StreamType) -> MediaKind {
    match stream_type {
        StreamType::Audio => MediaKind::Audio,
        StreamType::Camera | StreamType::Screen => MediaKind::Video,
    }
}

const fn source_policy_for_stream_type(stream_type: StreamType) -> SourcePolicy {
    match stream_type {
        StreamType::Audio => SourcePolicy::new(
            None,
            SourceAdaptationPolicy::None,
            Some(ActiveSpeakerPolicy::new(
                ActiveSpeakerGroup::MAIN,
                ActiveSpeakerSourceRole::Detector,
            )),
        ),
        StreamType::Camera => SourcePolicy::new(
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
        StreamType::Screen => SourcePolicy::new(
            Some(SourceLayoutPolicy::new(
                SourceRoomPolicySelector::ReadableDetail,
                None,
            )),
            SourceAdaptationPolicy::ReadableDetail,
            None,
        ),
    }
}
