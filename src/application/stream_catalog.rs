//! Compatibility stream catalog for the current Odoo-facing protocol.
//!
//! # Boundary role
//!
//! This module is the production ownership point for translating discuss
//! [`StreamType`] values into generic core source intent. Business decisions
//! such as which streams are bandwidth-scalable, which streams may become
//! active-speaker video and which streams should preserve readable detail
//! belong here, not in core room state or the router.
//!
//! Core receives [`SourcePublishIntent`] and
//! [`crate::core::SourceSubscriptionIntent`] values with opaque [`UserStreamId`]
//! keys. It does not know whether a stream id came from Odoo's camera slot, a
//! screen-share slot or a future custom stream.

use std::collections::BTreeMap;

use o_sfu_protocol::shared::StreamType;
use o_sfu_router::MediaKind;

use crate::core::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
    SourceLayoutPolicy, SourcePolicy, SourcePublishIntent, SourceRoomPolicySelector, UserStreamId,
};

pub(crate) const AUDIO_STREAM_LABEL: &str = "audio";
pub(crate) const CAMERA_STREAM_LABEL: &str = "camera";
pub(crate) const SCREEN_STREAM_LABEL: &str = "screen";

/// Build the core publish intent for one discuss upload request.
///
/// `UserSession` calls this before [`crate::core::MediaSession::stage_publish`].
/// Changing this mapping changes how future publications behave in core policy,
/// including media kind, layout role, receiver bandwidth adaptation and active
/// speaker participation.
pub(crate) fn source_publish_intent_for_stream_type(
    stream_type: StreamType,
) -> SourcePublishIntent {
    SourcePublishIntent::new(
        stream_id_for_stream_type(stream_type),
        media_kind_for_stream_type(stream_type),
        source_policy_for_stream_type(stream_type),
    )
}

/// Return the stable source id used to represent one discuss stream slot.
///
/// These strings are compatibility ids, not core policy names. Core stores
/// them only as opaque per-user stream keys. Any decision tied to a discuss slot
/// must be expressed through [`source_policy_for_stream_type`].
pub(crate) fn stream_id_for_stream_type(stream_type: StreamType) -> UserStreamId {
    match stream_type {
        StreamType::Audio => UserStreamId::new(AUDIO_STREAM_LABEL),
        StreamType::Camera => UserStreamId::new(CAMERA_STREAM_LABEL),
        StreamType::Screen => UserStreamId::new(SCREEN_STREAM_LABEL),
    }
}

/// Project a generic core stream id back to the current Odoo wire shape.
///
/// Unknown stream ids are valid for core, but cannot be represented by the
/// discuss protocol. Callers use `None` as the signal to omit that source from
/// compatibility-only track snapshots.
pub(crate) fn stream_type_for_stream_id(stream_id: &UserStreamId) -> Option<StreamType> {
    match stream_id.as_str() {
        AUDIO_STREAM_LABEL => Some(StreamType::Audio),
        CAMERA_STREAM_LABEL => Some(StreamType::Camera),
        SCREEN_STREAM_LABEL => Some(StreamType::Screen),
        _ => None,
    }
}

/// Read a generic per-stream counter through a discuss stream label.
///
/// HTTP stats still expose compatibility-shaped `audio`, `camera` and `screen`
/// buckets. This helper keeps that projection at the application edge while
/// diagnostics and room state stay keyed by [`UserStreamId`].
pub(crate) fn value_for_stream_type(
    by_stream: &BTreeMap<UserStreamId, u64>,
    stream_type: StreamType,
) -> u64 {
    by_stream
        .get(&stream_id_for_stream_type(stream_type))
        .copied()
        .unwrap_or(0)
}

/// Read a diagnostics bitrate bucket by generic stream id.
///
/// Diagnostics may contain streams that do not map back to discuss
/// [`StreamType`] values, so this helper intentionally accepts raw stream ids.
pub(crate) fn diagnostics_bitrate_for_stream_id(
    by_stream: &BTreeMap<String, u64>,
    stream_id: &str,
) -> u64 {
    by_stream.get(stream_id).copied().unwrap_or(0)
}

const fn media_kind_for_stream_type(stream_type: StreamType) -> MediaKind {
    match stream_type {
        StreamType::Audio => MediaKind::Audio,
        StreamType::Camera | StreamType::Screen => MediaKind::Video,
    }
}

/// Business policy matrix for the current discuss stream catalog.
///
/// This is the place to change product behavior. The current mapping means:
///
/// - audio contributes active-speaker detection but is not receiver-video
///   budgeted
/// - camera is scalable video, visible as a thumbnail by default and can be
///   promoted by active-speaker policy
/// - screen share is video that favors readable detail and is protected from
///   normal thumbnail downgrades
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
