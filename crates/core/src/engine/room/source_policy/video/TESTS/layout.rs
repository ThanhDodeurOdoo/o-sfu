#![allow(
    clippy::expect_used,
    reason = "test source fixtures should fail loudly when descriptor construction regresses"
)]

use super::*;
use crate::engine::source_model::{
    PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
    SourceAdaptationPolicy, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
    SourceEncodingId, SourceLayoutPolicy, SourcePolicy, UserStreamId,
};

fn source_with_layout(policy: SourceLayoutPolicy) -> PublishedSourceDescriptor {
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id: PublishedSourceId::from_raw(1),
        owner: PublishedSourceOwner::new(UserId::Integer(1)),
        stream_id: UserStreamId::new("video"),
        media_kind: o_sfu_router::MediaKind::Video,
        policy: SourcePolicy::new(Some(policy), SourceAdaptationPolicy::None, None),
        mid: None,
        encodings: vec![SourceEncodingDescriptor::new(
            SourceEncodingDescriptorParts {
                encoding_id: SourceEncodingId::from_raw(1),
                source_id: PublishedSourceId::from_raw(1),
                rid: Some(o_sfu_router::Rid::new("hi")),
                primary_ssrc: None,
                repair_ssrc: None,
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
                policy_role: None,
                max_temporal_layer_id: None,
                negotiated_format: None,
            },
        )],
    })
    .expect("test source descriptor should be valid")
}

#[test]
fn active_speaker_is_default_when_source_policy_allows_it() {
    let source = source_with_layout(SourceLayoutPolicy::new(
        SourceRoomPolicySelector::VisibleThumbnail,
        Some(SourceRoomPolicySelector::ActiveSpeaker),
    ));
    let intent = ReceiverVideoLayoutIntent::resolve(&source, None, true);

    assert_eq!(intent.role(), SourceRoomPolicySelector::ActiveSpeaker);
    assert_eq!(intent.priority(), SourceRoutePriority::ActiveSpeaker);
    assert!(intent.uses_featured_quality());
}

#[test]
fn pinned_intent_overrides_active_speaker_default() {
    let source = source_with_layout(SourceLayoutPolicy::new(
        SourceRoomPolicySelector::VisibleThumbnail,
        Some(SourceRoomPolicySelector::ActiveSpeaker),
    ));
    let intent =
        ReceiverVideoLayoutIntent::resolve(&source, Some(VideoLayoutIntent::Pinned), false);

    assert_eq!(intent.role(), SourceRoomPolicySelector::Pinned);
    assert_eq!(intent.priority(), SourceRoutePriority::PinnedOrFeatured);
    assert!(intent.uses_featured_quality());
}

#[test]
fn explicit_hidden_source_does_not_use_featured_quality_even_when_speaking() {
    let source = source_with_layout(SourceLayoutPolicy::new(
        SourceRoomPolicySelector::VisibleThumbnail,
        Some(SourceRoomPolicySelector::ActiveSpeaker),
    ));
    let intent = ReceiverVideoLayoutIntent::resolve(&source, Some(VideoLayoutIntent::Hidden), true);

    assert_eq!(intent.role(), SourceRoomPolicySelector::Hidden);
    assert_eq!(intent.priority(), SourceRoutePriority::HiddenOrOverflow);
    assert!(!intent.uses_featured_quality());
    assert!(!intent.counts_toward_visible_budget());
}

#[test]
fn readable_detail_source_has_readability_priority_by_default() {
    let source = source_with_layout(SourceLayoutPolicy::new(
        SourceRoomPolicySelector::ReadableDetail,
        None,
    ));
    let intent = ReceiverVideoLayoutIntent::resolve(&source, None, false);

    assert_eq!(intent.role(), SourceRoomPolicySelector::ReadableDetail);
    assert_eq!(intent.priority(), SourceRoutePriority::ReadableDetail);
    assert!(intent.uses_featured_quality());
}
