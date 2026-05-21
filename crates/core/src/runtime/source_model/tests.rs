#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "test assertions use expect, unwrap and direct indexing for direct fixture failures"
)]

use o_sfu_rfc::rtp::frame_marking;
use o_sfu_router::{MediaCodec, MediaFormat, MediaKind, Mid, PayloadType, Rid, Ssrc};

use super::*;
use crate::{Bitrate, runtime::UserId};

fn video_format(payload_type: u8) -> MediaFormat {
    MediaFormat::new(
        MediaKind::Video,
        MediaCodec::from("VP8"),
        PayloadType::new(payload_type),
        90_000,
    )
}

fn source_encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: &str,
) -> SourceEncodingDescriptor {
    source_encoding_with_options(
        source_id,
        encoding_id,
        Some(rid),
        Some(Bitrate::from_kbps(150 * encoding_id.as_u64())),
    )
}

fn source_encoding_with_options(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_bitrate: Option<Bitrate>,
) -> SourceEncodingDescriptor {
    source_encoding_with_policy_role(source_id, encoding_id, rid, max_bitrate, None)
}

fn source_encoding_with_policy_role(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_bitrate: Option<Bitrate>,
    policy_role: Option<UploadLayerPolicyRole>,
) -> SourceEncodingDescriptor {
    let raw_encoding_id =
        u32::try_from(encoding_id.as_u64()).expect("test encoding id should fit in u32");
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: rid.map(Rid::new),
        primary_ssrc: Some(Ssrc::new(100 + raw_encoding_id)),
        repair_ssrc: Some(Ssrc::new(200 + raw_encoding_id)),
        max_bitrate,
        resolution_scale: None,
        max_framerate: None,
        policy_role,
        max_temporal_layer_id: None,
        negotiated_format: Some(video_format(96)),
    })
}

fn selectable_encoding_ids(descriptor: &PublishedSourceDescriptor) -> Vec<SourceEncodingId> {
    descriptor
        .selectable_encodings()
        .map(SourceEncodingDescriptor::encoding_id)
        .collect()
}

#[test]
fn consumer_selection_delivery_requires_active_intent_and_policy_admission() {
    let mut selection = ConsumerSourceSelection::open(true);
    assert!(selection.delivery_active());

    selection.set_policy_pause_reason(Some(PolicyPauseReason::VideoDownloadLimit));
    assert!(!selection.delivery_active());

    selection.set_policy_pause_reason(None);
    selection.set_active(false);
    assert!(!selection.delivery_active());
}

#[test]
fn descriptor_keeps_source_encoding_identity_separate() {
    let source_id = PublishedSourceId::from_raw(7);
    let low_encoding_id = SourceEncodingId::from_raw(1);
    let high_encoding_id = SourceEncodingId::from_raw(2);
    let owner = PublishedSourceOwner::new(UserId::Integer(42));
    let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner,
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: Some(Mid::new("video-0")),
        encodings: vec![
            source_encoding(source_id, low_encoding_id, "lo"),
            source_encoding(source_id, high_encoding_id, "hi"),
        ],
    })
    .expect("source descriptor should be valid");

    assert_eq!(descriptor.source_id(), source_id);
    assert_eq!(descriptor.owner().user_id(), &UserId::Integer(42));
    assert_eq!(descriptor.stream_id().as_str(), "main-video");
    assert_eq!(descriptor.media_kind(), MediaKind::Video);
    assert_eq!(
        descriptor.mid().map(Mid::as_str),
        Some("video-0"),
        "the source owns the SDP media-section identity separately from RID"
    );

    let encodings = descriptor.encodings().collect::<Vec<_>>();
    assert_eq!(encodings.len(), 2);
    assert_eq!(encodings[0].source_id(), source_id);
    assert_eq!(encodings[0].rid().map(Rid::as_str), Some("lo"));
    assert_eq!(encodings[0].primary_ssrc(), Some(Ssrc::new(101)));
    assert_eq!(encodings[0].repair_ssrc(), Some(Ssrc::new(201)));
    assert_eq!(encodings[0].max_bitrate(), Some(Bitrate::from_kbps(150)));
    assert_eq!(encodings[0].max_temporal_layer_id(), None);
    assert_eq!(
        encodings[0]
            .negotiated_format()
            .map(MediaFormat::payload_type_id),
        Some(PayloadType::new(96))
    );
    assert_eq!(
        descriptor
            .encoding(high_encoding_id)
            .and_then(SourceEncodingDescriptor::rid)
            .map(Rid::as_str),
        Some("hi")
    );
}

#[test]
fn descriptor_orders_selectable_encodings_by_bitrate() {
    let source_id = PublishedSourceId::from_raw(7);
    let low_encoding_id = SourceEncodingId::from_raw(1);
    let middle_encoding_id = SourceEncodingId::from_raw(2);
    let high_encoding_id = SourceEncodingId::from_raw(3);
    let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![
            source_encoding_with_options(
                source_id,
                high_encoding_id,
                Some("hi"),
                Some(Bitrate::from_kbps(900)),
            ),
            source_encoding_with_options(
                source_id,
                low_encoding_id,
                Some("lo"),
                Some(Bitrate::from_kbps(150)),
            ),
            source_encoding_with_options(
                source_id,
                middle_encoding_id,
                Some("mid"),
                Some(Bitrate::from_kbps(450)),
            ),
        ],
    })
    .expect("source descriptor should be valid");

    assert_eq!(
        selectable_encoding_ids(&descriptor),
        vec![low_encoding_id, middle_encoding_id, high_encoding_id]
    );
    assert_eq!(
        descriptor
            .selectable_encoding_by_rank(1)
            .map(SourceEncodingDescriptor::encoding_id),
        Some(middle_encoding_id)
    );
}

#[test]
fn descriptor_keeps_declared_selectable_order_without_bitrates() {
    let source_id = PublishedSourceId::from_raw(7);
    let first_encoding_id = SourceEncodingId::from_raw(1);
    let second_encoding_id = SourceEncodingId::from_raw(2);
    let third_encoding_id = SourceEncodingId::from_raw(3);
    let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![
            source_encoding_with_options(source_id, first_encoding_id, Some("a"), None),
            source_encoding_with_options(source_id, second_encoding_id, Some("b"), None),
            source_encoding_with_options(source_id, third_encoding_id, Some("c"), None),
        ],
    })
    .expect("source descriptor should be valid");

    assert_eq!(
        selectable_encoding_ids(&descriptor),
        vec![first_encoding_id, second_encoding_id, third_encoding_id]
    );
}

#[test]
fn descriptor_orders_selectable_encodings_by_policy_role_without_bitrates() {
    let source_id = PublishedSourceId::from_raw(7);
    let high_encoding_id = SourceEncodingId::from_raw(1);
    let low_encoding_id = SourceEncodingId::from_raw(2);
    let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![
            source_encoding_with_policy_role(
                source_id,
                high_encoding_id,
                Some("hi"),
                None,
                Some(UploadLayerPolicyRole::Featured),
            ),
            source_encoding_with_policy_role(
                source_id,
                low_encoding_id,
                Some("lo"),
                None,
                Some(UploadLayerPolicyRole::Thumbnail),
            ),
        ],
    })
    .expect("source descriptor should be valid");

    assert_eq!(
        selectable_encoding_ids(&descriptor),
        vec![low_encoding_id, high_encoding_id]
    );
}

#[test]
fn descriptor_excludes_selectable_encodings_when_rid_is_missing() {
    let source_id = PublishedSourceId::from_raw(7);
    let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![
            source_encoding_with_options(
                source_id,
                SourceEncodingId::from_raw(1),
                Some("lo"),
                Some(Bitrate::from_kbps(150)),
            ),
            source_encoding_with_options(
                source_id,
                SourceEncodingId::from_raw(2),
                None,
                Some(Bitrate::from_kbps(450)),
            ),
        ],
    })
    .expect("source descriptor should be valid");

    assert_eq!(descriptor.selectable_encoding_count(), 0);
    assert!(descriptor.selectable_encoding_by_rank(0).is_none());
}

#[test]
fn descriptor_rejects_encoding_from_another_source() {
    let source_id = PublishedSourceId::from_raw(7);
    let other_source_id = PublishedSourceId::from_raw(8);
    let encoding_id = SourceEncodingId::from_raw(1);
    let result = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![source_encoding(other_source_id, encoding_id, "lo")],
    });

    assert_eq!(
        result.unwrap_err(),
        SourceModelError::EncodingSourceMismatch {
            source_id,
            encoding_id,
            encoding_source_id: other_source_id,
        }
    );
}

#[test]
fn descriptor_rejects_duplicate_encoding_ids() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let result = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![
            source_encoding(source_id, encoding_id, "lo"),
            source_encoding(source_id, encoding_id, "hi"),
        ],
    });

    assert_eq!(
        result.unwrap_err(),
        SourceModelError::DuplicateEncodingId {
            source_id,
            encoding_id,
        }
    );
}

#[test]
fn selector_targets_runtime_encoding_identity_not_transport_or_rid() {
    let encoding_id = SourceEncodingId::from_raw(3);
    let temporal_layer = SourceTemporalLayerId::new(2)
        .expect("test temporal layer should fit the RFC 9626 TID range");
    let operating_point = SourceOperatingPoint::new(encoding_id, temporal_layer);

    assert_eq!(
        SourceSelector::Encoding(encoding_id).selected_encoding(),
        Some(encoding_id)
    );
    assert_eq!(
        SourceSelector::OperatingPoint(operating_point).selected_encoding(),
        Some(encoding_id)
    );
    assert_eq!(
        SourceSelector::OperatingPoint(operating_point).selected_operating_point(),
        Some(operating_point)
    );
    assert_eq!(SourceSelector::Open.selected_encoding(), None);
    assert_eq!(
        SourceSelector::RoomPolicy(SourceRoomPolicySelector::VisibleThumbnail).selected_encoding(),
        None
    );
}

#[test]
fn temporal_layer_ids_follow_the_rfc_frame_marking_range() {
    assert_eq!(SourceTemporalLayerId::base().as_u8(), 0);
    assert_eq!(
        SourceTemporalLayerId::new(frame_marking::TEMPORAL_LAYER_ID_MAX)
            .map(SourceTemporalLayerId::as_u8),
        Some(frame_marking::TEMPORAL_LAYER_ID_MAX)
    );
    assert_eq!(
        SourceTemporalLayerId::new(frame_marking::TEMPORAL_LAYER_ID_MAX + 1),
        None
    );
}

#[test]
fn id_allocation_is_monotonic_and_topology_neutral() {
    let mut next_source_id = 1;
    let mut next_encoding_id = 1;

    assert_eq!(PublishedSourceId::allocate(&mut next_source_id).as_u64(), 1);
    assert_eq!(PublishedSourceId::allocate(&mut next_source_id).as_u64(), 2);
    assert_eq!(
        SourceEncodingId::allocate(&mut next_encoding_id).as_u64(),
        1
    );
    assert_eq!(
        SourceEncodingId::allocate(&mut next_encoding_id).as_u64(),
        2
    );
}
