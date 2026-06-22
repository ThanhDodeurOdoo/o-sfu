#![allow(
    clippy::expect_used,
    reason = "diagnostics fixtures should fail loudly when they build invalid source graphs"
)]

use o_sfu_router::{MediaKind, rtp::Rid};

use super::*;
use crate::engine::source_model::{
    PublishedSourceDescriptorParts, PublishedSourceOwner, SourceEncodingDescriptorParts,
    SourceOperatingPoint, SourcePolicy, SourceSelector, UserStreamId,
};

fn encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    max_temporal_layer_id: Option<u8>,
) -> SourceEncodingDescriptor {
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: Some(Rid::new("hi")),
        primary_ssrc: None,
        repair_ssrc: None,
        max_bitrate: None,
        resolution_scale: None,
        max_framerate: None,
        policy_role: None,
        max_temporal_layer_id: max_temporal_layer_id.and_then(SourceTemporalLayerId::new),
        negotiated_format: None,
    })
}

fn source_with_encoding(encoding: SourceEncodingDescriptor) -> PublishedSourceDescriptor {
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id: encoding.source_id(),
        owner: PublishedSourceOwner::new(UserId::Integer(1)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![encoding],
    })
    .expect("test source graph should be valid")
}

#[test]
fn source_encoding_diagnostics_distinguish_absent_temporal_metadata_from_base_layer() {
    let source_id = PublishedSourceId::from_raw(7);
    let absent_encoding = source_encoding(
        &encoding(source_id, SourceEncodingId::from_raw(1), None),
        None,
    );
    let base_layer_encoding = source_encoding(
        &encoding(
            source_id,
            SourceEncodingId::from_raw(2),
            Some(SourceTemporalLayerId::base().as_u8()),
        ),
        None,
    );

    assert_eq!(
        absent_encoding.temporal_layer_metadata,
        DiagnosticsTemporalLayerMetadata::Absent
    );
    assert_eq!(absent_encoding.max_temporal_layer_id, None);
    assert_eq!(
        base_layer_encoding.temporal_layer_metadata,
        DiagnosticsTemporalLayerMetadata::Advertised
    );
    assert_eq!(base_layer_encoding.max_temporal_layer_id, Some(0));
}

#[test]
fn source_encoding_diagnostics_project_rid_packet_activity() {
    let source_id = PublishedSourceId::from_raw(8);
    let activity = TransportSourceActivity::new(
        TransportMediaId::new(42),
        Duration::from_millis(80),
        Some(Duration::from_millis(70)),
        vec![TransportRidActivity::new(
            "hi".to_owned(),
            Duration::from_millis(50),
            Some(Duration::from_millis(30)),
        )],
    );

    let encoding = source_encoding(
        &encoding(source_id, SourceEncodingId::from_raw(1), None),
        Some(&activity),
    );

    assert_eq!(encoding.last_packet_age_ms, Some(50));
    assert_eq!(encoding.last_keyframe_age_ms, Some(30));
}

#[test]
fn source_selection_diagnostics_distinguish_unselected_temporal_layer_from_base_layer() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let source = source_with_encoding(encoding(
        source_id,
        encoding_id,
        Some(SourceTemporalLayerId::base().as_u8()),
    ));

    let mut rid_selection = ConsumerSourceSelection::open(true);
    rid_selection.set_selector(SourceSelector::Encoding(encoding_id));
    let rid_diagnostics = selection(&source, rid_selection);
    assert_eq!(
        rid_diagnostics.temporal_layer_selection,
        DiagnosticsTemporalLayerSelection::NotSelected
    );
    assert_eq!(rid_diagnostics.selected_temporal_layer_id, None);

    let mut base_layer_selection = ConsumerSourceSelection::open(true);
    base_layer_selection.set_selector(SourceSelector::OperatingPoint(SourceOperatingPoint::new(
        encoding_id,
        SourceTemporalLayerId::base(),
    )));
    let base_layer_diagnostics = selection(&source, base_layer_selection);
    assert_eq!(
        base_layer_diagnostics.temporal_layer_selection,
        DiagnosticsTemporalLayerSelection::Selected
    );
    assert_eq!(
        base_layer_diagnostics.selected_temporal_layer_id,
        Some(SourceTemporalLayerId::base().as_u8())
    );
}
