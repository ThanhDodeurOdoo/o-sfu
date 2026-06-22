#![allow(
    clippy::expect_used,
    reason = "test fixtures should fail loudly when they build invalid source graphs"
)]

use o_sfu_router::{MediaKind, rtp::Rid};

use super::*;
use crate::{
    Bitrate,
    engine::{
        UserId,
        source_model::{
            PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceOperatingPoint, SourcePolicy, SourceTemporalLayerId, UserStreamId,
        },
    },
};

fn source_with_encodings(encodings: Vec<SourceEncodingDescriptor>) -> PublishedSourceDescriptor {
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id: PublishedSourceId::from_raw(7),
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings,
    })
    .expect("test source graph should be valid")
}

fn encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_bitrate: Option<Bitrate>,
) -> SourceEncodingDescriptor {
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: rid.map(Rid::new),
        primary_ssrc: None,
        repair_ssrc: None,
        max_bitrate,
        resolution_scale: None,
        max_framerate: None,
        policy_role: None,
        max_temporal_layer_id: None,
        negotiated_format: None,
    })
}

fn layered_encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_temporal_layer_id: u8,
) -> SourceEncodingDescriptor {
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: rid.map(Rid::new),
        primary_ssrc: None,
        repair_ssrc: None,
        max_bitrate: None,
        resolution_scale: None,
        max_framerate: None,
        policy_role: None,
        max_temporal_layer_id: SourceTemporalLayerId::new(max_temporal_layer_id),
        negotiated_format: None,
    })
}

#[test]
fn projects_selected_encoding_to_rid_gate() {
    let source_id = PublishedSourceId::from_raw(7);
    let high_encoding_id = SourceEncodingId::from_raw(1);
    let low_encoding_id = SourceEncodingId::from_raw(2);
    let source = source_with_encodings(vec![
        encoding(
            source_id,
            high_encoding_id,
            Some("hi"),
            Some(Bitrate::from_kbps(750)),
        ),
        encoding(
            source_id,
            low_encoding_id,
            Some("lo"),
            Some(Bitrate::from_kbps(150)),
        ),
    ]);

    let selector = SourceSelector::Encoding(low_encoding_id);

    assert_eq!(
        source_packet_gate_for_selector(&source, selector),
        Ok(SourcePacketGate::Rid(String::from("lo")))
    );
}

#[test]
fn keeps_open_as_an_explicit_transport_gate() {
    let source_id = PublishedSourceId::from_raw(7);
    let source = source_with_encodings(vec![encoding(
        source_id,
        SourceEncodingId::from_raw(1),
        None,
        None,
    )]);

    assert_eq!(
        source_packet_gate_for_selector(&source, SourceSelector::Open),
        Ok(SourcePacketGate::Open)
    );
}

#[test]
fn rejects_ridless_selected_encoding() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let source = source_with_encodings(vec![encoding(source_id, encoding_id, None, None)]);

    assert_eq!(
        source_packet_gate_for_selector(&source, SourceSelector::Encoding(encoding_id)),
        Err(SourcePacketGateProjectionError::MissingRid)
    );
}

#[test]
fn projects_operating_point_to_transport_layer_gate() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let source = source_with_encodings(vec![layered_encoding(
        source_id,
        encoding_id,
        Some("hi"),
        2,
    )]);
    let temporal_layer = SourceTemporalLayerId::new(1)
        .expect("test temporal layer should fit the RFC 9626 TID range");

    assert_eq!(
        source_packet_gate_for_selector(
            &source,
            SourceSelector::OperatingPoint(SourceOperatingPoint::new(encoding_id, temporal_layer,))
        ),
        Ok(SourcePacketGate::OperatingPoint(
            SourcePacketOperatingPoint::new(Some(String::from("hi")), 1)
        ))
    );
}

#[test]
fn rejects_operating_points_without_advertised_temporal_metadata() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let source = source_with_encodings(vec![encoding(source_id, encoding_id, Some("hi"), None)]);
    let temporal_layer = SourceTemporalLayerId::base();

    assert_eq!(
        source_packet_gate_for_selector(
            &source,
            SourceSelector::OperatingPoint(SourceOperatingPoint::new(encoding_id, temporal_layer,))
        ),
        Err(SourcePacketGateProjectionError::MissingTemporalMetadata)
    );
}

#[test]
fn projects_advertised_base_layer_operating_point() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let source = source_with_encodings(vec![layered_encoding(
        source_id,
        encoding_id,
        Some("hi"),
        0,
    )]);
    let temporal_layer = SourceTemporalLayerId::base();

    assert_eq!(
        source_packet_gate_for_selector(
            &source,
            SourceSelector::OperatingPoint(SourceOperatingPoint::new(encoding_id, temporal_layer,))
        ),
        Ok(SourcePacketGate::OperatingPoint(
            SourcePacketOperatingPoint::new(Some(String::from("hi")), 0)
        ))
    );
}

#[test]
fn rejects_operating_points_above_advertised_layer() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);
    let source = source_with_encodings(vec![layered_encoding(source_id, encoding_id, None, 1)]);
    let temporal_layer = SourceTemporalLayerId::new(2)
        .expect("test temporal layer should fit the RFC 9626 TID range");

    assert_eq!(
        source_packet_gate_for_selector(
            &source,
            SourceSelector::OperatingPoint(SourceOperatingPoint::new(encoding_id, temporal_layer,))
        ),
        Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised)
    );
}
