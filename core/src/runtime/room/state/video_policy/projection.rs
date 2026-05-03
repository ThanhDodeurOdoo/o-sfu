//! Projection from source-domain selectors to transport packet gates.
//!
//! The budget planner speaks in `SourceSelector` values. This module is the
//! only room-state boundary that translates that room intent into the
//! packet-facing gate vocabulary consumed by the media transport.

#[cfg(test)]
use crate::runtime::source_model::SourceEncodingId;
use crate::runtime::{
    media_transport::{SourcePacketGate, SourcePacketOperatingPoint},
    source_model::{PublishedSourceDescriptor, SourceSelector},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourcePacketGateProjectionError {
    MissingEncoding,
    MissingRid,
    MissingTemporalMetadata,
    TemporalLayerExceedsAdvertised,
    UnsupportedRoomPolicy,
}

pub(super) fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Result<SourcePacketGate, SourcePacketGateProjectionError> {
    match selector {
        SourceSelector::Open => Ok(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => {
            let encoding = source
                .encoding(encoding_id)
                .ok_or(SourcePacketGateProjectionError::MissingEncoding)?;
            let rid = encoding
                .rid()
                .ok_or(SourcePacketGateProjectionError::MissingRid)?;
            Ok(SourcePacketGate::Rid(rid.as_str().to_owned()))
        }
        SourceSelector::OperatingPoint(operating_point) => {
            let encoding = source
                .encoding(operating_point.encoding_id())
                .ok_or(SourcePacketGateProjectionError::MissingEncoding)?;
            let max_temporal_layer_id = encoding
                .max_temporal_layer_id()
                .ok_or(SourcePacketGateProjectionError::MissingTemporalMetadata)?;
            if operating_point.max_temporal_layer_id() > max_temporal_layer_id {
                return Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised);
            }
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(
                    encoding.rid().map(|rid| rid.as_str().to_owned()),
                    operating_point.max_temporal_layer_id().as_u8(),
                ),
            ))
        }
        SourceSelector::RoomPolicy(_) => {
            Err(SourcePacketGateProjectionError::UnsupportedRoomPolicy)
        }
    }
}

#[cfg(test)]
fn lowest_declared_encoding(source: &PublishedSourceDescriptor) -> Option<SourceEncodingId> {
    let encodings = super::input::selectable_encodings(source);
    if encodings.len() < 2 || encodings.iter().any(|encoding| encoding.rid().is_none()) {
        return None;
    }
    let use_declared_order = encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none());
    encodings
        .into_iter()
        .enumerate()
        .min_by_key(|(index, encoding)| {
            if use_declared_order {
                (0_u64, *index)
            } else {
                (encoding.max_bitrate().unwrap_or(u64::MAX), *index)
            }
        })
        .map(|(_index, encoding)| encoding.encoding_id())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::runtime::{
        StreamType, UserId,
        source_model::{
            PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceOperatingPoint, SourceTemporalLayerId,
        },
    };

    fn source_with_encodings(
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: PublishedSourceId::from_raw(7),
            owner: PublishedSourceOwner::new(UserId::Integer(42)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: None,
            encodings,
        })
        .expect("test source graph should be valid")
    }

    fn encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: Option<&str>,
        max_bitrate: Option<u64>,
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
    fn source_selector_bridge_projects_selected_encoding_to_rid_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let high_encoding_id = SourceEncodingId::from_raw(1);
        let low_encoding_id = SourceEncodingId::from_raw(2);
        let source = source_with_encodings(vec![
            encoding(source_id, high_encoding_id, Some("hi"), Some(750_000)),
            encoding(source_id, low_encoding_id, Some("lo"), Some(150_000)),
        ]);

        let selector = lowest_declared_encoding(&source)
            .map_or(SourceSelector::Open, SourceSelector::Encoding);

        assert_eq!(selector, SourceSelector::Encoding(low_encoding_id));
        assert_eq!(
            source_packet_gate_for_selector(&source, selector),
            Ok(SourcePacketGate::Rid(String::from("lo")))
        );
    }

    #[test]
    fn source_selector_bridge_keeps_open_as_an_explicit_transport_gate() {
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
    fn source_selector_bridge_rejects_ridless_selected_encoding() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![encoding(source_id, encoding_id, None, None)]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Encoding(encoding_id)),
            Err(SourcePacketGateProjectionError::MissingRid)
        );
    }

    #[test]
    fn source_selector_bridge_projects_operating_point_to_transport_layer_gate() {
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
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(Some(String::from("hi")), 1)
            ))
        );
    }

    #[test]
    fn source_selector_bridge_rejects_operating_points_without_advertised_temporal_metadata() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source =
            source_with_encodings(vec![encoding(source_id, encoding_id, Some("hi"), None)]);
        let temporal_layer = SourceTemporalLayerId::base();

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Err(SourcePacketGateProjectionError::MissingTemporalMetadata)
        );
    }

    #[test]
    fn source_selector_bridge_projects_advertised_base_layer_operating_point() {
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
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(Some(String::from("hi")), 0)
            ))
        );
    }

    #[test]
    fn source_selector_bridge_rejects_operating_points_above_advertised_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(source_id, encoding_id, None, 1)]);
        let temporal_layer = SourceTemporalLayerId::new(2)
            .expect("test temporal layer should fit the RFC 9626 TID range");

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised)
        );
    }
}
