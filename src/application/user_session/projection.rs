//! translation layer between internal core models and signaling protocol wire shapes
//!
//! this module isolates the conversion logic needed to map domain-native state into the format
//! expected by the signaling protocol

use o_sfu_protocol::wire::{
    NegotiationUploadEncoding, NegotiationUploadSlot, SessionDescriptionPayload, SourceDescriptor,
    SourceEncodingDescriptor, StreamType, UploadLayerPolicyRole as ProtocolUploadLayerPolicyRole,
    UserId,
};

use crate::core::{
    prelude::{Bitrate, NegotiationOffer, UploadEncoding, UploadLayerPolicyRole, UploadSlot},
    server::source_model::{PublishedSourceDescriptor, SourceTemporalLayerId},
};

pub(super) fn session_description_payload(offer: NegotiationOffer) -> SessionDescriptionPayload {
    SessionDescriptionPayload {
        sdp: offer.sdp,
        upload_slots: offer
            .upload_slots
            .into_iter()
            .map(protocol_upload_slot)
            .collect(),
    }
}

pub(super) fn wire_source_descriptor(
    source: &PublishedSourceDescriptor,
    user_id: UserId,
    stream_type: StreamType,
    active: bool,
) -> SourceDescriptor {
    SourceDescriptor {
        source_id: source.source_id().to_string(),
        user_id,
        stream_type,
        active,
        mid: source.mid().map(|mid| mid.as_str().to_owned()),
        encodings: source_encodings(source),
    }
}

fn protocol_upload_slot(slot: UploadSlot) -> NegotiationUploadSlot {
    NegotiationUploadSlot {
        mid: slot.mid,
        kind: slot.kind,
        codecs: slot.codecs,
        simulcast_encodings: slot
            .simulcast_encodings
            .into_iter()
            .map(protocol_upload_encoding)
            .collect(),
    }
}

fn protocol_upload_encoding(encoding: UploadEncoding) -> NegotiationUploadEncoding {
    NegotiationUploadEncoding {
        rid: encoding.rid,
        max_bitrate: encoding.max_bitrate.map(Bitrate::as_bps),
        resolution_scale: encoding.resolution_scale,
        max_framerate: encoding.max_framerate,
    }
}

fn source_encodings(source: &PublishedSourceDescriptor) -> Vec<SourceEncodingDescriptor> {
    source
        .encodings()
        .map(|encoding| SourceEncodingDescriptor {
            encoding_id: encoding.encoding_id().to_string(),
            rid: encoding.rid().map(|rid| rid.as_str().to_owned()),
            max_bitrate: encoding.max_bitrate().map(Bitrate::as_bps),
            resolution_scale: encoding.resolution_scale(),
            max_framerate: encoding.max_framerate(),
            policy_role: encoding
                .policy_role()
                .map(protocol_upload_layer_policy_role),
            max_temporal_layer_id: encoding
                .max_temporal_layer_id()
                .map(SourceTemporalLayerId::as_u8),
        })
        .collect()
}

fn protocol_upload_layer_policy_role(role: UploadLayerPolicyRole) -> ProtocolUploadLayerPolicyRole {
    match role {
        UploadLayerPolicyRole::Featured => ProtocolUploadLayerPolicyRole::Featured,
        UploadLayerPolicyRole::Thumbnail => ProtocolUploadLayerPolicyRole::Thumbnail,
        UploadLayerPolicyRole::DegradedThumbnail => {
            ProtocolUploadLayerPolicyRole::DegradedThumbnail
        }
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_router::{MediaKind, Mid, Rid};

    use super::*;
    use crate::{
        application::stream_catalog::source_publish_intent_for_stream_type,
        core::server::source_model::{
            PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
            SourceEncodingDescriptor as CoreSourceEncodingDescriptor,
            SourceEncodingDescriptorParts, SourceEncodingId,
        },
    };

    #[test]
    fn session_description_payload_preserves_upload_slot_metadata() {
        assert_eq!(
            session_description_payload(NegotiationOffer {
                sdp: "v=0\r\n".to_owned(),
                upload_slots: vec![UploadSlot {
                    mid: "video-0".to_owned(),
                    kind: MediaKind::Video,
                    codecs: vec!["VP8".to_owned(), "H264".to_owned()],
                    simulcast_encodings: vec![
                        UploadEncoding {
                            rid: "lo".to_owned(),
                            max_bitrate: Some(Bitrate::from_kbps(150)),
                            resolution_scale: Some(2),
                            max_framerate: Some(15),
                        },
                        UploadEncoding {
                            rid: "hi".to_owned(),
                            max_bitrate: Some(Bitrate::from_kbps(900)),
                            resolution_scale: None,
                            max_framerate: Some(30),
                        },
                    ],
                }],
            }),
            SessionDescriptionPayload {
                sdp: "v=0\r\n".to_owned(),
                upload_slots: vec![NegotiationUploadSlot {
                    mid: "video-0".to_owned(),
                    kind: MediaKind::Video,
                    codecs: vec!["VP8".to_owned(), "H264".to_owned()],
                    simulcast_encodings: vec![
                        NegotiationUploadEncoding {
                            rid: "lo".to_owned(),
                            max_bitrate: Some(150_000),
                            resolution_scale: Some(2),
                            max_framerate: Some(15),
                        },
                        NegotiationUploadEncoding {
                            rid: "hi".to_owned(),
                            max_bitrate: Some(900_000),
                            resolution_scale: None,
                            max_framerate: Some(30),
                        },
                    ],
                }],
            },
        );
    }

    #[test]
    fn source_descriptor_preserves_encoding_metadata() -> Result<(), &'static str> {
        let source = published_source()?;
        assert_eq!(
            wire_source_descriptor(&source, UserId::Integer(7), StreamType::Camera, true),
            SourceDescriptor {
                source_id: "source-1".to_owned(),
                user_id: UserId::Integer(7),
                stream_type: StreamType::Camera,
                active: true,
                mid: Some("published-cam-0".to_owned()),
                encodings: vec![
                    SourceEncodingDescriptor {
                        encoding_id: "encoding-2".to_owned(),
                        rid: Some("lo".to_owned()),
                        max_bitrate: Some(150_000),
                        resolution_scale: Some(2),
                        max_framerate: Some(15),
                        policy_role: Some(ProtocolUploadLayerPolicyRole::Thumbnail),
                        max_temporal_layer_id: Some(0),
                    },
                    SourceEncodingDescriptor {
                        encoding_id: "encoding-3".to_owned(),
                        rid: Some("hi".to_owned()),
                        max_bitrate: Some(900_000),
                        resolution_scale: None,
                        max_framerate: Some(30),
                        policy_role: Some(ProtocolUploadLayerPolicyRole::Featured),
                        max_temporal_layer_id: Some(2),
                    },
                ],
            },
        );
        Ok(())
    }

    fn published_source() -> Result<PublishedSourceDescriptor, &'static str> {
        let source_id = PublishedSourceId::from_raw(1);
        let low_temporal_layer = SourceTemporalLayerId::new(0)
            .ok_or("low temporal layer should fit frame marking range")?;
        let high_temporal_layer = SourceTemporalLayerId::new(2)
            .ok_or("high temporal layer should fit frame marking range")?;
        let encodings = vec![
            CoreSourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
                encoding_id: SourceEncodingId::from_raw(2),
                source_id,
                rid: Some(Rid::new("lo")),
                primary_ssrc: None,
                repair_ssrc: None,
                max_bitrate: Some(Bitrate::from_kbps(150)),
                resolution_scale: Some(2),
                max_framerate: Some(15),
                policy_role: Some(UploadLayerPolicyRole::Thumbnail),
                max_temporal_layer_id: Some(low_temporal_layer),
                negotiated_format: None,
            }),
            CoreSourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
                encoding_id: SourceEncodingId::from_raw(3),
                source_id,
                rid: Some(Rid::new("hi")),
                primary_ssrc: None,
                repair_ssrc: None,
                max_bitrate: Some(Bitrate::from_kbps(900)),
                resolution_scale: None,
                max_framerate: Some(30),
                policy_role: Some(UploadLayerPolicyRole::Featured),
                max_temporal_layer_id: Some(high_temporal_layer),
                negotiated_format: None,
            }),
        ];
        let intent = source_publish_intent_for_stream_type(StreamType::Camera);
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(UserId::Integer(7)),
            stream_id: intent.stream_id().clone(),
            media_kind: intent.media_kind(),
            policy: intent.policy(),
            mid: Some(Mid::new("published-cam-0")),
            encodings,
        })
        .map_err(|_error| "test source descriptor should satisfy source graph invariants")
    }
}
