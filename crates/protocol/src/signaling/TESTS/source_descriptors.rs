use std::num::NonZeroU16;

use serde_json::json;

use super::*;
use crate::signaling::EnvelopeDecodeError;

#[test]
fn protocol_sources_message_serializes_source_descriptors() -> serde_json::Result<()> {
    let source = SourceDescriptor {
        source_id: String::from("source-7"),
        user_id: UserId::Integer(5),
        stream_type: StreamType::Camera,
        active: true,
        mid: Some(String::from("0")),
        encodings: vec![
            SourceEncodingDescriptor {
                encoding_id: String::from("encoding-1"),
                rid: Some(String::from("lo")),
                max_bitrate: Some(150_000),
                resolution_scale: NonZeroU16::new(4),
                max_framerate: None,
                policy_role: Some(UploadLayerPolicyRole::Thumbnail),
            },
            SourceEncodingDescriptor {
                encoding_id: String::from("encoding-2"),
                rid: Some(String::from("hi")),
                max_bitrate: Some(900_000),
                resolution_scale: NonZeroU16::new(1),
                max_framerate: None,
                policy_role: Some(UploadLayerPolicyRole::Featured),
            },
        ],
    };
    let source_update = ServerMessage::Sources(vec![source]).into_envelope()?;

    assert_eq!(
        serde_json::to_value(&source_update)?,
        json!({
            "t": "sources",
            "p": [{
                "sourceId": "source-7",
                "sessionId": 5,
                "type": "camera",
                "active": true,
                "mid": "0",
                "encodings": [
                    {
                        "encodingId": "encoding-1",
                        "rid": "lo",
                        "maxBitrate": 150_000,
                        "resolutionScale": 4,
                        "policyRole": "thumbnail",
                    },
                    {
                        "encodingId": "encoding-2",
                        "rid": "hi",
                        "maxBitrate": 900_000,
                        "resolutionScale": 1,
                        "policyRole": "featured",
                    },
                ],
            }],
        })
    );
    Ok(())
}

#[test]
fn protocol_sources_message_rejects_zero_resolution_scale() {
    assert_eq!(
        ServerEnvelope::decode(Envelope::message(
            "sources",
            Some(json!([{
                "sourceId": "source-7",
                "sessionId": 5,
                "type": "camera",
                "active": true,
                "encodings": [{
                    "encodingId": "encoding-1",
                    "resolutionScale": 0,
                }],
            }])),
        )),
        Err(EnvelopeDecodeError::InvalidPayload(String::from("sources")))
    );
}
