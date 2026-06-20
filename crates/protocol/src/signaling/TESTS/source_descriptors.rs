use serde_json::json;

use super::*;

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
                resolution_scale: Some(4),
                max_framerate: None,
                policy_role: Some(UploadLayerPolicyRole::Thumbnail),
                max_temporal_layer_id: Some(0),
            },
            SourceEncodingDescriptor {
                encoding_id: String::from("encoding-2"),
                rid: Some(String::from("hi")),
                max_bitrate: Some(900_000),
                resolution_scale: Some(1),
                max_framerate: None,
                policy_role: Some(UploadLayerPolicyRole::Featured),
                max_temporal_layer_id: Some(2),
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
                        "maxTemporalLayerId": 0,
                    },
                    {
                        "encodingId": "encoding-2",
                        "rid": "hi",
                        "maxBitrate": 900_000,
                        "resolutionScale": 1,
                        "policyRole": "featured",
                        "maxTemporalLayerId": 2,
                    },
                ],
            }],
        })
    );
    Ok(())
}
