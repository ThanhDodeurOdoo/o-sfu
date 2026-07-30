use serde_json::json;

use super::*;
use crate::signaling::EnvelopeDecodeError;

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
