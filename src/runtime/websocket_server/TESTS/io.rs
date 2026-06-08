use serde_json::json;

use super::{
    ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
    MAX_CLIENT_FRAME_BYTES, decode_client_batch,
};

#[test]
fn decode_client_batch_classifies_generated_failures() -> serde_json::Result<()> {
    let oversized_batch = serde_json::to_string(
        &(0..=MAX_CLIENT_BATCH_ENVELOPES)
            .map(|_| json!({ "t": "info", "p": {} }))
            .collect::<Vec<_>>(),
    )?;
    let cases = [
        (
            "not-json".to_owned(),
            ClientBatchDecodeFailureKind::InvalidInput,
        ),
        (
            serde_json::to_string(&[json!({ "t": "not-a-real-message", "p": {} })])?,
            ClientBatchDecodeFailureKind::UnsupportedFeature,
        ),
        (
            serde_json::to_string(&[json!({
                "t": "ping",
                "q": "1",
                "r": "2",
            })])?,
            ClientBatchDecodeFailureKind::InvalidInput,
        ),
        (
            serde_json::to_string(&[json!({ "t": "broadcast" })])?,
            ClientBatchDecodeFailureKind::InvalidInput,
        ),
        (oversized_batch, ClientBatchDecodeFailureKind::InvalidInput),
    ];

    for (payload, expected_kind) in cases {
        assert_eq!(
            decode_client_batch(&payload)
                .err()
                .map(|error| error.kind()),
            Some(expected_kind)
        );
    }
    Ok(())
}

#[test]
fn decode_client_batch_rejects_oversized_batch_before_routing_metadata() -> serde_json::Result<()> {
    let oversized_batch = serde_json::to_string(
        &(0..=MAX_CLIENT_BATCH_ENVELOPES)
            .map(|_| json!({ "t": "info", "p": {}, "q": "1", "r": "2" }))
            .collect::<Vec<_>>(),
    )?;

    assert_eq!(
        decode_client_batch(&oversized_batch),
        Err(ClientBatchDecodeError::BatchTooLarge {
            actual: MAX_CLIENT_BATCH_ENVELOPES + 1,
            limit: MAX_CLIENT_BATCH_ENVELOPES,
        })
    );
    Ok(())
}

#[test]
fn decode_client_batch_rejects_oversized_frame_before_json_decode() {
    let oversized_payload = "x".repeat(MAX_CLIENT_FRAME_BYTES + 1);

    let error = decode_client_batch(&oversized_payload);
    assert!(matches!(
        error,
        Err(ClientBatchDecodeError::FrameTooLarge {
            actual,
            limit: MAX_CLIENT_FRAME_BYTES,
        }) if actual == MAX_CLIENT_FRAME_BYTES + 1
    ));
}
