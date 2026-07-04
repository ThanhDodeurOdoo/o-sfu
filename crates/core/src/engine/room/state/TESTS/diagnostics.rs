#![allow(
    clippy::expect_used,
    reason = "diagnostics fixtures should fail loudly when they build invalid source graphs"
)]

use o_sfu_router::rtp::Rid;

use super::*;
use crate::engine::source_model::SourceEncodingDescriptorParts;

fn encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
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
        negotiated_format: None,
    })
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
        &encoding(source_id, SourceEncodingId::from_raw(1)),
        Some(&activity),
    );

    assert_eq!(encoding.last_packet_age_ms, Some(50));
    assert_eq!(encoding.last_keyframe_age_ms, Some(30));
}
