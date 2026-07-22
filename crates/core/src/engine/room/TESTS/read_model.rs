use o_sfu_router::rtp::Rid;

use super::*;
use crate::engine::source_model::{PublishedSourceId, SourceEncodingDescriptorParts};

#[test]
fn source_encoding_diagnostics_project_rid_packet_activity() {
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
        &SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id: SourceEncodingId::from_raw(1),
            source_id: PublishedSourceId::from_raw(8),
            rid: Some(Rid::new("hi")),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: None,
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            negotiated_format: None,
        }),
        Some(&activity),
    );
    assert_eq!(encoding.last_packet_age_ms, Some(50));
    assert_eq!(encoding.last_keyframe_age_ms, Some(30));
}
