use super::*;

const LONG_DESCRIPTOR: &[u8] = &[0x90, 0xe0, 0x80, 0x02, 0x09, 0x00, 0x00];
const SWITCHED_LONG_DESCRIPTOR: &[u8] = &[0x90, 0xe0, 0x80, 0x0a, 0x04, 0x00, 0x00];
const SHORT_DESCRIPTOR: &[u8] = &[0x90, 0xe0, 0x0a, 0x04, 0x00, 0x00];
const MALFORMED_TL0_PIC_IDX_WITHOUT_TID: &[u8] = &[
    0x90, 0x40, 0x42, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
];

#[test]
fn tl0_pic_idx_without_tid_is_not_a_decoder_refresh() {
    let packet = Packet::inspect(MALFORMED_TL0_PIC_IDX_WITHOUT_TID, true);

    assert!(packet.descriptor.is_none());
    assert!(!packet.decoder_refresh());
}

#[test]
fn packet_parse_and_source_switch_build_projected_patch() {
    let packet = Packet::inspect(LONG_DESCRIPTOR, true);
    let switched_packet = Packet::inspect(SWITCHED_LONG_DESCRIPTOR, true);
    assert!(packet.descriptor.is_some());
    assert!(switched_packet.descriptor.is_some());
    let mut projection = Projection::default();
    let first = projection.project(packet.identity());
    let switched = projection.reanchor(switched_packet.identity());
    let patch = switched_packet.patch(switched);
    let expected = switched_packet.descriptor.and_then(|descriptor| {
        descriptor
            .patch()
            .picture_id(3)
            .tl0_pic_idx(10)
            .build()
            .ok()
    });

    assert_eq!(first.picture_id, Some(2));
    assert_eq!(first.tl0_pic_idx, Some(9));
    assert_eq!(switched.picture_id, Some(3));
    assert_eq!(switched.tl0_pic_idx, Some(10));
    assert!(expected.is_some());
    assert_eq!(patch, expected);
}

#[test]
fn short_picture_id_patch_wraps_projected_identity() {
    let packet = Packet::inspect(SHORT_DESCRIPTOR, true);
    let mut projection = Projection::default();
    projection.project(Identity {
        picture_id: Some(127),
        tl0_pic_idx: Some(3),
    });
    let switched = projection.reanchor(packet.identity());
    let patch = packet.patch(switched);
    let expected = packet
        .descriptor
        .and_then(|descriptor| descriptor.patch().picture_id(0).tl0_pic_idx(4).build().ok());

    assert_eq!(switched.picture_id, Some(128));
    assert_eq!(switched.tl0_pic_idx, Some(4));
    assert!(expected.is_some());
    assert_eq!(patch, expected);
}

#[test]
fn projected_identifiers_wrap_across_source_switches() {
    let mut projection = Projection::default();
    let first = projection.project(Identity {
        picture_id: Some(32_767),
        tl0_pic_idx: Some(255),
    });
    let switched = projection.reanchor(Identity {
        picture_id: Some(12),
        tl0_pic_idx: Some(4),
    });
    let next = projection.project(Identity {
        picture_id: Some(14),
        tl0_pic_idx: Some(6),
    });

    assert_eq!(first.picture_id, Some(32_767));
    assert_eq!(first.tl0_pic_idx, Some(255));
    assert_eq!(switched.picture_id, Some(0));
    assert_eq!(switched.tl0_pic_idx, Some(0));
    assert_eq!(next.picture_id, Some(2));
    assert_eq!(next.tl0_pic_idx, Some(2));
}

#[test]
fn missing_fields_preserve_last_projected_identity_across_switches() {
    let mut projection = Projection::default();
    let first = projection.project(Identity {
        picture_id: Some(100),
        tl0_pic_idx: Some(30),
    });
    let gap = projection.reanchor(Identity::default());
    let resumed = projection.project(Identity {
        picture_id: Some(12),
        tl0_pic_idx: Some(4),
    });

    assert_eq!(first.picture_id, Some(100));
    assert_eq!(first.tl0_pic_idx, Some(30));
    assert_eq!(gap, Identity::default());
    assert_eq!(resumed.picture_id, Some(101));
    assert_eq!(resumed.tl0_pic_idx, Some(31));
}

#[test]
fn missing_fields_are_tracked_independently_across_switches() {
    let mut projection = Projection::default();
    let _first = projection.project(Identity {
        picture_id: Some(100),
        tl0_pic_idx: Some(30),
    });
    let missing_picture_id = projection.reanchor(Identity {
        picture_id: None,
        tl0_pic_idx: Some(4),
    });
    let picture_id_resumed = projection.project(Identity {
        picture_id: Some(12),
        tl0_pic_idx: Some(5),
    });

    assert_eq!(missing_picture_id.picture_id, None);
    assert_eq!(missing_picture_id.tl0_pic_idx, Some(31));
    assert_eq!(picture_id_resumed.picture_id, Some(101));
    assert_eq!(picture_id_resumed.tl0_pic_idx, Some(32));

    let mut projection = Projection::default();
    let _first = projection.project(Identity {
        picture_id: Some(200),
        tl0_pic_idx: Some(40),
    });
    let missing_tl0 = projection.reanchor(Identity {
        picture_id: Some(12),
        tl0_pic_idx: None,
    });
    let tl0_resumed = projection.project(Identity {
        picture_id: Some(13),
        tl0_pic_idx: Some(4),
    });

    assert_eq!(missing_tl0.picture_id, Some(201));
    assert_eq!(missing_tl0.tl0_pic_idx, None);
    assert_eq!(tl0_resumed.picture_id, Some(202));
    assert_eq!(tl0_resumed.tl0_pic_idx, Some(41));
}
