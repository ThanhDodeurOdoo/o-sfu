use super::{
    fmtp, frame_marking,
    h264::{self, LevelIdc, PacketizationMode, Profile, ProfileLevelId},
    header_extension, rtcp_feedback_format,
};

#[test]
fn h264_profile_level_id_parses_profile_and_level() {
    let parsed = ProfileLevelId::parse("42e01f");
    assert_eq!(
        parsed.map(ProfileLevelId::profile),
        Some(Profile::ConstrainedBaseline)
    );
    assert_eq!(parsed.map(ProfileLevelId::level), Some(LevelIdc::Level3_1));
}

#[test]
fn h264_default_profile_level_id_is_baseline_level_1() {
    let parsed = ProfileLevelId::parse(fmtp::H264_DEFAULT_PROFILE_LEVEL_ID);
    assert_eq!(parsed.map(ProfileLevelId::profile), Some(Profile::Baseline));
    assert_eq!(parsed.map(ProfileLevelId::level), Some(LevelIdc::Level1));
}

#[test]
fn h264_profile_level_id_distinguishes_level_1_variants() {
    let cases = [
        ("42e00a", Profile::ConstrainedBaseline, LevelIdc::Level1),
        ("42e00b", Profile::ConstrainedBaseline, LevelIdc::Level1_1),
        ("42a00b", Profile::Baseline, LevelIdc::Level1_1),
        ("4de00b", Profile::ConstrainedBaseline, LevelIdc::Level1_1),
        ("58e00b", Profile::ConstrainedBaseline, LevelIdc::Level1_1),
        ("42500b", Profile::ConstrainedBaseline, LevelIdc::Level1B),
        ("4d100b", Profile::Main, LevelIdc::Level1B),
        ("58100b", Profile::Extended, LevelIdc::Level1B),
        ("640009", Profile::High, LevelIdc::Level1B),
    ];

    for (token, profile, level) in cases {
        let parsed = ProfileLevelId::parse(token);
        assert_eq!(parsed.map(ProfileLevelId::profile), Some(profile));
        assert_eq!(parsed.map(ProfileLevelId::level), Some(level));
    }
}

#[test]
fn h264_profile_level_id_rejects_invalid_level_1b_encodings() {
    let invalid_tokens = ["42e000", "42e009", "4d0009", "58e009", "640000"];

    for token in invalid_tokens {
        assert_eq!(ProfileLevelId::parse(token), None);
    }
}

#[test]
fn h264_level_ordering_keeps_level_1b_between_level_1_and_level_1_1() {
    assert!(LevelIdc::Level1 < LevelIdc::Level1B);
    assert!(LevelIdc::Level1B < LevelIdc::Level1_1);
}

#[test]
fn h264_payload_keyframe_detection_covers_idr_packetizations() {
    assert!(h264::payload_starts_idr(
        &[0x65, 0x88],
        PacketizationMode::SingleNalUnit
    ));
    assert!(h264::payload_starts_idr(
        &[0x78, 0x00, 0x02, 0x67, 0x42, 0x00, 0x02, 0x65, 0x88],
        PacketizationMode::NonInterleaved
    ));
    assert!(h264::payload_starts_idr(
        &[0x7c, 0x85, 0x88],
        PacketizationMode::NonInterleaved
    ));
    assert!(!h264::payload_starts_idr(
        &[0x7c, 0xc5, 0x88],
        PacketizationMode::NonInterleaved
    ));
    assert!(!h264::payload_starts_idr(
        &[0x41, 0x9a],
        PacketizationMode::NonInterleaved
    ));
    assert!(!h264::payload_starts_idr(
        &[0x7c, 0x05, 0x88],
        PacketizationMode::NonInterleaved
    ));
    assert!(!h264::payload_starts_idr(
        &[0x78, 0x00, 0x02, 0x65, 0x88],
        PacketizationMode::SingleNalUnit
    ));
    assert!(!h264::payload_starts_idr(
        &[0x7c, 0x85, 0x88],
        PacketizationMode::SingleNalUnit
    ));
}

#[test]
fn rtcp_mux_payload_type_range_follows_rfc_5761() {
    assert!(super::is_rtcp_mux_payload_type(63));
    assert!(!super::is_rtcp_mux_payload_type(64));
    assert!(!super::is_rtcp_mux_payload_type(95));
    assert!(super::is_rtcp_mux_payload_type(96));
    assert!(super::is_rtcp_mux_payload_type(127));
    assert!(!super::is_rtcp_mux_payload_type(128));
}

#[test]
fn rtcp_feedback_values_include_layer_refresh_request() {
    assert_eq!(super::RTCP_PACKET_TYPE_RTPFB, 205);
    assert_eq!(super::RTCP_PACKET_TYPE_PSFB, 206);
    assert_eq!(rtcp_feedback_format::RTPFB_GENERIC_NACK, 1);
    assert_eq!(rtcp_feedback_format::PSFB_PLI, 1);
    assert_eq!(rtcp_feedback_format::PSFB_FIR, 4);
    assert_eq!(rtcp_feedback_format::PSFB_LRR, 10);
}

#[test]
fn stream_id_sdes_items_follow_rfc_8852_allocations() {
    assert_eq!(super::RTCP_SDES_ITEM_RTP_STREAM_ID, 12);
    assert_eq!(super::RTCP_SDES_ITEM_REPAIRED_RTP_STREAM_ID, 13);
}

#[test]
fn frame_marking_helpers_extract_temporal_layer_id() {
    let flags = frame_marking::START_OF_FRAME_MASK
        | frame_marking::BASE_LAYER_SYNC_MASK
        | frame_marking::TEMPORAL_LAYER_ID_MAX;

    assert_eq!(
        frame_marking::temporal_layer_id(flags),
        frame_marking::TEMPORAL_LAYER_ID_MAX
    );
    assert!(frame_marking::is_valid_temporal_layer_id(
        frame_marking::TEMPORAL_LAYER_ID_MAX
    ));
    assert!(!frame_marking::is_valid_temporal_layer_id(8));
    assert!(header_extension::is_one_byte_id(
        header_extension::ONE_BYTE_ID_MIN
    ));
}

#[test]
fn vp8_payload_keyframe_detection_follows_payload_descriptor() {
    assert!(!super::vp8::payload_starts_keyframe(&[]));
    assert!(super::vp8::payload_starts_keyframe(&[0x10, 0x00]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x10, 0x01]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x00, 0x00]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x11, 0x00]));
    assert!(super::vp8::payload_starts_keyframe(&[
        0x90, 0x80, 0x42, 0x00,
    ]));
    assert!(super::vp8::payload_starts_keyframe(&[
        0x90, 0x80, 0x80, 0x42, 0x00,
    ]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x90, 0x80]));
}

#[test]
fn vp8_payload_descriptor_rewrite_updates_long_picture_id_and_tl0() {
    let mut payload = vec![0x90, 0xc0, 0x80, 0x02, 0x09, 0x00];
    let descriptor = super::vp8::payload_descriptor(&payload);
    assert!(descriptor.is_some());
    let Some(descriptor) = descriptor else {
        return;
    };

    assert_eq!(descriptor.picture_id(), Some(2));
    assert_eq!(descriptor.tl0_pic_idx(), Some(9));

    super::vp8::rewrite_payload_descriptor(
        &mut payload,
        descriptor,
        super::vp8::PayloadDescriptorRewrite {
            picture_id: Some(0x1234),
            tl0_pic_idx: Some(44),
        },
    );

    let rewritten = super::vp8::payload_descriptor(&payload);
    assert!(rewritten.is_some());
    let Some(rewritten) = rewritten else {
        return;
    };
    assert_eq!(rewritten.picture_id(), Some(0x1234));
    assert_eq!(rewritten.tl0_pic_idx(), Some(44));
    assert_eq!(payload, vec![0x90, 0xc0, 0x92, 0x34, 44, 0x00]);
}

#[test]
fn vp8_payload_descriptor_rewrite_keeps_short_picture_id_width() {
    let mut payload = vec![0x90, 0x80, 0x02, 0x00];
    let descriptor = super::vp8::payload_descriptor(&payload);
    assert!(descriptor.is_some());
    let Some(descriptor) = descriptor else {
        return;
    };

    assert_eq!(descriptor.picture_id(), Some(2));

    super::vp8::rewrite_payload_descriptor(
        &mut payload,
        descriptor,
        super::vp8::PayloadDescriptorRewrite {
            picture_id: Some(0x1234),
            tl0_pic_idx: None,
        },
    );

    let rewritten = super::vp8::payload_descriptor(&payload);
    assert!(rewritten.is_some());
    let Some(rewritten) = rewritten else {
        return;
    };
    assert_eq!(rewritten.picture_id(), Some(0x34));
    assert_eq!(payload, vec![0x90, 0x80, 0x34, 0x00]);
}
