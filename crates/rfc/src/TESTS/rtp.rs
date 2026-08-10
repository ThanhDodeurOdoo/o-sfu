use super::{
    PayloadType, fmtp, frame_marking,
    h264::{self, LevelIdc, PacketizationMode, Profile, ProfileLevelId},
    header_extension,
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
fn h264_profile_level_id_builds_canonical_fmtp_values() {
    let cases = [
        (Profile::Baseline, LevelIdc::Level3_1, "42001f"),
        (Profile::ConstrainedBaseline, LevelIdc::Level3_1, "42e01f"),
        (Profile::Main, LevelIdc::Level3_1, "4d001f"),
        (Profile::Extended, LevelIdc::Level3_1, "58001f"),
        (Profile::High, LevelIdc::Level3_1, "64001f"),
        (Profile::High10, LevelIdc::Level3_1, "6e001f"),
        (Profile::High422, LevelIdc::Level3_1, "7a001f"),
        (Profile::High444Predictive, LevelIdc::Level3_1, "f4001f"),
        (Profile::High10Intra, LevelIdc::Level3_1, "6e101f"),
        (Profile::High422Intra, LevelIdc::Level3_1, "7a101f"),
        (Profile::High444Intra, LevelIdc::Level3_1, "f4101f"),
        (Profile::Cavlc444Intra, LevelIdc::Level3_1, "2c101f"),
        (Profile::ConstrainedBaseline, LevelIdc::Level1B, "42f00b"),
        (Profile::High, LevelIdc::Level1B, "640009"),
    ];

    for (profile, level, fmtp) in cases {
        let profile_level_id = ProfileLevelId::new(profile, level);
        assert_eq!(profile_level_id.fmtp_value(), fmtp);
        assert_eq!(ProfileLevelId::parse(fmtp), Some(profile_level_id));
    }
}

#[test]
fn h264_profile_level_id_matches_profile_iop_wildcards() {
    let cases = [
        ("42f01f", Profile::ConstrainedBaseline),
        ("42801f", Profile::Baseline),
        ("4d401f", Profile::Main),
        ("58301f", Profile::Extended),
        ("6e101f", Profile::High10Intra),
    ];

    for (token, profile) in cases {
        assert_eq!(
            ProfileLevelId::parse(token).map(ProfileLevelId::profile),
            Some(profile)
        );
    }

    for token in ["42e11f", "6e201f"] {
        assert_eq!(ProfileLevelId::parse(token), None);
    }
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
fn h264_packetization_mode_parses_rfc_values() {
    let cases = [
        (0, PacketizationMode::SingleNalUnit),
        (1, PacketizationMode::NonInterleaved),
        (2, PacketizationMode::Interleaved),
    ];

    for (value, mode) in cases {
        assert_eq!(PacketizationMode::from_fmtp_value(value), Some(mode));
        assert_eq!(mode.fmtp_value(), value);
    }

    assert_eq!(PacketizationMode::from_fmtp_value(3), None);
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
    assert!(!h264::payload_starts_idr(
        &[0x78, 0x00, 0x01, 0x65, 0x00, 0x03, 0x41, 0x88],
        PacketizationMode::NonInterleaved
    ));
    for nested_header in [0x78, 0x7c] {
        assert!(!h264::payload_starts_idr(
            &[0x78, 0x00, 0x02, 0x65, 0x88, 0x00, 0x01, nested_header],
            PacketizationMode::NonInterleaved
        ));
    }
}

#[test]
fn rtcp_mux_payload_type_range_follows_rfc_5761() {
    for (value, allowed) in [
        (63, true),
        (64, false),
        (95, false),
        (96, true),
        (127, true),
        (128, false),
    ] {
        assert_eq!(super::is_rtcp_mux_payload_type(value), allowed);
        assert_eq!(PayloadType::try_new(value).is_some(), allowed);
    }
    assert_eq!(PayloadType::try_from(96), Ok(PayloadType::new(96)));
    assert_eq!(PayloadType::try_from(64), Err(super::InvalidPayloadType));
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
    assert!(!super::vp8::payload_starts_keyframe(&[0x10, 0x00]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x10, 0x01]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x00, 0x00]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x11, 0x00]));
    assert!(super::vp8::payload_starts_keyframe(&[
        0x10, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
    ]));
    assert!(super::vp8::payload_starts_keyframe(&[
        0x90, 0x80, 0x42, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
    ]));
    assert!(super::vp8::payload_starts_keyframe(&[
        0x90, 0x80, 0x80, 0x42, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
    ]));
    assert!(!super::vp8::payload_starts_keyframe(&[
        0x90, 0x40, 0x42, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
    ]));
    assert!(!super::vp8::payload_starts_keyframe(&[
        0x10, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2b, 0x80, 0x02, 0x68, 0x01,
    ]));
    assert!(!super::vp8::payload_starts_keyframe(&[
        0x10, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x00, 0x00, 0x68, 0x01,
    ]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x90, 0x40, 0, 0]));
    assert!(!super::vp8::payload_starts_keyframe(&[0x90, 0x80]));
}
