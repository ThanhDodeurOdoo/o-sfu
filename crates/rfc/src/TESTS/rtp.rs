use super::{
    CodecName, PayloadType, RtpRtcpMuxPacketKind, Ssrc, classify_rtp_rtcp_mux, fmtp, frame_marking,
    generic_nack_sequence_numbers,
    h264::{self, LevelIdc, PacketizationMode, Profile, ProfileLevelId},
    header_extension, is_rtcp_mux_dynamic_payload_type, parse_muxed_rtp_fixed_header,
    rtcp_receiver_report_without_report_blocks, rtx_original_sequence_number,
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
fn rtcp_mux_dynamic_payload_types_include_unassigned_avp_ranges() {
    for payload_type in [20, 24, 27, 29, 30, 35, 63, 96, 127] {
        assert!(is_rtcp_mux_dynamic_payload_type(payload_type));
    }
    for payload_type in [0, 19, 25, 26, 28, 31, 34, 64, 95, 128] {
        assert!(!is_rtcp_mux_dynamic_payload_type(payload_type));
    }
}

#[test]
fn rtp_rtcp_mux_classification_uses_version_and_second_octet() {
    for (second, kind) in [
        (63, RtpRtcpMuxPacketKind::Rtp),
        (64, RtpRtcpMuxPacketKind::Rtcp),
        (95, RtpRtcpMuxPacketKind::Rtcp),
        (96, RtpRtcpMuxPacketKind::Rtp),
        (191, RtpRtcpMuxPacketKind::Rtp),
        (192, RtpRtcpMuxPacketKind::Rtcp),
        (223, RtpRtcpMuxPacketKind::Rtcp),
        (224, RtpRtcpMuxPacketKind::Rtp),
    ] {
        assert_eq!(classify_rtp_rtcp_mux(&[128, second, 0]), Some(kind));
    }
    for packet in [
        &[][..],
        &[128][..],
        &[128, 96][..],
        &[127, 96, 0][..],
        &[192, 96, 0][..],
    ] {
        assert_eq!(classify_rtp_rtcp_mux(packet), None);
    }
}

#[test]
fn muxed_rtp_fixed_header_exposes_wire_fields_and_flags() {
    let packet = [
        0xb1, 0xe0, 0x12, 0x34, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xde, 0xad, 0xbe,
        0xef,
    ];
    let header = parse_muxed_rtp_fixed_header(&packet);
    assert!(header.is_some());
    let Some(header) = header else {
        return;
    };

    assert_eq!(header.payload_type(), 96);
    assert_eq!(header.sequence_number(), 0x1234);
    assert_eq!(header.timestamp(), 0x0123_4567);
    assert_eq!(header.ssrc(), Ssrc::new(0x89ab_cdef));
    assert!(header.marker());
    assert!(header.has_padding());
    assert!(header.has_extension());
    assert!(parse_muxed_rtp_fixed_header(&packet[..15]).is_none());

    let mut rtcp = packet;
    rtcp[1] = 200;
    assert!(parse_muxed_rtp_fixed_header(&rtcp).is_none());
}

#[test]
fn generic_nack_expansion_includes_pid_bits_and_rollover() {
    assert_eq!(
        generic_nack_sequence_numbers(u16::MAX - 1, 0b101).collect::<Vec<_>>(),
        [u16::MAX - 1, u16::MAX, 1]
    );
    assert_eq!(
        generic_nack_sequence_numbers(42, 0).collect::<Vec<_>>(),
        [42]
    );
    assert_eq!(
        generic_nack_sequence_numbers(42, 1 << 15).collect::<Vec<_>>(),
        [42, 58]
    );
}

#[test]
fn rtx_payload_starts_with_original_sequence_number() {
    assert_eq!(
        rtx_original_sequence_number(&[0x12, 0x34, 0xaa]),
        Some(0x1234)
    );
    assert_eq!(rtx_original_sequence_number(&[0x12]), None);
}

#[test]
fn empty_receiver_report_uses_rtcp_length_units() {
    assert_eq!(
        rtcp_receiver_report_without_report_blocks(Ssrc::new(0x0102_0304)),
        [0x80, 201, 0, 1, 1, 2, 3, 4]
    );
}

#[test]
fn one_byte_header_extension_lookup_handles_csrc_and_padding() {
    let packet = one_byte_extension_packet([0, 0x31, 0xaa, 0xbb, 0x40, 0xcc, 0, 0]);

    assert_eq!(
        header_extension::find_one_byte_element(&packet, 3),
        Some(&[0xaa, 0xbb][..])
    );
    assert_eq!(
        header_extension::find_one_byte_element(&packet, 4),
        Some(&[0xcc][..])
    );
    assert_eq!(header_extension::find_one_byte_element(&packet, 5), None);
    assert_eq!(header_extension::find_one_byte_element(&packet, 0), None);
    assert_eq!(header_extension::find_one_byte_element(&packet, 15), None);
}

#[test]
fn one_byte_header_extension_lookup_rejects_reserved_id_forms() {
    let malformed_padding = one_byte_extension_packet([0x01, 0xaa, 0xbb, 0x30, 0xcc, 0, 0, 0]);
    let reserved = one_byte_extension_packet([0xf0, 0x30, 0xcc, 0, 0, 0, 0, 0]);

    assert_eq!(
        header_extension::find_one_byte_element(&malformed_padding, 3),
        None
    );
    assert_eq!(header_extension::find_one_byte_element(&reserved, 3), None);
}

fn one_byte_extension_packet(elements: [u8; 8]) -> Vec<u8> {
    let mut packet = vec![
        0x91, 96, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0xbe, 0xde, 0, 2,
    ];
    packet.extend(elements);
    packet
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

#[test]
fn codec_name_from_str_is_case_insensitive() {
    assert_eq!(CodecName::from("opus"), CodecName::Opus);
    assert_eq!(CodecName::from("OPUS"), CodecName::Opus);
    assert_eq!(CodecName::from("vp8"), CodecName::Vp8);
    assert_eq!(CodecName::from("VP8"), CodecName::Vp8);
    assert_eq!(CodecName::from("h264"), CodecName::H264);
    assert_eq!(CodecName::from("H264"), CodecName::H264);
    assert_eq!(CodecName::from("rtx"), CodecName::Rtx);
    assert_eq!(CodecName::from("RTX"), CodecName::Rtx);
    assert_eq!(
        CodecName::from("custom-codec"),
        CodecName::Other("custom-codec".to_owned())
    );
}
