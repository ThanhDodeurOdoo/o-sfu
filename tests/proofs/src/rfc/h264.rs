use o_sfu_rfc::rtp::h264::{LevelIdc, Profile, ProfileLevelId};

const CONSTRAINT_SET3_FLAG: u8 = 0x10;

/// Proves the full 24-bit `profile-level-id` space against an independent
/// RFC-shaped model. Profile parsing feeds every later H264 compatibility
/// decision while unit tests can only sample the accepted and rejected space.
#[kani::proof]
fn h264_profile_level_id_parse_matches_rfc_patterns() {
    let raw = kani::any::<u32>() & 0x00FF_FFFF;
    let token = hex_profile_level_id(raw);
    let parsed = ProfileLevelId::parse_ascii_bytes(&token);
    let bytes = raw.to_be_bytes();
    let expected = spec_profile_from_bytes(bytes[1], bytes[2])
        .zip(spec_normalized_level_idc(bytes[1], bytes[2], bytes[3]));

    assert_eq!(parsed.is_some(), expected.is_some());
    if let (Some(parsed), Some((profile, level))) = (parsed, expected) {
        assert_eq!(parsed.profile(), profile);
        assert_eq!(parsed.level(), level);
    }
}

fn hex_profile_level_id(raw: u32) -> [u8; 6] {
    [
        hex_byte(((raw >> 20) & 0x0F) as u8),
        hex_byte(((raw >> 16) & 0x0F) as u8),
        hex_byte(((raw >> 12) & 0x0F) as u8),
        hex_byte(((raw >> 8) & 0x0F) as u8),
        hex_byte(((raw >> 4) & 0x0F) as u8),
        hex_byte((raw & 0x0F) as u8),
    ]
}

fn hex_byte(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        10..=15 => b'a' + (nibble - 10),
        _ => b'0',
    }
}

fn spec_normalized_level_idc(profile_idc: u8, profile_iop: u8, level_idc: u8) -> Option<LevelIdc> {
    if matches!(profile_idc, 0x42 | 0x4D | 0x58) {
        if level_idc == 9 {
            return None;
        }
        if level_idc == 11 {
            return if (profile_iop & CONSTRAINT_SET3_FLAG) != 0 {
                Some(LevelIdc::Level1B)
            } else {
                Some(LevelIdc::Level1_1)
            };
        }
    }
    match level_idc {
        9 => Some(LevelIdc::Level1B),
        10 => Some(LevelIdc::Level1),
        11 => Some(LevelIdc::Level1_1),
        12 => Some(LevelIdc::Level1_2),
        13 => Some(LevelIdc::Level1_3),
        20 => Some(LevelIdc::Level2),
        21 => Some(LevelIdc::Level2_1),
        22 => Some(LevelIdc::Level2_2),
        30 => Some(LevelIdc::Level3),
        31 => Some(LevelIdc::Level3_1),
        32 => Some(LevelIdc::Level3_2),
        40 => Some(LevelIdc::Level4),
        41 => Some(LevelIdc::Level4_1),
        42 => Some(LevelIdc::Level4_2),
        50 => Some(LevelIdc::Level5),
        51 => Some(LevelIdc::Level5_1),
        52 => Some(LevelIdc::Level5_2),
        _ => None,
    }
}

fn spec_profile_from_bytes(profile_idc: u8, profile_iop: u8) -> Option<Profile> {
    const PATTERNS: &[(Profile, u8, u8, u8)] = &[
        (Profile::ConstrainedBaseline, 0x42, 0b0100_1111, 0b0100_0000),
        (Profile::ConstrainedBaseline, 0x4D, 0b1000_1111, 0b1000_0000),
        (Profile::ConstrainedBaseline, 0x58, 0b1100_1111, 0b1100_0000),
        (Profile::Baseline, 0x42, 0b0100_1111, 0b0000_0000),
        (Profile::Baseline, 0x58, 0b1100_1111, 0b1000_0000),
        (Profile::Main, 0x4D, 0b1010_1111, 0b0000_0000),
        (Profile::Extended, 0x58, 0b1100_1111, 0b0000_0000),
        (Profile::High, 0x64, 0b1111_1111, 0b0000_0000),
        (Profile::High10, 0x6E, 0b1111_1111, 0b0000_0000),
        (Profile::High422, 0x7A, 0b1111_1111, 0b0000_0000),
        (Profile::High444Predictive, 0xF4, 0b1111_1111, 0b0000_0000),
        (Profile::High10Intra, 0x6E, 0b1111_1111, 0b0001_0000),
        (Profile::High422Intra, 0x7A, 0b1111_1111, 0b0001_0000),
        (Profile::High444Intra, 0xF4, 0b1111_1111, 0b0001_0000),
        (Profile::Cavlc444Intra, 0x2C, 0b1111_1111, 0b0001_0000),
    ];

    PATTERNS
        .iter()
        .find_map(|(profile, expected_profile_idc, mask, expected_bits)| {
            (*expected_profile_idc == profile_idc && (profile_iop & *mask) == *expected_bits)
                .then_some(*profile)
        })
}
