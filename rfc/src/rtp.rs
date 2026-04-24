//! RFC references covered:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>
//! - RTCP feedback profile: <https://www.rfc-editor.org/rfc/rfc4585>
//! - RTP stream identifier SDES items: <https://www.rfc-editor.org/rfc/rfc8852>
//! - Video frame marking RTP header extension: <https://www.rfc-editor.org/rfc/rfc9626>
//! - Layer Refresh Request feedback: <https://www.rfc-editor.org/rfc/rfc9627>

use std::fmt;

/// RTP version defined by RFC 3550 section 5.1.
pub const RTP_VERSION: u8 = 2;

/// Fixed RTP header length in bytes with no CSRC and no extension.
///
/// Reference: RFC 3550 section 5.1.
pub const RTP_FIXED_HEADER_BYTES: usize = 12;

/// Maximum number of CSRC identifiers in the RTP fixed header.
///
/// The CC field is 4 bits, so the valid range is 0..=15.
/// Reference: RFC 3550 section 5.1.
pub const RTP_MAX_CSRC_COUNT: u8 = 15;

/// Sequence number rollover modulus for the 16-bit RTP sequence number.
///
/// Reference: RFC 3550 section 5.1.
pub const RTP_SEQUENCE_NUMBER_MODULUS: u32 = 1_u32 << 16;

/// Timestamp rollover modulus for the 32-bit RTP timestamp field.
///
/// Reference: RFC 3550 section 5.1.
pub const RTP_TIMESTAMP_MODULUS: u64 = 1_u64 << 32;

/// Dynamic RTP payload type range in RTP/AVP.
///
/// Reference: RFC 3551 section 6.
pub const RTP_DYNAMIC_PAYLOAD_TYPE_START: u8 = 96;
pub const RTP_DYNAMIC_PAYLOAD_TYPE_END: u8 = 127;

/// Payload type values reserved to avoid confusion with RTCP packet types.
///
/// When the marker bit is set, these payload types produce second-byte values
/// that collide with RTCP packet types 200–204 (SR, RR, SDES, BYE, APP).
/// This is critical for RTP/RTCP multiplexing per RFC 5761 section 4.
///
/// Reference: RFC 3551 section 6.
pub const RTP_RESERVED_PAYLOAD_TYPE_72: u8 = 72;
pub const RTP_RESERVED_PAYLOAD_TYPE_73: u8 = 73;
pub const RTP_RESERVED_PAYLOAD_TYPE_74: u8 = 74;
pub const RTP_RESERVED_PAYLOAD_TYPE_75: u8 = 75;
pub const RTP_RESERVED_PAYLOAD_TYPE_76: u8 = 76;

/// Common static RTP/AVP payload type assignments.
///
/// Reference: RFC 3551 section 6, tables 4 and 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AvpStaticPayloadType {
    Pcmu = 0,
    Gsm = 3,
    G723 = 4,
    Dvi4_8000 = 5,
    Dvi4_16000 = 6,
    Lpc = 7,
    Pcma = 8,
    G722 = 9,
    L16Stereo = 10,
    L16Mono = 11,
    Qcelp = 12,
    ComfortNoise = 13,
    Mpa = 14,
    G728 = 15,
    Dvi4_11025 = 16,
    Dvi4_22050 = 17,
    G729 = 18,
    Celb = 25,
    Jpeg = 26,
    Nv = 28,
    H261 = 31,
    Mpv = 32,
    Mp2t = 33,
    H263 = 34,
}

impl AvpStaticPayloadType {
    #[must_use]
    #[expect(
        clippy::as_conversions,
        reason = "repr(u8) guarantees safe identity cast"
    )]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// RTP payload-format MIME subtype names commonly used by WebRTC endpoints.
pub mod codec_name {
    /// Opus RTP payload-format subtype.
    ///
    /// Reference: RFC 7587.
    pub const OPUS: &str = "opus";

    /// VP8 RTP payload-format subtype.
    ///
    /// Reference: RFC 7741.
    pub const VP8: &str = "VP8";

    /// H264 RTP payload-format subtype.
    ///
    /// Reference: RFC 6184.
    pub const H264: &str = "H264";

    /// RTX retransmission RTP payload-format subtype.
    ///
    /// Reference: RFC 4588.
    pub const RTX: &str = "rtx";
}

/// RTP payload-format MIME subtype names commonly used by WebRTC endpoints.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CodecName {
    Opus,
    Vp8,
    H264,
    Rtx,
    Other(String),
}

impl CodecName {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Opus => codec_name::OPUS,
            Self::Vp8 => codec_name::VP8,
            Self::H264 => codec_name::H264,
            Self::Rtx => codec_name::RTX,
            Self::Other(name) => name.as_str(),
        }
    }

    #[must_use]
    pub fn is_rtx(&self) -> bool {
        matches!(self, Self::Rtx)
    }
}

impl From<&str> for CodecName {
    fn from(value: &str) -> Self {
        if value.eq_ignore_ascii_case(codec_name::OPUS) {
            return Self::Opus;
        }
        if value.eq_ignore_ascii_case(codec_name::VP8) {
            return Self::Vp8;
        }
        if value.eq_ignore_ascii_case(codec_name::H264) {
            return Self::H264;
        }
        if value.eq_ignore_ascii_case(codec_name::RTX) {
            return Self::Rtx;
        }
        Self::Other(value.to_owned())
    }
}

impl From<String> for CodecName {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl AsRef<str> for CodecName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for CodecName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Codec `fmtp` parameter names and canonical valuse
pub mod fmtp {
    /// RTX associated payload type parameter
    ///
    /// Reference: RFC 4588 section 8.
    pub const RTX_ASSOCIATION: &str = "apt";

    /// H264 packetization mode parameter.
    ///
    /// Reference: RFC 6184 section 8.1.
    pub const H264_PACKETIZATION_MODE: &str = "packetization-mode";

    /// H264 profile-level-id parameter.
    ///
    /// Reference: RFC 6184 section 8.1
    pub const H264_PROFILE_LEVEL_ID: &str = "profile-level-id";

    /// Default H264 packetization mode when the parameter is omitted.
    ///
    /// Reference: RFC 6184 section 6.2.
    pub const H264_DEFAULT_PACKETIZATION_MODE: u8 = 0;

    /// VP9 profile-id parameter.
    ///
    /// Reference: RFC 9628 section 4.2.
    pub const VP9_PROFILE_ID: &str = "profile-id";

    /// Default VP9 profile when `profile-id` is omitted.
    ///
    /// Reference: RFC 9628 section 4.2.
    pub const VP9_DEFAULT_PROFILE_ID: u8 = 0;

    /// Opus in-band FEC parameter
    ///
    /// Reference: RFC 7587 section 6.1.
    pub const OPUS_USE_IN_BAND_FEC: &str = "useinbandfec";

    /// Canonical numeric enabled flag used in WebRTC `fmtp` dictionaries.
    pub const VALUE_ENABLED: &str = "1";

    /// Canonical numeric disabled flag used in WebRTC `fmtp` dictionaries
    pub const VALUE_DISABLED: &str = "0";

    /// Textual enabled flag accepted by current ORTC/WebRTC capability payloads
    pub const VALUE_TRUE: &str = "true";

    /// Textual disabled flag accepted by current ORTC/WebRTC capability payloads
    pub const VALUE_FALSE: &str = "false";
}

/// H264 SDP and payload-format helpers derived from RFC 6184.
pub mod h264 {
    use std::cmp::Ordering;

    /// Parsed H264 `profile-level-id` value.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ProfileLevelId {
        profile: Profile,
        level: LevelIdc,
    }

    impl ProfileLevelId {
        /// Parse the RFC 6184 `profile-level-id` hex token.
        #[must_use]
        pub fn parse(value: &str) -> Option<Self> {
            Self::parse_ascii_bytes(value.as_bytes())
        }

        /// Parse the six-byte ASCII hex form of a `profile-level-id` token.
        #[must_use]
        pub fn parse_ascii_bytes(value: &[u8]) -> Option<Self> {
            let [profile_idc, profile_iop, level_idc] = parse_profile_level_id_bytes(value)?;
            let level = normalized_level_idc(profile_idc, profile_iop, level_idc)?;
            let profile = profile_from_bytes(profile_idc, profile_iop)?;
            Some(Self { profile, level })
        }

        #[must_use]
        pub const fn profile(self) -> Profile {
            self.profile
        }

        #[must_use]
        pub const fn level(self) -> LevelIdc {
            self.level
        }
    }

    /// H264 profiles defined by RFC 6184 section 8.1.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Profile {
        Baseline,
        ConstrainedBaseline,
        Main,
        Extended,
        High,
        High10,
        High422,
        High444Predictive,
        High10Intra,
        High422Intra,
        High444Intra,
        Cavlc444Intra,
    }

    /// H264 level identifiers ordered by decoder capability.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum LevelIdc {
        Level1,
        Level1B,
        Level1_1,
        Level1_2,
        Level1_3,
        Level2,
        Level2_1,
        Level2_2,
        Level3,
        Level3_1,
        Level3_2,
        Level4,
        Level4_1,
        Level4_2,
        Level5,
        Level5_1,
        Level5_2,
    }

    impl LevelIdc {
        #[must_use]
        const fn ordinal(self) -> u8 {
            match self {
                Self::Level1 => 0,
                Self::Level1B => 1,
                Self::Level1_1 => 2,
                Self::Level1_2 => 3,
                Self::Level1_3 => 4,
                Self::Level2 => 5,
                Self::Level2_1 => 6,
                Self::Level2_2 => 7,
                Self::Level3 => 8,
                Self::Level3_1 => 9,
                Self::Level3_2 => 10,
                Self::Level4 => 11,
                Self::Level4_1 => 12,
                Self::Level4_2 => 13,
                Self::Level5 => 14,
                Self::Level5_1 => 15,
                Self::Level5_2 => 16,
            }
        }
    }

    impl PartialOrd for LevelIdc {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for LevelIdc {
        fn cmp(&self, other: &Self) -> Ordering {
            self.ordinal().cmp(&other.ordinal())
        }
    }

    impl TryFrom<u8> for LevelIdc {
        type Error = ();

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                0 => Ok(Self::Level1B),
                10 => Ok(Self::Level1),
                11 => Ok(Self::Level1_1),
                12 => Ok(Self::Level1_2),
                13 => Ok(Self::Level1_3),
                20 => Ok(Self::Level2),
                21 => Ok(Self::Level2_1),
                22 => Ok(Self::Level2_2),
                30 => Ok(Self::Level3),
                31 => Ok(Self::Level3_1),
                32 => Ok(Self::Level3_2),
                40 => Ok(Self::Level4),
                41 => Ok(Self::Level4_1),
                42 => Ok(Self::Level4_2),
                50 => Ok(Self::Level5),
                51 => Ok(Self::Level5_1),
                52 => Ok(Self::Level5_2),
                _ => Err(()),
            }
        }
    }

    const H264_LEVEL_1B_CONSTRAINT_SET3_FLAG: u8 = 0x10;
    const H264_PROFILE_PATTERNS: &[(Profile, u8, u8, u8)] = &[
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

    fn normalized_level_idc(profile_idc: u8, profile_iop: u8, level_idc: u8) -> Option<LevelIdc> {
        if [0x42, 0x4D, 0x58].contains(&profile_idc) && level_idc == 11 {
            return if (profile_iop & H264_LEVEL_1B_CONSTRAINT_SET3_FLAG) != 0 {
                Some(LevelIdc::Level1B)
            } else {
                Some(LevelIdc::Level1)
            };
        }
        LevelIdc::try_from(level_idc).ok()
    }

    fn profile_from_bytes(profile_idc: u8, profile_iop: u8) -> Option<Profile> {
        H264_PROFILE_PATTERNS.iter().find_map(
            |(profile, expected_profile_idc, mask, expected_bits)| {
                (*expected_profile_idc == profile_idc && (profile_iop & *mask) == *expected_bits)
                    .then_some(*profile)
            },
        )
    }

    fn parse_profile_level_id_bytes(value: &[u8]) -> Option<[u8; 3]> {
        let [
            first_high,
            first_low,
            second_high,
            second_low,
            third_high,
            third_low,
        ] = value
        else {
            return None;
        };
        Some([
            decode_hex_byte(*first_high, *first_low)?,
            decode_hex_byte(*second_high, *second_low)?,
            decode_hex_byte(*third_high, *third_low)?,
        ])
    }

    fn decode_hex_byte(high: u8, low: u8) -> Option<u8> {
        Some((decode_hex_nibble(high)? << 4) | decode_hex_nibble(low)?)
    }

    fn decode_hex_nibble(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            b'A'..=b'F' => Some(value - b'A' + 10),
            _ => None,
        }
    }
}

/// Returns `true` if `payload_type` falls in the dynamic range (96–127).
///
/// Reference: RFC 3551sectionn 6
#[must_use]
pub const fn is_dynamic_payload_type(payload_type: u8) -> bool {
    payload_type >= RTP_DYNAMIC_PAYLOAD_TYPE_START && payload_type <= RTP_DYNAMIC_PAYLOAD_TYPE_END
}

/// RTCP packet type codes defined by RFC 3550 section 12.1.
pub const RTCP_PACKET_TYPE_SR: u8 = 200;
pub const RTCP_PACKET_TYPE_RR: u8 = 201;
pub const RTCP_PACKET_TYPE_SDES: u8 = 202;
pub const RTCP_PACKET_TYPE_BYE: u8 = 203;
pub const RTCP_PACKET_TYPE_APP: u8 = 204;
/// RTCP transport-layer feedback packet type.
///
/// Reference: RFC 4585 section 6.1.
pub const RTCP_PACKET_TYPE_RTPFB: u8 = 205;
/// RTCP payload-specific feedback packet type.
///
/// Reference: RFC 4585 section 6.1.
pub const RTCP_PACKET_TYPE_PSFB: u8 = 206;

/// RTCP SDES item type codes from RFC 3550 section 12.2.
pub const RTCP_SDES_ITEM_CNAME: u8 = 1;
pub const RTCP_SDES_ITEM_NAME: u8 = 2;
pub const RTCP_SDES_ITEM_EMAIL: u8 = 3;
pub const RTCP_SDES_ITEM_PHONE: u8 = 4;
pub const RTCP_SDES_ITEM_LOC: u8 = 5;
pub const RTCP_SDES_ITEM_TOOL: u8 = 6;
pub const RTCP_SDES_ITEM_NOTE: u8 = 7;
pub const RTCP_SDES_ITEM_PRIV: u8 = 8;
/// RTCP SDES item type for `RtpStreamId`.
///
/// Reference: RFC 8852 section 4.1.
pub const RTCP_SDES_ITEM_RTP_STREAM_ID: u8 = 12;
/// RTCP SDES item type for `RepairedRtpStreamId`.
///
/// Reference: RFC 8852 section 4.2.
pub const RTCP_SDES_ITEM_REPAIRED_RTP_STREAM_ID: u8 = 13;

/// RTCP feedback FMT values.
pub mod rtcp_feedback_format {
    /// Generic NACK FMT value for RTPFB packets.
    ///
    /// Reference: RFC 4585 section 6.2.1.
    pub const RTPFB_GENERIC_NACK: u8 = 1;

    /// Picture Loss Indication FMT value for PSFB packets.
    ///
    /// Reference: RFC 4585 section 6.3.1.
    pub const PSFB_PLI: u8 = 1;

    /// Full Intra Request FMT value for PSFB packets.
    ///
    /// Reference: RFC 5104 section 4.3.1.
    pub const PSFB_FIR: u8 = 4;

    /// Layer Refresh Request FMT value for PSFB packets.
    ///
    /// Reference: RFC 9627 section 8.
    pub const PSFB_LRR: u8 = 10;
}

/// RTP header-extension profile IDs from RFC 8285.
pub mod header_extension {
    /// RFC 8285 one-byte header extension profile ID.
    pub const ONE_BYTE_PROFILE_ID: u16 = 0xBEDE;

    /// Base value for the RFC 8285 two-byte header profile with appbits set to 0.
    pub const TWO_BYTE_PROFILE_ID_BASE: u16 = 0x1000;

    /// Mask for the fixed 12-bit prefix (`0x100`) in the two-byte profile ID.
    pub const TWO_BYTE_PROFILE_PREFIX_MASK: u16 = 0xFFF0;

    /// One-byte header extension identifier values.
    pub const ONE_BYTE_ID_PAD: u8 = 0;
    pub const ONE_BYTE_ID_MIN: u8 = 1;
    pub const ONE_BYTE_ID_MAX: u8 = 14;
    pub const ONE_BYTE_ID_RESERVED: u8 = 15;

    /// One-byte header extension data size bounds from RFC 8285 section 4.2.
    pub const ONE_BYTE_DATA_LEN_MIN: u8 = 1;
    pub const ONE_BYTE_DATA_LEN_MAX: u8 = 16;

    /// Two-byte header extension data size bounds from RFC 8285 section 4.3.
    pub const TWO_BYTE_DATA_LEN_MIN: u8 = 0;
    pub const TWO_BYTE_DATA_LEN_MAX: u8 = u8::MAX;

    #[must_use]
    pub const fn is_one_byte_id(id: u8) -> bool {
        id >= ONE_BYTE_ID_MIN && id <= ONE_BYTE_ID_MAX
    }

    #[must_use]
    pub fn two_byte_profile_id(appbits: u8) -> Option<u16> {
        if appbits > 0x0F {
            return None;
        }
        Some(TWO_BYTE_PROFILE_ID_BASE | u16::from(appbits))
    }
}

/// Video Frame Marking RTP header-extension payload values.
///
/// Reference: RFC 9626 section 3.
pub mod frame_marking {
    /// Full long-form frame-marking payload length.
    pub const LONG_DATA_LEN_WITH_TL0PICIDX: u8 = 3;

    /// Long-form payload length when TL0PICIDX is omitted.
    pub const LONG_DATA_LEN_WITHOUT_TL0PICIDX: u8 = 2;

    /// Long-form payload length when both LID and TL0PICIDX are omitted.
    pub const LONG_DATA_LEN_FLAGS_ONLY: u8 = 1;

    /// Short-form non-scalable frame-marking payload length.
    pub const SHORT_DATA_LEN: u8 = 1;

    /// Start-of-frame flag in the first frame-marking octet.
    pub const START_OF_FRAME_MASK: u8 = 0b1000_0000;

    /// End-of-frame flag in the first frame-marking octet.
    pub const END_OF_FRAME_MASK: u8 = 0b0100_0000;

    /// Independent-frame flag in the first frame-marking octet.
    pub const INDEPENDENT_FRAME_MASK: u8 = 0b0010_0000;

    /// Discardable-frame flag in the first frame-marking octet.
    pub const DISCARDABLE_FRAME_MASK: u8 = 0b0001_0000;

    /// Base-layer-sync flag in the long-form first octet.
    pub const BASE_LAYER_SYNC_MASK: u8 = 0b0000_1000;

    /// Temporal-layer identifier bits in the long-form first octet.
    pub const TEMPORAL_LAYER_ID_MASK: u8 = 0b0000_0111;

    /// Maximum temporal-layer identifier representable by the 3-bit TID field.
    pub const TEMPORAL_LAYER_ID_MAX: u8 = 7;

    /// Base layer identifier used for TID and LID.
    pub const BASE_LAYER_ID: u8 = 0;

    #[must_use]
    pub const fn temporal_layer_id(first_octet: u8) -> u8 {
        first_octet & TEMPORAL_LAYER_ID_MASK
    }

    #[must_use]
    pub const fn is_valid_temporal_layer_id(value: u8) -> bool {
        value <= TEMPORAL_LAYER_ID_MAX
    }
}

#[cfg(test)]
mod tests {
    use super::h264::{LevelIdc, Profile, ProfileLevelId};
    use super::{frame_marking, header_extension, rtcp_feedback_format};

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
    fn h264_level_ordering_keeps_level_1b_between_level_1_and_level_1_1() {
        assert!(LevelIdc::Level1 < LevelIdc::Level1B);
        assert!(LevelIdc::Level1B < LevelIdc::Level1_1);
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
}
