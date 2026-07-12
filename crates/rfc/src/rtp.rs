//! RFC references covered:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>
//! - RTCP feedback profile: <https://www.rfc-editor.org/rfc/rfc4585>
//! - RTP stream identifier SDES items: <https://www.rfc-editor.org/rfc/rfc8852>
//! - Video frame marking RTP header extension: <https://www.rfc-editor.org/rfc/rfc9626>
//! - Layer Refresh Request feedback: <https://www.rfc-editor.org/rfc/rfc9627>
//! - RTP payload format for VP8: <https://www.rfc-editor.org/rfc/rfc7741>
//! - RTP payload format for H264: <https://www.rfc-editor.org/rfc/rfc6184>
//!
//! A complete RTP packet has this outer shape:
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |V=2|P|X|  CC   |M|     PT      |       sequence number         |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |                           timestamp                           |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |                             SSRC                              |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |                         CSRC list ...                         |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |              extension block if X=1 ...                       |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |                         codec payload ...                     |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! ```

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

/// Maximum value representable by the 7-bit RTP payload type field.
///
/// Reference: RFC 3550 section 5.1.
pub const RTP_PAYLOAD_TYPE_MAX: u8 = 127;

/// Dynamic RTP payload type range in RTP/AVP.
///
/// Reference: RFC 3551 section 6.
pub const RTP_DYNAMIC_PAYLOAD_TYPE_START: u8 = 96;
pub const RTP_DYNAMIC_PAYLOAD_TYPE_END: u8 = 127;

/// Payload type range disallowed when RTP and RTCP share one port.
///
/// RFC 5761 reserves the full 64 through 95 payload type range for muxed
/// sessions so the second packet octet can unambiguously distinguish RTP from
/// RTCP.
///
/// Reference: RFC 5761 section 4.
pub const RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_START: u8 = 64;
pub const RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_END: u8 = 95;

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

/// 7-bit RTP payload type value usable in RTP/RTCP muxed sessions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PayloadType(u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPayloadType;

impl PayloadType {
    #[must_use]
    pub const fn try_new(value: u8) -> Option<Self> {
        if is_rtcp_mux_payload_type(value) {
            Some(Self(value))
        } else {
            None
        }
    }

    /// builds a payload type for the muxed RTP sessions used by `o-sfu`
    ///
    /// # Panics
    ///
    /// panics when `value` does not fit the RTP payload type field or is in the
    /// RTP/RTCP mux forbidden range from RFC 5761 section 4
    #[must_use]
    pub const fn new(value: u8) -> Self {
        assert!(is_rtcp_mux_payload_type(value));
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for PayloadType {
    type Error = InvalidPayloadType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_new(value).ok_or(InvalidPayloadType)
    }
}

impl From<PayloadType> for u8 {
    fn from(value: PayloadType) -> Self {
        value.value()
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

/// Opus RTP payload-format constants.
///
/// Reference: RFC 7587 for the RTP payload format and RFC 6716 for the
/// packet table-of-contents layout.
pub mod opus {
    /// RTP clock rate for Opus payloads.
    ///
    /// Reference: RFC 7587 section 4.1.
    pub const RTP_CLOCK_RATE_HZ: u32 = 48_000;

    /// Opus packet frame-count codes from the table-of-contents byte.
    ///
    /// Reference: RFC 6716 section 3.1.
    pub mod frame_count_code {
        /// One frame in the Opus packet.
        pub const ONE_FRAME: u8 = 0;
    }

    /// Opus table-of-contents configuration numbers.
    ///
    /// Reference: RFC 6716 section 3.1.
    pub mod toc_config {
        /// SILK-only wideband packet with a 20 ms frame duration.
        pub const SILK_WIDEBAND_20_MS: u8 = 9;
    }
}

/// VP8 RTP payload helpers.
///
/// These helpers operate on the codec payload slice, not on the full RTP
/// packet. In runtime forwarding that slice starts after the RTP fixed header,
/// CSRC list and any RTP header-extension block.
///
/// RFC 7741 puts a VP8 payload descriptor in front of the VP8 payload header.
/// The descriptor is also where simulcast and temporal-layer packet identity is
/// carried. o-sfu keeps the RFC constants here, while the runtime uses str0m's
/// descriptor parser and patch support for local egress.
///
/// ```text
/// VP8 payload slice passed to this module
///
/// +------------------+----------------------+----------------------+
/// | descriptor byte  | extension bytes      | VP8 payload header   |
/// +------------------+----------------------+----------------------+
/// | X R N S PartID   | I L T K fields       | P bit and VP8 data   |
/// +------------------+----------------------+----------------------+
///
/// Extension bytes when X=1
///
/// +-------------+-------------------+-------------+----------------+
/// | I L T K ... | PictureID if I=1  | TL0 if L=1  | T/K if present |
/// +-------------+-------------------+-------------+----------------+
/// |             | 1 or 2 bytes      | 1 byte      | 1 byte         |
/// +-------------+-------------------+-------------+----------------+
/// ```
pub mod vp8 {
    /// Extended control bits are present in the VP8 payload descriptor.
    pub const X_BIT: u8 = 0b1000_0000;

    /// Start of VP8 partition bit in the payload descriptor.
    pub const S_BIT: u8 = 0b0001_0000;

    const PARTITION_ID_MASK: u8 = 0b0000_1111;

    /// `PictureID` present bit in the extended VP8 payload descriptor.
    pub const I_BIT: u8 = 0b1000_0000;

    /// TL0PICIDX present bit in the extended VP8 payload descriptor.
    pub const L_BIT: u8 = 0b0100_0000;

    /// TID/Y/KEYIDX present bit in the extended VP8 payload descriptor.
    pub const T_BIT: u8 = 0b0010_0000;

    const K_BIT: u8 = 0b0001_0000;

    /// Long `PictureID` marker bit in the VP8 `PictureID` field.
    pub const LONG_PICTURE_ID_BIT: u8 = 0b1000_0000;

    /// VP8 payload-header P bit set for interframes.
    pub const INTERFRAME_BIT: u8 = 0b0000_0001;

    /// value mask for the 7-bit VP8 short `PictureID` field
    pub const SHORT_PICTURE_ID_MASK: u16 = 0x7f;

    /// Value mask for the 15-bit VP8 long `PictureID` field.
    pub const LONG_PICTURE_ID_MASK: u16 = 0x7fff;

    /// Modulus for the 15-bit VP8 long `PictureID` field.
    pub const LONG_PICTURE_ID_MODULUS: u16 = 1 << 15;

    /// Value mask for the two-bit VP8 temporal-layer identity.
    pub const TEMPORAL_LAYER_ID_MASK: u8 = 0b0000_0011;

    /// Detects the first RTP packet of a VP8 keyframe.
    ///
    /// RFC 7741 section 4.2 defines the VP8 payload descriptor. A decodable
    /// keyframe starts at partition 0 (`S=1`, `PartID=0`) and the VP8 payload
    /// header starts with `P=0`. The input must be the RTP codec payload. It is
    /// not a complete RTP packet.
    ///
    /// Truncated descriptors, missing payload headers and non-start partitions
    /// return `false`. The helper performs only the cheap
    /// keyframe probe needed by packet gates and decoder-refresh detection.
    #[must_use]
    pub fn payload_starts_keyframe(payload: &[u8]) -> bool {
        let Some((&descriptor, rest)) = payload.split_first() else {
            return false;
        };
        if descriptor & S_BIT == 0 || descriptor & PARTITION_ID_MASK != 0 {
            return false;
        }
        let payload_header = if descriptor & X_BIT == 0 {
            rest.first()
        } else {
            extended_payload_header(rest)
        };
        payload_header.is_some_and(|header| header & INTERFRAME_BIT == 0)
    }

    /// Locates the VP8 payload header after an extended descriptor.
    ///
    /// `None` means the extension bits advertise fields that are not present in
    /// the slice. The keyframe probe treats that as "not a keyframe".
    fn extended_payload_header(payload: &[u8]) -> Option<&u8> {
        let (&extension, mut rest) = payload.split_first()?;
        if extension & I_BIT != 0 {
            let (&picture_id, remaining) = rest.split_first()?;
            rest = if picture_id & LONG_PICTURE_ID_BIT != 0 {
                remaining.get(1..)?
            } else {
                remaining
            };
        }
        if extension & L_BIT != 0 {
            rest = rest.get(1..)?;
        }
        if extension & T_BIT != 0 || extension & K_BIT != 0 {
            rest = rest.get(1..)?;
        }
        rest.first()
    }
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

    /// Default H264 profile-level-id when the parameter is omitted.
    ///
    /// Reference: RFC 6184 section 8.1.
    pub const H264_DEFAULT_PROFILE_LEVEL_ID: &str = "42000a";

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
///
/// Packet helpers in this module receive the H264 RTP payload slice. The first
/// byte of that slice identifies the packetization mode used by the packet.
/// Decoder-refresh detection needs to recognize IDR access units in all packet
/// shapes that browsers commonly send.
///
/// ```text
/// Single NAL unit packet
///
/// +-------------+----------------------+
/// | NAL header  | NAL payload          |
/// +-------------+----------------------+
/// | F NRI Type  | ...                  |
/// +-------------+----------------------+
///
/// STAP-A aggregation packet
///
/// +---------------+----------+-------------+----------+-------------+
/// | STAP-A header | NAL size | NAL bytes   | NAL size | NAL bytes   |
/// +---------------+----------+-------------+----------+-------------+
/// | Type=24       | 16 bits  | starts with | 16 bits  | starts with |
/// |               |          | NAL header  |          | NAL header  |
/// +---------------+----------+-------------+----------+-------------+
///
/// FU-A fragmentation packet
///
/// +---------------+-------------+----------------------+
/// | FU indicator  | FU header   | fragment payload     |
/// +---------------+-------------+----------------------+
/// | Type=28       | S E R Type  | first fragment if S=1 |
/// +---------------+-------------+----------------------+
/// ```
///
/// rfc 6184 maps `profile-level-id` to three bytes:
///
/// ```text
/// profile_idc | profile-iop | level_idc
///
/// profile-iop bit layout:
///  7   6   5   4   3   2   1   0
/// +---+---+---+---+---+---+---+---+
/// |c0 |c1 |c2 |c3 |c4 |c5 | r | r |
/// +---+---+---+---+---+---+---+---+
///
/// cN = constraint_setN_flag
/// r  = reserved_zero bit
/// ```
pub mod h264 {
    /// Mask for the H264 NAL unit type field.
    pub const NAL_UNIT_TYPE_MASK: u8 = 0x1f;

    /// Highest NRI value for reference NAL units.
    pub const NAL_REF_IDC_HIGH: u8 = 0b0110_0000;

    /// H264 IDR slice NAL unit type.
    pub const NAL_UNIT_TYPE_IDR: u8 = 5;

    /// H264 STAP-A aggregation packet type.
    pub const NAL_UNIT_TYPE_STAP_A: u8 = 24;

    /// H264 FU-A fragmentation packet type.
    pub const NAL_UNIT_TYPE_FU_A: u8 = 28;

    /// FU-A start bit in the FU header.
    pub const FU_START_BIT: u8 = 0x80;

    /// FU-A end bit in the FU header.
    pub const FU_END_BIT: u8 = 0x40;

    /// H264 RTP packetization mode.
    ///
    /// Reference: RFC 6184 section 6.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PacketizationMode {
        SingleNalUnit,
        NonInterleaved,
        Interleaved,
    }

    impl PacketizationMode {
        #[must_use]
        pub const fn from_fmtp_value(value: u8) -> Option<Self> {
            match value {
                0 => Some(Self::SingleNalUnit),
                1 => Some(Self::NonInterleaved),
                2 => Some(Self::Interleaved),
                _ => None,
            }
        }

        #[must_use]
        pub const fn fmtp_value(self) -> u8 {
            match self {
                Self::SingleNalUnit => 0,
                Self::NonInterleaved => 1,
                Self::Interleaved => 2,
            }
        }

        const fn allows_aggregation_and_fragmentation(self) -> bool {
            matches!(self, Self::NonInterleaved)
        }
    }

    /// parsed H264 `profile-level-id` capability
    ///
    /// `profile_idc` names the broad H264 profile family, `profile-iop`
    /// carries constraint bits that narrow that family to a sub-profile and
    /// `level_idc` carries the decoder capability level. [`ProfileLevelId`]
    /// normalizes equivalent RFC 6184 profile encodings before router
    /// negotiation compares [`Profile`] and [`LevelIdc`]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ProfileLevelId {
        profile: Profile,
        level: LevelIdc,
    }

    impl ProfileLevelId {
        #[must_use]
        pub const fn new(profile: Profile, level: LevelIdc) -> Self {
            Self { profile, level }
        }

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

        #[must_use]
        pub const fn packed_value(self) -> u32 {
            let (profile_idc, profile_iop, level_idc) =
                profile_level_id_bytes(self.profile, self.level);
            pack_profile_level_id(profile_idc, profile_iop, level_idc)
        }

        #[must_use]
        pub fn fmtp_value(self) -> String {
            let value = self.packed_value();
            format!("{value:06x}")
        }
    }

    /// H264 sub-profile after equivalent RFC 6184 encodings are normalized
    ///
    /// the same sub-profile may be advertised through different
    /// `profile_idc` families when `profile-iop` constraints restrict them to
    /// the same coding tools
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

    /// H264 decoder capability level
    ///
    /// all levels except [`LevelIdc::Level1B`] map directly to `level_idc`
    /// level 1b was inserted between level 1 and level 1.1 after those byte
    /// values existed, so RFC 6184 gives it profile-dependent encodings
    ///
    /// the variant order is the negotiation order
    /// wire `level_idc` bytes are parsed and rendered explicitly because Level
    /// 1b is encoded specially
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

    /// Detects an H264 RTP packet that starts an IDR access unit.
    ///
    /// RFC 6184 carries IDR frames either as a single NAL unit, inside a STAP-A
    /// aggregation packet, or as the first fragment of a FU-A packet.
    ///
    /// The input must be the RTP codec payload, not a complete RTP packet.
    /// Truncated aggregation packets and incomplete FU-A packets return
    /// `false`. The helper does not parse a full access unit. It only answers
    /// whether this packet can refresh a decoder if forwarded.
    #[must_use]
    pub fn payload_starts_idr(payload: &[u8], packetization_mode: PacketizationMode) -> bool {
        let Some((&nal_header, rest)) = payload.split_first() else {
            return false;
        };
        match nal_header & NAL_UNIT_TYPE_MASK {
            NAL_UNIT_TYPE_IDR => true,
            NAL_UNIT_TYPE_STAP_A if packetization_mode.allows_aggregation_and_fragmentation() => {
                stap_a_contains_idr(rest)
            }
            NAL_UNIT_TYPE_FU_A if packetization_mode.allows_aggregation_and_fragmentation() => {
                fu_a_starts_idr(rest)
            }
            _ => false,
        }
    }

    /// Scans a STAP-A payload for any contained IDR NAL unit.
    ///
    /// each aggregate entry is length-prefixed and must use a single NAL unit type
    /// malformed lengths fail closed because the caller cannot safely trust later
    /// bytes as NAL boundaries
    fn stap_a_contains_idr(mut payload: &[u8]) -> bool {
        let mut contains_idr = false;
        while !payload.is_empty() {
            let Some((len, rest)) = payload.split_first_chunk::<2>() else {
                return false;
            };
            let nal_len = usize::from(u16::from_be_bytes(*len));
            let Some((nal, remaining_payload)) = rest.split_at_checked(nal_len) else {
                return false;
            };
            let Some(&nal_header) = nal.first() else {
                return false;
            };
            let nal_type = nal_header & NAL_UNIT_TYPE_MASK;
            if !(1..NAL_UNIT_TYPE_STAP_A).contains(&nal_type) {
                return false;
            }
            contains_idr |= nal_type == NAL_UNIT_TYPE_IDR;
            payload = remaining_payload;
        }
        contains_idr
    }

    /// Detects whether a FU-A packet is the first fragment of an IDR NAL unit.
    fn fu_a_starts_idr(payload: &[u8]) -> bool {
        let Some((&fu_header, _fragment)) = payload.split_first() else {
            return false;
        };
        fu_header & FU_START_BIT != 0
            && fu_header & FU_END_BIT == 0
            && fu_header & NAL_UNIT_TYPE_MASK == NAL_UNIT_TYPE_IDR
    }

    impl TryFrom<u8> for LevelIdc {
        type Error = ();

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            match value {
                9 => Ok(Self::Level1B),
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

    const BASELINE_IDC: u8 = 0x42;
    const MAIN_IDC: u8 = 0x4d;
    const EXTENDED_IDC: u8 = 0x58;
    const HIGH_IDC: u8 = 0x64;
    const HIGH_10_IDC: u8 = 0x6e;
    const HIGH_422_IDC: u8 = 0x7a;
    const HIGH_444_IDC: u8 = 0xf4;
    const CAVLC_444_IDC: u8 = 0x2c;

    // `profile-iop` is the middle byte of `profile-level-id`
    // constraint flags are H264 compatibility bits that refine `profile_idc`
    // into the sub-profile used for offer or answer matching
    //
    // bit 7 -> constraint_set0_flag -> `IOP_CONSTRAINT_SET0`
    // bit 6 -> constraint_set1_flag -> `IOP_CONSTRAINT_SET1`
    // bit 5 -> constraint_set2_flag -> `IOP_CONSTRAINT_SET2`
    // bit 4 -> constraint_set3_flag -> `IOP_CONSTRAINT_SET3`
    // bit 3 -> constraint_set4_flag -> fixed to 0 by profile-equivalence rows
    // bit 2 -> constraint_set5_flag -> fixed to 0 by profile-equivalence rows
    // bit 1..0 -> reserved_zero_2bits -> fixed to 0 by H264
    const IOP_NONE: u8 = 0x00;
    const IOP_CONSTRAINT_SET0: u8 = 0x80;
    const IOP_CONSTRAINT_SET1: u8 = 0x40;
    const IOP_CONSTRAINT_SET2: u8 = 0x20;
    const IOP_CONSTRAINT_SET3: u8 = 0x10;
    const IOP_PROFILE_PATTERN_LOW_ZERO_MASK: u8 =
        !(IOP_CONSTRAINT_SET0 | IOP_CONSTRAINT_SET1 | IOP_CONSTRAINT_SET2 | IOP_CONSTRAINT_SET3);
    const IOP_CONSTRAINED_BASELINE: u8 =
        IOP_CONSTRAINT_SET0 | IOP_CONSTRAINT_SET1 | IOP_CONSTRAINT_SET2;

    const LEVEL_1B_IDCS: [u8; 3] = [BASELINE_IDC, MAIN_IDC, EXTENDED_IDC];
    const LEVEL_1B_OTHER_IDC: u8 = 9;
    const LEVEL_1_1_IDC: u8 = 11;

    /// one accepted RFC 6184 profile-equivalence row
    ///
    /// a row maps one concrete `profile_idc` family and one wildcarded
    /// `profile-iop` bit pattern to the normalized [`Profile`] used by
    /// negotiation
    #[derive(Clone, Copy)]
    struct ProfilePattern {
        profile: Profile,
        profile_idc: u8,
        iop: ProfileIopPattern,
    }

    impl ProfilePattern {
        const fn masked(profile: Profile, profile_idc: u8, iop: ProfileIopPattern) -> Self {
            Self {
                profile,
                profile_idc,
                iop,
            }
        }

        const fn exact(profile: Profile, profile_idc: u8, value: u8) -> Self {
            Self {
                profile,
                profile_idc,
                iop: ProfileIopPattern::exact(value),
            }
        }

        const fn matches(self, profile_idc: u8, profile_iop: u8) -> bool {
            self.profile_idc == profile_idc && self.iop.matches(profile_iop)
        }
    }

    /// masked `profile-iop` matcher for profile-equivalence wildcard bits
    ///
    /// `mask` selects bits that must equal `value`. unmasked bits are RFC
    /// wildcards such as the `x` bits in `x1xx0000`
    #[derive(Clone, Copy)]
    struct ProfileIopPattern {
        mask: u8,
        value: u8,
    }

    impl ProfileIopPattern {
        /// starts with the low four bits fixed to zero and high bits wildcarded
        const fn wildcarded() -> Self {
            Self {
                mask: IOP_PROFILE_PATTERN_LOW_ZERO_MASK,
                value: IOP_NONE,
            }
        }

        const fn exact(value: u8) -> Self {
            Self {
                mask: u8::MAX,
                value,
            }
        }

        const fn one(self, bit: u8) -> Self {
            Self {
                mask: self.mask | bit,
                value: self.value | bit,
            }
        }

        const fn zero(self, bit: u8) -> Self {
            Self {
                mask: self.mask | bit,
                value: self.value & !bit,
            }
        }

        const fn matches(self, value: u8) -> bool {
            value & self.mask == self.value
        }
    }

    // rfc 6184 lists equivalent sub-profile encodings rather than one canonical byte
    // example: `42 x1xx0000`, `4d 1xxx0000` and `58 11xx0000` all mean
    // constrained baseline, so `x` bits must be ignored during matching
    const PROFILE_PATTERNS: &[ProfilePattern] = &[
        ProfilePattern::masked(
            Profile::ConstrainedBaseline,
            BASELINE_IDC,
            ProfileIopPattern::wildcarded().one(IOP_CONSTRAINT_SET1),
        ),
        ProfilePattern::masked(
            Profile::ConstrainedBaseline,
            MAIN_IDC,
            ProfileIopPattern::wildcarded().one(IOP_CONSTRAINT_SET0),
        ),
        ProfilePattern::masked(
            Profile::ConstrainedBaseline,
            EXTENDED_IDC,
            ProfileIopPattern::wildcarded()
                .one(IOP_CONSTRAINT_SET0)
                .one(IOP_CONSTRAINT_SET1),
        ),
        ProfilePattern::masked(
            Profile::Baseline,
            BASELINE_IDC,
            ProfileIopPattern::wildcarded().zero(IOP_CONSTRAINT_SET1),
        ),
        ProfilePattern::masked(
            Profile::Baseline,
            EXTENDED_IDC,
            ProfileIopPattern::wildcarded()
                .one(IOP_CONSTRAINT_SET0)
                .zero(IOP_CONSTRAINT_SET1),
        ),
        ProfilePattern::masked(
            Profile::Main,
            MAIN_IDC,
            ProfileIopPattern::wildcarded()
                .zero(IOP_CONSTRAINT_SET0)
                .zero(IOP_CONSTRAINT_SET2),
        ),
        ProfilePattern::masked(
            Profile::Extended,
            EXTENDED_IDC,
            ProfileIopPattern::wildcarded()
                .zero(IOP_CONSTRAINT_SET0)
                .zero(IOP_CONSTRAINT_SET1),
        ),
        ProfilePattern::exact(Profile::High, HIGH_IDC, IOP_NONE),
        ProfilePattern::exact(Profile::High10, HIGH_10_IDC, IOP_NONE),
        ProfilePattern::exact(Profile::High422, HIGH_422_IDC, IOP_NONE),
        ProfilePattern::exact(Profile::High444Predictive, HIGH_444_IDC, IOP_NONE),
        ProfilePattern::exact(Profile::High10Intra, HIGH_10_IDC, IOP_CONSTRAINT_SET3),
        ProfilePattern::exact(Profile::High422Intra, HIGH_422_IDC, IOP_CONSTRAINT_SET3),
        ProfilePattern::exact(Profile::High444Intra, HIGH_444_IDC, IOP_CONSTRAINT_SET3),
        ProfilePattern::exact(Profile::Cavlc444Intra, CAVLC_444_IDC, IOP_CONSTRAINT_SET3),
    ];

    const fn profile_level_id_bytes(profile: Profile, level: LevelIdc) -> (u8, u8, u8) {
        let (profile_idc, profile_iop) = profile_bytes(profile);
        match level {
            LevelIdc::Level1B => level_1b_profile_level_id_bytes(profile_idc, profile_iop),
            _ => (profile_idc, profile_iop, level_idc_value(level)),
        }
    }

    const fn profile_bytes(profile: Profile) -> (u8, u8) {
        match profile {
            Profile::Baseline => (BASELINE_IDC, IOP_NONE),
            Profile::ConstrainedBaseline => (BASELINE_IDC, IOP_CONSTRAINED_BASELINE),
            Profile::Main => (MAIN_IDC, IOP_NONE),
            Profile::Extended => (EXTENDED_IDC, IOP_NONE),
            Profile::High => (HIGH_IDC, IOP_NONE),
            Profile::High10 => (HIGH_10_IDC, IOP_NONE),
            Profile::High422 => (HIGH_422_IDC, IOP_NONE),
            Profile::High444Predictive => (HIGH_444_IDC, IOP_NONE),
            Profile::High10Intra => (HIGH_10_IDC, IOP_CONSTRAINT_SET3),
            Profile::High422Intra => (HIGH_422_IDC, IOP_CONSTRAINT_SET3),
            Profile::High444Intra => (HIGH_444_IDC, IOP_CONSTRAINT_SET3),
            Profile::Cavlc444Intra => (CAVLC_444_IDC, IOP_CONSTRAINT_SET3),
        }
    }

    // level 1b has no single `level_idc` byte
    // baseline, main and extended borrow level 1.1's `level_idc=11` and set
    // constraint_set3_flag, while other profiles use `level_idc=9`
    const fn level_1b_profile_level_id_bytes(profile_idc: u8, profile_iop: u8) -> (u8, u8, u8) {
        match profile_idc {
            BASELINE_IDC | MAIN_IDC | EXTENDED_IDC => (
                profile_idc,
                profile_iop | IOP_CONSTRAINT_SET3,
                LEVEL_1_1_IDC,
            ),
            _ => (profile_idc, profile_iop, LEVEL_1B_OTHER_IDC),
        }
    }

    const fn level_idc_value(level: LevelIdc) -> u8 {
        match level {
            LevelIdc::Level1 => 10,
            LevelIdc::Level1B => LEVEL_1B_OTHER_IDC,
            LevelIdc::Level1_1 => 11,
            LevelIdc::Level1_2 => 12,
            LevelIdc::Level1_3 => 13,
            LevelIdc::Level2 => 20,
            LevelIdc::Level2_1 => 21,
            LevelIdc::Level2_2 => 22,
            LevelIdc::Level3 => 30,
            LevelIdc::Level3_1 => 31,
            LevelIdc::Level3_2 => 32,
            LevelIdc::Level4 => 40,
            LevelIdc::Level4_1 => 41,
            LevelIdc::Level4_2 => 42,
            LevelIdc::Level5 => 50,
            LevelIdc::Level5_1 => 51,
            LevelIdc::Level5_2 => 52,
        }
    }

    #[expect(clippy::as_conversions, reason = "u8 to u32 widening is lossless")]
    const fn pack_profile_level_id(profile_idc: u8, profile_iop: u8, level_idc: u8) -> u32 {
        ((profile_idc as u32) << 16) | ((profile_iop as u32) << 8) | level_idc as u32
    }

    fn normalized_level_idc(profile_idc: u8, profile_iop: u8, level_idc: u8) -> Option<LevelIdc> {
        // reject the non-canonical level 1b form for baseline, main and extended
        // those profile families must use `level_idc=11` plus constraint_set3_flag
        if LEVEL_1B_IDCS.contains(&profile_idc) {
            if level_idc == LEVEL_1B_OTHER_IDC {
                return None;
            }
            if level_idc == LEVEL_1_1_IDC {
                return if (profile_iop & IOP_CONSTRAINT_SET3) != 0 {
                    Some(LevelIdc::Level1B)
                } else {
                    Some(LevelIdc::Level1_1)
                };
            }
        }
        LevelIdc::try_from(level_idc).ok()
    }

    fn profile_from_bytes(profile_idc: u8, profile_iop: u8) -> Option<Profile> {
        PROFILE_PATTERNS.iter().copied().find_map(|pattern| {
            pattern
                .matches(profile_idc, profile_iop)
                .then_some(pattern.profile)
        })
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

/// Returns `true` if `payload_type` is an RTP/AVP dynamic payload type.
///
/// Use this for the `PT` field from the RTP fixed header. It does not validate
/// whether a signaling layer actually negotiated that payload type.
///
/// Reference: RFC 3551 section 6.
#[must_use]
pub const fn is_dynamic_payload_type(payload_type: u8) -> bool {
    payload_type >= RTP_DYNAMIC_PAYLOAD_TYPE_START && payload_type <= RTP_DYNAMIC_PAYLOAD_TYPE_END
}

/// Returns `true` if `payload_type` fits in the RTP fixed-header PT field.
///
/// Reference: RFC 3550 section 5.1.
#[must_use]
pub const fn is_payload_type(payload_type: u8) -> bool {
    payload_type <= RTP_PAYLOAD_TYPE_MAX
}

/// Returns `true` if `payload_type` can be used with RTP/RTCP mux.
///
/// Reference: RFC 5761 section 4.
#[must_use]
pub const fn is_rtcp_mux_payload_type(payload_type: u8) -> bool {
    is_payload_type(payload_type)
        && (payload_type < RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_START
            || payload_type > RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_END)
}

/// rtcp common-header `PT` value namespace
///
/// reference: RFC 3550 section 12.1 and RFC 4585 section 6.1
pub mod rtcp_packet_type {
    /// sender report packet
    pub const SR: u8 = 200;
    /// receiver report packet
    pub const RR: u8 = 201;
    /// source description packet
    pub const SDES: u8 = 202;
    /// goodbye packet
    pub const BYE: u8 = 203;
    /// application-defined packet
    pub const APP: u8 = 204;

    /// transport-layer feedback packet
    pub const RTPFB: u8 = 205;
    /// payload-specific feedback packet
    pub const PSFB: u8 = 206;
}

/// rtcp SDES item `type` value namespace
///
/// reference: RFC 3550 section 12.2 and RFC 8852 section 4
pub mod rtcp_sdes_item {
    /// canonical end-point identifier
    pub const CNAME: u8 = 1;
    /// user name
    pub const NAME: u8 = 2;
    /// email address
    pub const EMAIL: u8 = 3;
    /// phone number
    pub const PHONE: u8 = 4;
    /// geographic location
    pub const LOC: u8 = 5;
    /// application or tool name
    pub const TOOL: u8 = 6;
    /// transient note
    pub const NOTE: u8 = 7;
    /// private extension item
    pub const PRIV: u8 = 8;

    /// RTP stream identifier carried in an SDES packet
    pub const RTP_STREAM_ID: u8 = 12;
    /// RTP stream repaired by a redundancy stream
    pub const REPAIRED_RTP_STREAM_ID: u8 = 13;
}

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
///
/// RTP packets carry an extension block only when the fixed-header `X` bit is
/// set. The 16-bit profile ID then decides whether extension elements use the
/// one-byte or two-byte element header shape.
///
/// ```text
/// RTP extension block
///
/// +----------------+----------------+-----------------------------+
/// | profile ID     | length in u32  | extension elements          |
/// +----------------+----------------+-----------------------------+
/// | 16 bits        | 16 bits        | padded to 32-bit boundary   |
/// +----------------+----------------+-----------------------------+
///
/// One-byte element, profile 0xBEDE
///
/// +---------+---------+----------------------+
/// | ID      | len-1   | data                 |
/// +---------+---------+----------------------+
/// | 4 bits  | 4 bits  | 1 to 16 bytes        |
/// +---------+---------+----------------------+
///
/// Two-byte element, profile 0x1000 through 0x100F
///
/// +---------+---------+----------------------+
/// | ID      | len     | data                 |
/// +---------+---------+----------------------+
/// | 8 bits  | 8 bits  | 0 to 255 bytes       |
/// +---------+---------+----------------------+
/// ```
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

    /// Returns whether `id` is usable as an RFC 8285 one-byte element ID.
    ///
    /// ID 0 is padding and ID 15 is reserved, so only 1 through 14 can identify
    /// negotiated extension values.
    #[must_use]
    pub const fn is_one_byte_id(id: u8) -> bool {
        id >= ONE_BYTE_ID_MIN && id <= ONE_BYTE_ID_MAX
    }

    /// Builds an RFC 8285 two-byte profile ID from the 4-bit appbits value.
    ///
    /// Returns `None` when `appbits` cannot fit in the low nibble of the
    /// profile ID.
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
/// Frame marking is carried as the value of one negotiated RTP header
/// extension. Packet gates use it as packet-local metadata for temporal-layer
/// selection. The first octet is enough for the current route-control decision.
///
/// ```text
/// Short form, non-scalable stream
///
/// +---+---+---+---+---------------+
/// | S | E | I | D | reserved      |
/// +---+---+---+---+---------------+
///
/// Long form, scalable stream
///
/// +---+---+---+---+---+-----------+---------------+---------------+
/// | S | E | I | D | B | TID       | LID           | TL0PICIDX     |
/// +---+---+---+---+---+-----------+---------------+---------------+
/// | first octet                    | optional      | optional      |
/// +---+---+---+---+---+-----------+---------------+---------------+
/// ```
///
/// Reference: RFC 9626 section 3.
pub mod frame_marking {
    /// Full long-form frame-marking payload length.
    pub const LONG_DATA_LEN_WITH_TL0PICIDX: u8 = 3;

    /// Long-form payload length when `TL0PICIDX` is omitted.
    pub const LONG_DATA_LEN_WITHOUT_TL0PICIDX: u8 = 2;

    /// Long-form payload length when both `LID` and `TL0PICIDX` are omitted.
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

    /// Extracts the temporal-layer ID from the first long-form frame-marking
    /// octet.
    ///
    /// Callers must only use this value as temporal metadata when signaling or
    /// negotiated extension state proves the packet carries frame marking.
    #[must_use]
    pub const fn temporal_layer_id(first_octet: u8) -> u8 {
        first_octet & TEMPORAL_LAYER_ID_MASK
    }

    /// Returns whether `value` fits in the RFC 9626 three-bit temporal-layer
    /// field.
    #[must_use]
    pub const fn is_valid_temporal_layer_id(value: u8) -> bool {
        value <= TEMPORAL_LAYER_ID_MAX
    }
}

#[cfg(test)]
#[path = "TESTS/rtp.rs"]
mod tests;
