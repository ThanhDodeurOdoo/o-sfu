//! RFC references covered:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>
//! - RTCP feedback profile: <https://www.rfc-editor.org/rfc/rfc4585>
//! - RTP stream identifier SDES items: <https://www.rfc-editor.org/rfc/rfc8852>
//! - Video frame marking RTP header extension: <https://www.rfc-editor.org/rfc/rfc9626>
//! - Layer Refresh Request feedback: <https://www.rfc-editor.org/rfc/rfc9627>
//! - RTP payload format for VP8: <https://www.rfc-editor.org/rfc/rfc7741>
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
/// carried. o-sfu rewrites only the descriptor bytes that already exist so a
/// downstream browser receives one continuous stream after source switches.
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

    const SHORT_PICTURE_ID_MASK: u16 = 0x7f;

    /// Value mask for the 15-bit VP8 long `PictureID` field.
    pub const LONG_PICTURE_ID_MASK: u16 = 0x7fff;

    /// Value mask for the two-bit VP8 temporal-layer identity.
    pub const TEMPORAL_LAYER_ID_MASK: u8 = 0b0000_0011;

    /// Offset-carrying view of the VP8 descriptor fields that o-sfu may need
    /// to rewrite before forwarding a packet to a browser consumer.
    ///
    /// The value borrows no packet data, but the stored offsets are only valid
    /// for the payload slice that was passed to [`payload_descriptor`] or for a
    /// byte-identical clone of that slice. Use it as a short-lived parse result
    /// on the packet forwarding path.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PayloadDescriptor {
        picture_id: Option<PictureId>,
        tl0_pic_idx: Option<Tl0PicIdx>,
    }

    /// Replacement values for VP8 descriptor fields that already exist in the
    /// source packet.
    ///
    /// The rewrite step does not add missing descriptor fields and does not
    /// change the descriptor width. A short `PictureID` keeps its one-byte shape
    /// and stores the low 7 bits of the requested value. A long `PictureID` keeps
    /// its two-byte shape and stores the low 15 bits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PayloadDescriptorRewrite {
        /// Receiver-local `PictureID` value to write when the packet carried a
        /// `PictureID` field.
        pub picture_id: Option<u16>,
        /// Receiver-local `TL0PICIDX` value to write when the packet carried a
        /// `TL0PICIDX` field.
        pub tl0_pic_idx: Option<u8>,
    }

    /// Parsed VP8 `PictureID` with the byte offset needed for in-place rewrite.
    ///
    /// Callers should use [`PayloadDescriptor::picture_id`] instead of relying
    /// on this internal layout. Its fields stay private to keep offset handling
    /// inside this module.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PictureId {
        value: u16,
        encoding: PictureIdEncoding,
        offset: usize,
    }

    /// Parsed VP8 `TL0PICIDX` with the byte offset needed for in-place rewrite.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Tl0PicIdx {
        value: u8,
        offset: usize,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PictureIdEncoding {
        Short,
        Long,
    }

    impl PayloadDescriptor {
        /// Returns the VP8 `PictureID` value carried by the packet descriptor.
        ///
        /// `None` means the payload descriptor did not advertise a `PictureID`.
        /// It does not mean the packet is invalid because `PictureID` is optional
        /// in RFC 7741.
        #[must_use]
        pub const fn picture_id(self) -> Option<u16> {
            match self.picture_id {
                Some(picture_id) => Some(picture_id.value),
                None => None,
            }
        }

        /// Returns the VP8 `TL0PICIDX` value carried by the packet descriptor.
        ///
        /// `TL0PICIDX` is present only when the descriptor extension sets `L=1`.
        /// The forwarding path uses this value with `PictureID` to preserve
        /// temporal-layer continuity after source switches.
        #[must_use]
        pub const fn tl0_pic_idx(self) -> Option<u8> {
            match self.tl0_pic_idx {
                Some(tl0_pic_idx) => Some(tl0_pic_idx.value),
                None => None,
            }
        }
    }

    /// Detects the first RTP packet of a VP8 keyframe.
    ///
    /// RFC 7741 section 4.2 defines the VP8 payload descriptor. A decodable
    /// keyframe starts at partition 0 (`S=1`, `PartID=0`) and the VP8 payload
    /// header starts with `P=0`. The input must be the RTP codec payload. It is
    /// not a complete RTP packet.
    ///
    /// Truncated descriptors, missing payload headers and non-start partitions
    /// return `false`. The helper intentionally performs only the cheap
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

    /// Parses the RFC 7741 VP8 payload descriptor fields that must stay
    /// continuous when an SFU rewrites multiple publisher SSRCs into one
    /// downstream browser stream.
    ///
    /// The input must start at the VP8 descriptor byte. `Some` means the
    /// advertised descriptor shape was internally consistent enough to locate
    /// the optional `PictureID` and `TL0PICIDX` fields. `None` means the packet was
    /// truncated inside the advertised descriptor and must not be rewritten.
    ///
    /// The returned descriptor is an offset map for this exact payload shape.
    /// Keep it with the packet that produced it and pass it to
    /// [`rewrite_payload_descriptor`] before the payload bytes are handed to the
    /// downstream sender.
    #[must_use]
    pub fn payload_descriptor(payload: &[u8]) -> Option<PayloadDescriptor> {
        let (&descriptor, mut rest) = payload.split_first()?;
        if descriptor & X_BIT == 0 {
            return Some(PayloadDescriptor {
                picture_id: None,
                tl0_pic_idx: None,
            });
        }
        let (&extension, remaining) = rest.split_first()?;
        rest = remaining;
        let mut offset = 2;
        let picture_id = if extension & I_BIT != 0 {
            let parsed = parse_picture_id(rest, offset)?;
            offset += parsed.encoding.len();
            rest = payload.get(offset..)?;
            Some(parsed)
        } else {
            None
        };
        let tl0_pic_idx = if extension & L_BIT != 0 {
            let value = *rest.first()?;
            let parsed = Tl0PicIdx { value, offset };
            offset += 1;
            rest = payload.get(offset..)?;
            Some(parsed)
        } else {
            None
        };
        if extension & (T_BIT | K_BIT) != 0 {
            rest.first()?;
        }
        Some(PayloadDescriptor {
            picture_id,
            tl0_pic_idx,
        })
    }

    /// Rewrites existing VP8 descriptor identity fields in place.
    ///
    /// This is the mutation half of [`payload_descriptor`]. It is used after the
    /// forwarding adapter has selected receiver-local RTP identity, while the
    /// payload buffer is still owned by the local send path. Missing rewrite
    /// values and absent source fields are ignored. The function never grows the
    /// payload, never converts short `PictureID` to long `PictureID` and never
    /// panics if the caller hands it a shorter slice than the one that produced
    /// the descriptor.
    pub fn rewrite_payload_descriptor(
        payload: &mut [u8],
        descriptor: PayloadDescriptor,
        rewrite: PayloadDescriptorRewrite,
    ) {
        if let (Some(picture_id), Some(value)) = (descriptor.picture_id, rewrite.picture_id) {
            rewrite_picture_id(payload, picture_id, value);
        }
        if let (Some(tl0_pic_idx), Some(value)) = (descriptor.tl0_pic_idx, rewrite.tl0_pic_idx)
            && let Some(byte) = payload.get_mut(tl0_pic_idx.offset)
        {
            *byte = value;
        }
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

    /// Parses the `PictureID` field and remembers where it starts in the payload
    /// descriptor.
    fn parse_picture_id(payload: &[u8], offset: usize) -> Option<PictureId> {
        let (&first, remaining) = payload.split_first()?;
        if first & LONG_PICTURE_ID_BIT == 0 {
            return Some(PictureId {
                value: u16::from(first) & SHORT_PICTURE_ID_MASK,
                encoding: PictureIdEncoding::Short,
                offset,
            });
        }
        let second = *remaining.first()?;
        Some(PictureId {
            value: (u16::from(first & !LONG_PICTURE_ID_BIT) << 8) | u16::from(second),
            encoding: PictureIdEncoding::Long,
            offset,
        })
    }

    /// Writes a receiver-local `PictureID` while preserving the source field
    /// width.
    fn rewrite_picture_id(payload: &mut [u8], picture_id: PictureId, value: u16) {
        match picture_id.encoding {
            PictureIdEncoding::Short => {
                if let Some(byte) = payload.get_mut(picture_id.offset)
                    && let Ok(value) = u8::try_from(value & SHORT_PICTURE_ID_MASK)
                {
                    *byte = value;
                }
            }
            PictureIdEncoding::Long => {
                let value = value & LONG_PICTURE_ID_MASK;
                if let Some(bytes) = payload.get_mut(picture_id.offset..picture_id.offset + 2) {
                    let Some((first, rest)) = bytes.split_first_mut() else {
                        return;
                    };
                    let Some(second) = rest.first_mut() else {
                        return;
                    };
                    let (Ok(high), Ok(low)) =
                        (u8::try_from(value >> 8), u8::try_from(value & 0xff))
                    else {
                        return;
                    };
                    *first = LONG_PICTURE_ID_BIT | high;
                    *second = low;
                }
            }
        }
    }

    impl PictureIdEncoding {
        const fn len(self) -> usize {
            match self {
                Self::Short => 1,
                Self::Long => 2,
            }
        }
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
pub mod h264 {
    use std::cmp::Ordering;

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
    pub fn payload_starts_idr(payload: &[u8]) -> bool {
        let Some((&nal_header, rest)) = payload.split_first() else {
            return false;
        };
        match nal_header & NAL_UNIT_TYPE_MASK {
            NAL_UNIT_TYPE_IDR => true,
            NAL_UNIT_TYPE_STAP_A => stap_a_contains_idr(rest),
            NAL_UNIT_TYPE_FU_A => fu_a_starts_idr(rest),
            _ => false,
        }
    }

    /// Scans a STAP-A payload for any contained IDR NAL unit.
    ///
    /// Each aggregate entry is length-prefixed. A malformed length makes the
    /// whole probe fail closed because the caller cannot safely trust later
    /// bytes as NAL boundaries.
    fn stap_a_contains_idr(mut payload: &[u8]) -> bool {
        while payload.len() >= 2 {
            let Some((&first_len_octet, rest)) = payload.split_first() else {
                return false;
            };
            let Some((&second_len_octet, rest)) = rest.split_first() else {
                return false;
            };
            let nal_len = usize::from(u16::from_be_bytes([first_len_octet, second_len_octet]));
            if nal_len == 0 || rest.len() < nal_len {
                return false;
            }
            let Some((&nal_header, remaining_payload)) = rest.split_first() else {
                return false;
            };
            if nal_header & NAL_UNIT_TYPE_MASK == NAL_UNIT_TYPE_IDR {
                return true;
            }
            payload = remaining_payload.get(nal_len - 1..).unwrap_or_default();
        }
        false
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

    const H264_LEVEL_1B_PROFILE_IDCS: [u8; 3] = [0x42, 0x4D, 0x58];
    const H264_LEVEL_1B_OTHER_PROFILE_LEVEL_IDC: u8 = 9;
    const H264_LEVEL_1_1_LEVEL_IDC: u8 = 11;
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
        if H264_LEVEL_1B_PROFILE_IDCS.contains(&profile_idc) {
            if level_idc == H264_LEVEL_1B_OTHER_PROFILE_LEVEL_IDC {
                return None;
            }
            if level_idc == H264_LEVEL_1_1_LEVEL_IDC {
                return if (profile_iop & H264_LEVEL_1B_CONSTRAINT_SET3_FLAG) != 0 {
                    Some(LevelIdc::Level1B)
                } else {
                    Some(LevelIdc::Level1_1)
                };
            }
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
mod tests {
    use super::{
        fmtp, frame_marking,
        h264::{self, LevelIdc, Profile, ProfileLevelId},
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
        assert!(h264::payload_starts_idr(&[0x65, 0x88]));
        assert!(h264::payload_starts_idr(&[
            0x78, 0x00, 0x02, 0x67, 0x42, 0x00, 0x02, 0x65, 0x88
        ]));
        assert!(h264::payload_starts_idr(&[0x7c, 0x85, 0x88]));
        assert!(!h264::payload_starts_idr(&[0x7c, 0xc5, 0x88]));
        assert!(!h264::payload_starts_idr(&[0x41, 0x9a]));
        assert!(!h264::payload_starts_idr(&[0x7c, 0x05, 0x88]));
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
}
