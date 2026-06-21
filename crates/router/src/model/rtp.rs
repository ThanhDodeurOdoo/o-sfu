//! Router-native RTP model for the transport edge.
//!
//! This module defines the typed domain model used to describe media streams,
//! codecs, and their negotiated properties. It sits between the signaling/SDP
//! layer and the raw packet loop, allowing the router to reason about media
//! without parsing raw bytes or string-heavy protocol bags.
//!
//! ### RTP Packet Context
//!
//! Most of the types defined here map directly to fields in the "RFC 3550"
//! RTP header or its extensions:
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |V=2|P|X|  CC   |M|     PT      |       Sequence Number         |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |                           Timestamp                           |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |           Synchronization Source (SSRC) identifier            |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! ```
//!
//! - `V`: version (always 2)
//! - `P`: padding bit
//! - `X`: extension bit (if set, an extension header follows the SSRC)
//! - `CC`: "CSRC count", the number of contributing source identifiers (0-15) that follow the SSRC
//! - `M`: "Marker" bit, used by profiles to mark significant events like the end of a video frame
//! - `PT`: "Payload Type" (identifies the codec)
//! - `SSRC`: unique stream identifier
//!
//! RFC references for this module:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>

use std::borrow::Cow;

use o_sfu_rfc::{rtp as rfc_rtp, webrtc as rfc_webrtc};
pub use rfc_rtp::PayloadType;

use super::MediaKind;

/// Canonical name for a media codec (e.g. "opus", "vp8", "h264")
pub type MediaCodec = rfc_rtp::CodecName;

/// Uniform resource identifier for a header extension (e.g. "urn:ietf:params:rtp-hdrext:ssrc-audio-level")
pub type HeaderExtensionUri = rfc_webrtc::RtpHeaderExtensionUri;

/// Categories of feedback messages sent over RTCP to control stream behavior.
///
/// These define how the receiver reports issues (like packet loss) or requests
/// changes (like a new keyframe) to the sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtcpFeedbackKind {
    /// Generic negative acknowledgment (RFC 4585)
    Nack,
    /// Picture loss indication (RFC 4585), used to request a full keyframe
    NackPli,
    /// Full intra request (RFC 5104), a more forceful keyframe request
    CcmFir,
    /// Google-specific receiver estimated maximum bitrate
    GoogRemb,
    /// Transport-wide congestion control (draft-holmer-rmcat-transport-wide-cc-extensions)
    TransportCc,
    /// Any other vendor-specific or experimental feedback type
    Other(String),
}

/// One negotiated RTCP feedback mechanism for a codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtcpFeedback {
    kind: RtcpFeedbackKind,
    parameter: Option<String>,
}

impl RtcpFeedback {
    #[must_use]
    pub fn new(kind: RtcpFeedbackKind, parameter: Option<String>) -> Self {
        Self { kind, parameter }
    }

    #[must_use]
    pub fn kind(&self) -> &RtcpFeedbackKind {
        &self.kind
    }

    #[must_use]
    pub fn parameter(&self) -> Option<&str> {
        self.parameter.as_deref()
    }
}

/// Synchronization source identifier for a media stream (RFC 3550).
///
/// Every distinct stream of packets (e.g. one audio track, one camera layer)
/// is assigned a random 32-bit SSRC. This allows multiple streams to be
/// multiplexed over a single transport (e.g. one UDP port).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ssrc(u32);

impl Ssrc {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl From<u32> for Ssrc {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<Ssrc> for u32 {
    fn from(value: Ssrc) -> Self {
        value.value()
    }
}

/// Restriction identifier (RFC 8853).
///
/// Used in "Simulcast" to label different encodings of the same source (e.g.
/// "low" and "high" resolution). Unlike SSRC which is a random number that
/// can change if a collision occurs, RID is a stable string label.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rid(String);

impl Rid {
    #[must_use]
    pub fn try_new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        rfc_webrtc::sdp::rid::is_id(value.as_str()).then_some(Self(value))
    }

    /// Builds a RID after applying the RFC 8852 stream-id grammar.
    ///
    /// # Panics
    ///
    /// Panics when `value` is empty, too long or contains a non-alphanumeric
    /// byte.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        assert!(rfc_webrtc::sdp::rid::is_id(value.as_str()));
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Rid {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Rid {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Media identification (RFC 8843).
///
/// Ties an RTP stream to a specific "m=" section in the SDP. This is
/// critical for "BUNDLE" where multiple media sections share one transport,
/// as it provides a stable way to route packets to the correct logical track.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mid(String);

impl Mid {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Mid {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Mid {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Local 4-bit identifier for a header extension (RFC 8285).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeaderExtensionId(u8);

impl HeaderExtensionId {
    #[must_use]
    pub const fn try_new(value: u8) -> Option<Self> {
        if rfc_rtp::header_extension::is_one_byte_id(value) {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Builds a one-byte RTP header-extension id.
    ///
    /// # Panics
    ///
    /// Panics when `value` is padding, reserved or outside the RFC 8285
    /// one-byte element id range.
    #[must_use]
    pub const fn new(value: u8) -> Self {
        assert!(rfc_rtp::header_extension::is_one_byte_id(value));
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl From<u8> for HeaderExtensionId {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl From<HeaderExtensionId> for u8 {
    fn from(value: HeaderExtensionId) -> Self {
        value.value()
    }
}

/// Typed codec parameter that affects interoperability.
///
/// These correspond to "a=fmtp" parameters in SDP. Mismatched settings
/// here usually mean the receiver cannot decode the sender's bitstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecSetting {
    /// Associated payload type for RTX (RFC 4588)
    RtxAssociation(PayloadType),
    /// h264-specific packetization mode
    H264PacketizationMode(rfc_rtp::h264::PacketizationMode),
    /// H264 profile and level (e.g. "42e01f" for Constrained Baseline Level 3.1)
    H264ProfileLevelId(String),
    /// VP9-specific profile identifier
    Vp9ProfileId(u8),
    /// OPUS-specific flag for in-band forward error correction
    UseInBandFec(bool),
    /// Generic catch-all for unknown or vendor parameters
    Other { key: String, value: String },
}

impl CodecSetting {
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::RtxAssociation(_) => rfc_rtp::fmtp::RTX_ASSOCIATION,
            Self::H264PacketizationMode(_) => rfc_rtp::fmtp::H264_PACKETIZATION_MODE,
            Self::H264ProfileLevelId(_) => rfc_rtp::fmtp::H264_PROFILE_LEVEL_ID,
            Self::Vp9ProfileId(_) => rfc_rtp::fmtp::VP9_PROFILE_ID,
            Self::UseInBandFec(_) => rfc_rtp::fmtp::OPUS_USE_IN_BAND_FEC,
            Self::Other { key, .. } => key.as_str(),
        }
    }

    #[must_use]
    pub fn wire_value(&self) -> Cow<'_, str> {
        match self {
            Self::RtxAssociation(payload_type) => Cow::Owned(payload_type.value().to_string()),
            Self::H264PacketizationMode(mode) => Cow::Owned(mode.fmtp_value().to_string()),
            Self::H264ProfileLevelId(profile_level_id) => Cow::Borrowed(profile_level_id.as_str()),
            Self::Vp9ProfileId(profile_id) => Cow::Owned(profile_id.to_string()),
            Self::UseInBandFec(enabled) => Cow::Borrowed(if *enabled {
                rfc_rtp::fmtp::VALUE_ENABLED
            } else {
                rfc_rtp::fmtp::VALUE_DISABLED
            }),
            Self::Other { value, .. } => Cow::Borrowed(value.as_str()),
        }
    }
}

/// RTP header-extension configuration (RFC 8285).
///
/// Allows carrying extra metadata (like bandwidth estimation hints or
/// audio levels) in a standard way within the RTP packet header.
///
/// ```text
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |      defined by profile       |           length              |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ID   |  len  |     data...                                   |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderExtension {
    uri: HeaderExtensionUri,
    id: HeaderExtensionId,
    encrypt: bool,
}

impl HeaderExtension {
    #[must_use]
    pub fn new(uri: impl Into<HeaderExtensionUri>, id: impl Into<HeaderExtensionId>) -> Self {
        Self {
            uri: uri.into(),
            id: id.into(),
            encrypt: false,
        }
    }

    #[must_use]
    pub fn with_encryption(mut self, encrypt: bool) -> Self {
        self.encrypt = encrypt;
        self
    }

    #[must_use]
    pub fn uri_kind(&self) -> &HeaderExtensionUri {
        &self.uri
    }

    #[must_use]
    pub fn id(&self) -> HeaderExtensionId {
        self.id
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }

    #[must_use]
    pub fn encrypt(&self) -> bool {
        self.encrypt
    }
}

/// Codec capability advertised by a router or endpoint.
///
/// Represents one possible way an endpoint can encode or decode media.
/// The `payload_type` is optional here because capabilities are often
/// just templates (e.g. "i support VP8") before a concrete session pins
/// a specific PT number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaCodecCapability {
    media_kind: MediaKind,
    codec: MediaCodec,
    clock_rate: u32,
    payload_type: Option<PayloadType>,
    channels: Option<u16>,
    settings: Vec<CodecSetting>,
    rtcp_feedback: Vec<RtcpFeedback>,
}

impl MediaCodecCapability {
    #[must_use]
    pub fn new(media_kind: MediaKind, codec: impl Into<MediaCodec>, clock_rate: u32) -> Self {
        Self {
            media_kind,
            codec: codec.into(),
            clock_rate,
            payload_type: None,
            channels: None,
            settings: Vec::new(),
            rtcp_feedback: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_payload_type(mut self, payload_type: PayloadType) -> Self {
        self.payload_type = Some(payload_type);
        self
    }

    #[must_use]
    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }

    #[must_use]
    pub fn with_setting(mut self, setting: CodecSetting) -> Self {
        self.settings.push(setting);
        self
    }

    #[must_use]
    pub fn with_parameter(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_setting(codec_setting_from_wire(name.into(), value.into()))
    }

    #[must_use]
    pub fn with_rtcp_feedback(mut self, feedback: RtcpFeedback) -> Self {
        self.rtcp_feedback.push(feedback);
        self
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub fn codec(&self) -> &MediaCodec {
        &self.codec
    }

    #[must_use]
    pub fn codec_name(&self) -> &str {
        self.codec.as_str()
    }

    #[must_use]
    pub fn clock_rate(&self) -> u32 {
        self.clock_rate
    }

    #[must_use]
    pub fn payload_type_id(&self) -> Option<PayloadType> {
        self.payload_type
    }

    #[must_use]
    pub fn payload_type(&self) -> Option<u8> {
        self.payload_type.map(PayloadType::value)
    }

    #[must_use]
    pub fn channels(&self) -> Option<u16> {
        self.channels
    }

    pub fn settings(&self) -> impl Iterator<Item = &CodecSetting> {
        self.settings.iter()
    }

    pub fn parameters(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.settings
            .iter()
            .map(|setting| (setting.key().to_owned(), setting.wire_value().into_owned()))
    }

    pub fn rtcp_feedback(&self) -> impl Iterator<Item = &RtcpFeedback> {
        self.rtcp_feedback.iter()
    }

    #[must_use]
    pub fn rtx_associated_payload_type_id(&self) -> Option<PayloadType> {
        self.settings.iter().find_map(|setting| match setting {
            CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
            _ => None,
        })
    }

    #[must_use]
    pub fn rtx_associated_payload_type(&self) -> Option<u8> {
        self.rtx_associated_payload_type_id()
            .map(PayloadType::value)
    }
}

/// Full set of codec and extension capabilities for an RTP endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaCapabilities {
    codecs: Vec<MediaCodecCapability>,
    header_extensions: Vec<HeaderExtension>,
}

impl MediaCapabilities {
    #[must_use]
    pub fn new(codecs: Vec<MediaCodecCapability>, header_extensions: Vec<HeaderExtension>) -> Self {
        Self {
            codecs,
            header_extensions,
        }
    }

    pub fn codecs(&self) -> impl Iterator<Item = &MediaCodecCapability> {
        self.codecs.iter()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &HeaderExtension> {
        self.header_extensions.iter()
    }
}

/// Negotiated codec format for a concrete media stream.
///
/// Unlike a capability, a format has a fixed `payload_type` that matches the
/// actual value expected in the RTP packets on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaFormat {
    media_kind: MediaKind,
    codec: MediaCodec,
    payload_type: PayloadType,
    clock_rate: u32,
    channels: Option<u16>,
    settings: Vec<CodecSetting>,
    rtcp_feedback: Vec<RtcpFeedback>,
}

impl MediaFormat {
    #[must_use]
    pub fn new(
        media_kind: MediaKind,
        codec: impl Into<MediaCodec>,
        payload_type: PayloadType,
        clock_rate: u32,
    ) -> Self {
        Self {
            media_kind,
            codec: codec.into(),
            payload_type,
            clock_rate,
            channels: None,
            settings: Vec::new(),
            rtcp_feedback: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }

    #[must_use]
    pub fn with_setting(mut self, setting: CodecSetting) -> Self {
        self.settings.push(setting);
        self
    }

    #[must_use]
    pub fn with_parameter(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_setting(codec_setting_from_wire(name.into(), value.into()))
    }

    #[must_use]
    pub fn with_rtcp_feedback(mut self, feedback: RtcpFeedback) -> Self {
        self.rtcp_feedback.push(feedback);
        self
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub fn codec(&self) -> &MediaCodec {
        &self.codec
    }

    #[must_use]
    pub fn codec_name(&self) -> &str {
        self.codec.as_str()
    }

    #[must_use]
    pub fn payload_type_id(&self) -> PayloadType {
        self.payload_type
    }

    #[must_use]
    pub fn payload_type(&self) -> u8 {
        self.payload_type.value()
    }

    #[must_use]
    pub fn clock_rate(&self) -> u32 {
        self.clock_rate
    }

    #[must_use]
    pub fn channels(&self) -> Option<u16> {
        self.channels
    }

    pub fn settings(&self) -> impl Iterator<Item = &CodecSetting> {
        self.settings.iter()
    }

    pub fn parameters(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.settings
            .iter()
            .map(|setting| (setting.key().to_owned(), setting.wire_value().into_owned()))
    }

    pub fn rtcp_feedback(&self) -> impl Iterator<Item = &RtcpFeedback> {
        self.rtcp_feedback.iter()
    }

    #[must_use]
    pub fn rtx_associated_payload_type_id(&self) -> Option<PayloadType> {
        self.settings.iter().find_map(|setting| match setting {
            CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
            _ => None,
        })
    }

    #[must_use]
    pub fn rtx_associated_payload_type(&self) -> Option<u8> {
        self.rtx_associated_payload_type_id()
            .map(PayloadType::value)
    }
}

/// Routing bridge between a negotiated media format and a physical stream.
///
/// It ties the codec format to the network-level identifiers (SSRC or RID)
/// used to identify the stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamBinding {
    ssrc: Option<Ssrc>,
    rid: Option<Rid>,
    payload_type: Option<PayloadType>,
    max_bitrate: Option<u64>,
}

impl StreamBinding {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_ssrc(mut self, ssrc: impl Into<Ssrc>) -> Self {
        self.ssrc = Some(ssrc.into());
        self
    }

    #[must_use]
    pub fn with_rid(mut self, rid: impl Into<Rid>) -> Self {
        self.rid = Some(rid.into());
        self
    }

    #[must_use]
    pub fn with_payload_type(mut self, payload_type: PayloadType) -> Self {
        self.payload_type = Some(payload_type);
        self
    }

    #[must_use]
    pub fn with_max_bitrate(mut self, max_bitrate: u64) -> Self {
        self.max_bitrate = Some(max_bitrate);
        self
    }

    #[must_use]
    pub fn ssrc(&self) -> Option<u32> {
        self.ssrc.map(Ssrc::value)
    }

    #[must_use]
    pub fn rid(&self) -> Option<&str> {
        self.rid.as_ref().map(Rid::as_str)
    }

    #[must_use]
    pub(super) fn with_payload_type_mapping(
        mut self,
        payload_types: &[(PayloadType, PayloadType)],
    ) -> Self {
        if let Some(payload_type) = self.payload_type {
            let mapped_payload_type = payload_types
                .iter()
                .find_map(|(original, mapped)| (*original == payload_type).then_some(*mapped))
                .unwrap_or(payload_type);
            self.payload_type = Some(mapped_payload_type);
        }
        self
    }

    #[must_use]
    pub fn payload_type_id(&self) -> Option<PayloadType> {
        self.payload_type
    }

    #[must_use]
    pub fn payload_type(&self) -> Option<u8> {
        self.payload_type.map(PayloadType::value)
    }

    #[must_use]
    pub fn max_bitrate(&self) -> Option<u64> {
        self.max_bitrate
    }
}

/// Description of one logical media stream (e.g. "camera") at the router boundary.
///
/// Combines negotiated formats (codecs), extensions, and the stream bindings
/// that tell the router how to identify the packets on the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaStream {
    formats: Vec<MediaFormat>,
    header_extensions: Vec<HeaderExtension>,
    bindings: Vec<StreamBinding>,
    mid: Option<Mid>,
}

impl MediaStream {
    #[must_use]
    pub fn new(
        formats: Vec<MediaFormat>,
        header_extensions: Vec<HeaderExtension>,
        bindings: Vec<StreamBinding>,
    ) -> Self {
        Self {
            formats,
            header_extensions,
            bindings,
            mid: None,
        }
    }

    #[must_use]
    pub fn with_mid(mut self, mid: impl Into<Mid>) -> Self {
        self.mid = Some(mid.into());
        self
    }

    pub fn formats(&self) -> impl Iterator<Item = &MediaFormat> {
        self.formats.iter()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &HeaderExtension> {
        self.header_extensions.iter()
    }

    pub fn bindings(&self) -> impl Iterator<Item = &StreamBinding> {
        self.bindings.iter()
    }

    #[must_use]
    pub fn mid(&self) -> Option<&str> {
        self.mid.as_ref().map(Mid::as_str)
    }
}

fn codec_setting_from_wire(key: String, value: String) -> CodecSetting {
    match key.as_str() {
        rfc_rtp::fmtp::RTX_ASSOCIATION => value
            .parse::<u8>()
            .ok()
            .and_then(PayloadType::try_new)
            .map_or(
                CodecSetting::Other { key, value },
                CodecSetting::RtxAssociation,
            ),
        rfc_rtp::fmtp::H264_PACKETIZATION_MODE => value
            .parse::<u8>()
            .ok()
            .and_then(rfc_rtp::h264::PacketizationMode::from_fmtp_value)
            .map_or_else(
                || CodecSetting::Other { key, value },
                CodecSetting::H264PacketizationMode,
            ),
        rfc_rtp::fmtp::H264_PROFILE_LEVEL_ID => CodecSetting::H264ProfileLevelId(value),
        rfc_rtp::fmtp::VP9_PROFILE_ID => value.parse::<u8>().map_or(
            CodecSetting::Other { key, value },
            CodecSetting::Vp9ProfileId,
        ),
        rfc_rtp::fmtp::OPUS_USE_IN_BAND_FEC => match value.as_str() {
            rfc_rtp::fmtp::VALUE_ENABLED | rfc_rtp::fmtp::VALUE_TRUE => {
                CodecSetting::UseInBandFec(true)
            }
            rfc_rtp::fmtp::VALUE_DISABLED | rfc_rtp::fmtp::VALUE_FALSE => {
                CodecSetting::UseInBandFec(false)
            }
            _ => CodecSetting::Other { key, value },
        },
        _ => CodecSetting::Other { key, value },
    }
}
