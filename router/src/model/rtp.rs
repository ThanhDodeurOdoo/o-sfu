//! RFC references for this module:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>
//!
//! This module define the router-native RTP model used at the transport edge.
//! It keeps codec, header-extension, and stream-binding data in typed domain
//! strcutures instead of protocol-shaped JSON bags.

use std::borrow::Cow;

use super::MediaKind;
use o_sfu_rfc::{rtp as rfc_rtp, webrtc as rfc_webrtc};

pub type MediaCodec = rfc_rtp::CodecName;
pub type HeaderExtensionUri = rfc_webrtc::RtpHeaderExtensionUri;

/// RTCP feedback categories used by codec capabilities and negotiated formats.
///
/// Reference:
/// - RFC 3550 (RTP/RTCP)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtcpFeedbackKind {
    Nack,
    NackPli,
    CcmFir,
    GoogRemb,
    TransportCc,
    Other(String),
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PayloadType(u8);

impl PayloadType {
    #[must_use]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl From<u8> for PayloadType {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl From<PayloadType> for u8 {
    fn from(value: PayloadType) -> Self {
        value.value()
    }
}

/// Synchronization source identifier for a media stream.
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

/// RTP stream identifier used for simulcast or layered media.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rid(String);

impl Rid {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
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

/// Media identification carried at the SDP and RTP routing boundary.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeaderExtensionId(u8);

impl HeaderExtensionId {
    #[must_use]
    pub const fn new(value: u8) -> Self {
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

/// Typed codec parameter (affects interoperability).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecSetting {
    RtxAssociation(PayloadType),
    H264PacketizationMode(u8),
    H264ProfileLevelId(String),
    Vp9ProfileId(u8),
    UseInBandFec(bool),
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
            Self::H264PacketizationMode(mode) => Cow::Owned(mode.to_string()),
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

/// RTP header-extension capability or negotiated use.
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
/// `media_kind`, `codec`, and `clock_rate` identify the codec family,
/// `payload_type` is optional because capability advertisements may express a
/// preference instead of a fixed mapping, `channels` is only meaningful for
/// audio, and `settings` plus `rtcp_feedback` preserve the interoperability
/// constraints that matter during negotiation.
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
    pub fn with_payload_type(mut self, payload_type: impl Into<PayloadType>) -> Self {
        self.payload_type = Some(payload_type.into());
        self
    }

    #[must_use]
    pub fn with_preferred_payload_type(self, payload_type: u8) -> Self {
        self.with_payload_type(payload_type)
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
}

/// Full capability set for one RTP endpoint.
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

/// One negotiated codec format inside a concrete media stream.
///
/// Unlike [`MediaCodecCapability`], the payload type is fixed here because this
/// shape represents an actual producer, consumable stream, or consumer result.
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
        payload_type: impl Into<PayloadType>,
        clock_rate: u32,
    ) -> Self {
        Self {
            media_kind,
            codec: codec.into(),
            payload_type: payload_type.into(),
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
}

/// Binding between a negotiated media format and the source-specific routing ids.
///
/// Depending on the flow, a binding may carry SSRC, RID, payload type remapping,
/// and bitrate hints. The router keeps this data typed so transport code can map
/// media packets without re-parsing protocol-shaped dictionaries.
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
    pub fn with_payload_type(mut self, payload_type: impl Into<PayloadType>) -> Self {
        self.payload_type = Some(payload_type.into());
        self
    }

    #[must_use]
    pub fn with_max_bitrate(mut self, max_bitrate: u64) -> Self {
        self.max_bitrate = Some(max_bitrate);
        self
    }

    #[must_use]
    pub(super) fn ssrc_id(&self) -> Option<Ssrc> {
        self.ssrc
    }

    #[must_use]
    pub fn ssrc(&self) -> Option<u32> {
        self.ssrc.map(Ssrc::value)
    }

    #[must_use]
    pub(super) fn rid_id(&self) -> Option<&Rid> {
        self.rid.as_ref()
    }

    #[must_use]
    pub fn rid(&self) -> Option<&str> {
        self.rid.as_ref().map(Rid::as_str)
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
    pub fn with_codec_payload_type(self, payload_type: u8) -> Self {
        self.with_payload_type(payload_type)
    }

    #[must_use]
    pub fn max_bitrate(&self) -> Option<u64> {
        self.max_bitrate
    }
}

/// Concrete RTP stream description used at the router and transport boundary.
///
/// `formats` lists the negotiated codecs, `header_extensions` lists the
/// negotiated extension set, `bindings` ties source ids such as SSRC or RID to
/// those formats, and `mid` carries the media section identity when one exists.
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

    pub fn codecs(&self) -> impl Iterator<Item = &MediaFormat> {
        self.formats()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &HeaderExtension> {
        self.header_extensions.iter()
    }

    pub fn bindings(&self) -> impl Iterator<Item = &StreamBinding> {
        self.bindings.iter()
    }

    pub fn encodings(&self) -> impl Iterator<Item = &StreamBinding> {
        self.bindings()
    }

    #[must_use]
    pub fn mid(&self) -> Option<&str> {
        self.mid.as_ref().map(Mid::as_str)
    }
}

fn codec_setting_from_wire(key: String, value: String) -> CodecSetting {
    match key.as_str() {
        rfc_rtp::fmtp::RTX_ASSOCIATION => value.parse::<u8>().map(PayloadType::new).map_or(
            CodecSetting::Other { key, value },
            CodecSetting::RtxAssociation,
        ),
        rfc_rtp::fmtp::H264_PACKETIZATION_MODE => value.parse::<u8>().map_or(
            CodecSetting::Other { key, value },
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
