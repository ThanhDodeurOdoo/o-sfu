//! RFC references for this module:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>
//! - ORTC API dictionaries (for type-shape alignment): <https://www.w3.org/TR/ortc/>

use std::collections::BTreeMap;

use super::MediaKind;

/// RTCP feedback categories used by RTP codec capabilities and parameters.
///
/// Reference:
/// - RFC 3550 (RTP/RTCP)
/// - ORTC API `RTCRtcpFeedback` dictionary
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtcpFeedbackKind {
    Nack,
    NackPli,
    CcmFir,
    GoogRemb,
    TransportCc,
    Other(String),
}

/// A single RTCP feedback entry.
///
/// `parameter` allows feedback-specific payloads such as `"pli"` for NACK.
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

/// Router-supported RTP codec capability.
///
/// Reference:
/// - RFC 3551 (RTP A/V profile payload and media clock conventions)
/// - ORTC API `RTCRtpCodecCapability` dictionary
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpCodecCapability {
    media_kind: MediaKind,
    codec_name: String,
    clock_rate: u32,
    preferred_payload_type: Option<u8>,
    channels: Option<u16>,
    parameters: BTreeMap<String, String>,
    rtcp_feedback: Vec<RtcpFeedback>,
}

impl RtpCodecCapability {
    #[must_use]
    pub fn new(media_kind: MediaKind, codec_name: impl Into<String>, clock_rate: u32) -> Self {
        Self {
            media_kind,
            codec_name: codec_name.into(),
            clock_rate,
            preferred_payload_type: None,
            channels: None,
            parameters: BTreeMap::new(),
            rtcp_feedback: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_preferred_payload_type(mut self, payload_type: u8) -> Self {
        self.preferred_payload_type = Some(payload_type);
        self
    }

    #[must_use]
    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }

    #[must_use]
    pub fn with_parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
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
    pub fn codec_name(&self) -> &str {
        &self.codec_name
    }

    #[must_use]
    pub fn clock_rate(&self) -> u32 {
        self.clock_rate
    }

    #[must_use]
    pub fn preferred_payload_type(&self) -> Option<u8> {
        self.preferred_payload_type
    }

    #[must_use]
    pub fn channels(&self) -> Option<u16> {
        self.channels
    }

    pub fn parameters(&self) -> impl Iterator<Item = (&str, &str)> {
        self.parameters
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    pub fn rtcp_feedback(&self) -> impl Iterator<Item = &RtcpFeedback> {
        self.rtcp_feedback.iter()
    }
}

/// RTP header extension capability.
///
/// Reference:
/// - RFC 8285 (RTP header extensions)
/// - ORTC API `RTCRtpHeaderExtensionCapability` dictionary
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpHeaderExtension {
    uri: String,
    id: u8,
    encrypt: bool,
}

impl RtpHeaderExtension {
    #[must_use]
    pub fn new(uri: impl Into<String>, id: u8) -> Self {
        Self {
            uri: uri.into(),
            id,
            encrypt: false,
        }
    }

    #[must_use]
    pub fn with_encryption(mut self, encrypt: bool) -> Self {
        self.encrypt = encrypt;
        self
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub fn id(&self) -> u8 {
        self.id
    }

    #[must_use]
    pub fn encrypt(&self) -> bool {
        self.encrypt
    }
}

/// Router-level RTP capabilities used to gate producer/consumer compatibility.
///
/// Reference:
/// - ORTC API `RTCRtpCapabilities` dictionary
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RtpCapabilities {
    codecs: Vec<RtpCodecCapability>,
    header_extensions: Vec<RtpHeaderExtension>,
}

impl RtpCapabilities {
    #[must_use]
    pub fn new(
        codecs: Vec<RtpCodecCapability>,
        header_extensions: Vec<RtpHeaderExtension>,
    ) -> Self {
        Self {
            codecs,
            header_extensions,
        }
    }

    pub fn codecs(&self) -> impl Iterator<Item = &RtpCodecCapability> {
        self.codecs.iter()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &RtpHeaderExtension> {
        self.header_extensions.iter()
    }
}

/// Concrete RTP codec parameters for one negotiated RTP stream.
///
/// Reference:
/// - ORTC API `RTCRtpCodecParameters` dictionary
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpCodecParameters {
    media_kind: MediaKind,
    codec_name: String,
    payload_type: u8,
    clock_rate: u32,
    channels: Option<u16>,
    parameters: BTreeMap<String, String>,
    rtcp_feedback: Vec<RtcpFeedback>,
}

impl RtpCodecParameters {
    #[must_use]
    pub fn new(
        media_kind: MediaKind,
        codec_name: impl Into<String>,
        payload_type: u8,
        clock_rate: u32,
    ) -> Self {
        Self {
            media_kind,
            codec_name: codec_name.into(),
            payload_type,
            clock_rate,
            channels: None,
            parameters: BTreeMap::new(),
            rtcp_feedback: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }

    #[must_use]
    pub fn with_parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
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
    pub fn codec_name(&self) -> &str {
        &self.codec_name
    }

    #[must_use]
    pub fn payload_type(&self) -> u8 {
        self.payload_type
    }

    #[must_use]
    pub fn clock_rate(&self) -> u32 {
        self.clock_rate
    }

    #[must_use]
    pub fn channels(&self) -> Option<u16> {
        self.channels
    }

    pub fn parameters(&self) -> impl Iterator<Item = (&str, &str)> {
        self.parameters
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    pub fn rtcp_feedback(&self) -> impl Iterator<Item = &RtcpFeedback> {
        self.rtcp_feedback.iter()
    }
}

/// Per-encoding RTP settings.
///
/// Reference:
/// - ORTC API `RTCRtpEncodingParameters` dictionary
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RtpEncoding {
    ssrc: Option<u32>,
    rid: Option<String>,
    codec_payload_type: Option<u8>,
    max_bitrate: Option<u64>,
}

impl RtpEncoding {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_ssrc(mut self, ssrc: u32) -> Self {
        self.ssrc = Some(ssrc);
        self
    }

    #[must_use]
    pub fn with_rid(mut self, rid: impl Into<String>) -> Self {
        self.rid = Some(rid.into());
        self
    }

    #[must_use]
    pub fn with_codec_payload_type(mut self, codec_payload_type: u8) -> Self {
        self.codec_payload_type = Some(codec_payload_type);
        self
    }

    #[must_use]
    pub fn with_max_bitrate(mut self, max_bitrate: u64) -> Self {
        self.max_bitrate = Some(max_bitrate);
        self
    }

    #[must_use]
    pub fn ssrc(&self) -> Option<u32> {
        self.ssrc
    }

    #[must_use]
    pub fn rid(&self) -> Option<&str> {
        self.rid.as_deref()
    }

    #[must_use]
    pub fn codec_payload_type(&self) -> Option<u8> {
        self.codec_payload_type
    }

    #[must_use]
    pub fn max_bitrate(&self) -> Option<u64> {
        self.max_bitrate
    }
}

/// Full RTP parameters for a producer or consumer stream.
///
/// Reference:
/// - ORTC API `RTCRtpParameters` dictionary
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RtpParameters {
    codecs: Vec<RtpCodecParameters>,
    header_extensions: Vec<RtpHeaderExtension>,
    encodings: Vec<RtpEncoding>,
    mid: Option<String>,
}

impl RtpParameters {
    #[must_use]
    pub fn new(
        codecs: Vec<RtpCodecParameters>,
        header_extensions: Vec<RtpHeaderExtension>,
        encodings: Vec<RtpEncoding>,
    ) -> Self {
        Self {
            codecs,
            header_extensions,
            encodings,
            mid: None,
        }
    }

    #[must_use]
    pub fn with_mid(mut self, mid: impl Into<String>) -> Self {
        self.mid = Some(mid.into());
        self
    }

    pub fn codecs(&self) -> impl Iterator<Item = &RtpCodecParameters> {
        self.codecs.iter()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &RtpHeaderExtension> {
        self.header_extensions.iter()
    }

    pub fn encodings(&self) -> impl Iterator<Item = &RtpEncoding> {
        self.encodings.iter()
    }

    #[must_use]
    pub fn mid(&self) -> Option<&str> {
        self.mid.as_deref()
    }
}
