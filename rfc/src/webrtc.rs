//! RFC references for this module:
//! - WebRTC RTP usage profile: <https://www.rfc-editor.org/rfc/rfc8834>
//! - ICE protocol: <https://www.rfc-editor.org/rfc/rfc8445>
//! - ICE candidate grammar (legacy, still interoperable in SDP): <https://www.rfc-editor.org/rfc/rfc5245>
//! - DTLS-SRTP protection profiles: <https://www.rfc-editor.org/rfc/rfc5764>
//! - BUNDLE and MID signaling: <https://www.rfc-editor.org/rfc/rfc8843>
//! - RTP stream ID header extensions: <https://www.rfc-editor.org/rfc/rfc8852>
//! - RTCP multiplexing: <https://www.rfc-editor.org/rfc/rfc5761>
//! - SDP `setup` roles for connection-oriented media: <https://www.rfc-editor.org/rfc/rfc4145>
//! - DTLS-SRTP offer/answer usage of `setup`: <https://www.rfc-editor.org/rfc/rfc5763>

use std::fmt;

/// WebRTC RTP profile name.
///
/// Reference: RFC 8834 section 4.2.
pub const RTP_PROFILE_SAVPF: &str = "RTP/SAVPF";

/// ICE portocol registries used by WebRTC signaling.
pub mod ice {
    /// ICE component IDs for RTP and RTCP.
    ///
    /// Reference: RFC 8445 section 5.1.1.
    pub mod component {
        pub const RTP: u16 = 1;
        pub const RTCP: u16 = 2;
    }

    /// ICE candidate type literals used by SDP candidate attributes.
    ///
    /// References:
    /// - RFC 5245 section 15.1 candidate grammar (`typ host|srflx|prflx|relay`)
    /// - RFC 8445 (semantic model preserved by the updated ICE specification)
    pub mod candidate_type {
        pub const HOST: &str = "host";
        pub const SERVER_REFLEXIVE: &str = "srflx";
        pub const PEER_REFLEXIVE: &str = "prflx";
        pub const RELAYED: &str = "relay";
    }

    /// Recommended ICE type-preference values.
    ///
    /// Reference: RFC 8445 section 5.1.2.2.
    pub mod type_preference {
        pub const HOST: u8 = 126;
        pub const PEER_REFLEXIVE: u8 = 110;
        pub const SERVER_REFLEXIVE: u8 = 100;
        pub const RELAYED: u8 = 0;
    }

    /// ICE transport token used in SDP candidate lines.
    ///
    /// Reference: RFC 8445 section 5.1.1 and candidate grammar inherited from RFC 5245.
    pub mod transport {
        pub const UDP: &str = "udp";
        pub const TCP: &str = "tcp";
    }
}

/// ICE transport tokens used in SDP candidate lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IceTransport {
    Udp,
    Tcp,
}

impl IceTransport {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Udp => ice::transport::UDP,
            Self::Tcp => ice::transport::TCP,
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        if token.eq_ignore_ascii_case(ice::transport::UDP) {
            return Some(Self::Udp);
        }
        if token.eq_ignore_ascii_case(ice::transport::TCP) {
            return Some(Self::Tcp);
        }
        None
    }
}

impl AsRef<str> for IceTransport {
    fn as_ref(&self) -> &str {
        match self {
            Self::Udp => ice::transport::UDP,
            Self::Tcp => ice::transport::TCP,
        }
    }
}

impl fmt::Display for IceTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// ICE candidate type tokens used in SDP candidate attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IceCandidateType {
    Host,
    ServerReflexive,
    PeerReflexive,
    Relayed,
}

impl IceCandidateType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => ice::candidate_type::HOST,
            Self::ServerReflexive => ice::candidate_type::SERVER_REFLEXIVE,
            Self::PeerReflexive => ice::candidate_type::PEER_REFLEXIVE,
            Self::Relayed => ice::candidate_type::RELAYED,
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        match token {
            ice::candidate_type::HOST => Some(Self::Host),
            ice::candidate_type::SERVER_REFLEXIVE => Some(Self::ServerReflexive),
            ice::candidate_type::PEER_REFLEXIVE => Some(Self::PeerReflexive),
            ice::candidate_type::RELAYED => Some(Self::Relayed),
            _ => None,
        }
    }
}

impl AsRef<str> for IceCandidateType {
    fn as_ref(&self) -> &str {
        match self {
            Self::Host => ice::candidate_type::HOST,
            Self::ServerReflexive => ice::candidate_type::SERVER_REFLEXIVE,
            Self::PeerReflexive => ice::candidate_type::PEER_REFLEXIVE,
            Self::Relayed => ice::candidate_type::RELAYED,
        }
    }
}

impl fmt::Display for IceCandidateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// MIME top-level media kidns used by ORTC and SDP paylods
/// same as on web stream/tracks APIs
pub mod media_kind {
    pub const AUDIO: &str = "audio";
    pub const VIDEO: &str = "video";
}

/// RTCP feedback type and parameter tokens used by current WebRTC capability paylods.
pub mod rtcp_feedback {
    /// RTCP feedback kind tokens used in capability dictionnaries.
    pub mod kind {
        /// Generic NACK feedback type token.
        ///
        /// Reference: RFC 4585 section 6.2.1.
        pub const NACK: &str = "nack";

        /// Codec control message feedback type token.
        ///
        /// Reference: RFC 5104.
        pub const CCM: &str = "ccm";

        /// Google Receiver Estimated Maximum Bitrate token used by current browser stacks.
        pub const GOOG_REMB: &str = "goog-remb";

        /// Transport-wide congestion control feedback token.
        ///
        /// Reference:
        /// <https://www.ietf.org/archive/id/draft-holmer-rmcat-transport-wide-cc-extensions-01.txt>
        pub const TRANSPORT_CC: &str = "transport-cc";
    }

    /// RTCP feedback parameter tokens used by current WebRTC cpaability payloads.
    pub mod parameter {
        /// Picture loss indication parameter token.
        ///
        /// Reference: RFC 4585 section 6.3.1.
        pub const PLI: &str = "pli";

        /// Full intra request parameter token.
        ///
        /// Reference: RFC 5104 section 4.3.1.
        pub const FIR: &str = "fir";
    }
}

pub mod sdp {
    pub mod group_semantics {
        /// `a=group:BUNDLE ...`
        ///
        /// Reference: RFC 8843.
        pub const BUNDLE: &str = "BUNDLE";
    }

    pub mod attribute {
        /// `a=rtcp-mux`
        ///
        /// Reference: RFC 5761.
        pub const RTCP_MUX: &str = "rtcp-mux";

        /// `a=setup:<role>`
        ///
        /// References: RFC 4145, RFC 5763.
        pub const SETUP: &str = "setup";
    }

    pub mod setup_role {
        pub const ACTIVE: &str = "active";
        pub const PASSIVE: &str = "passive";
        pub const ACTPASS: &str = "actpass";
        pub const HOLDCONN: &str = "holdconn";
    }

    pub mod direction {
        /// `a=sendrecv`
        ///
        /// Reference: RFC 8866 section 6.7.
        pub const SEND_RECV: &str = "sendrecv";
    }
}

/// DTLS `setup` roles used by current WebRTC transport payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DtlsRole {
    Auto,
    Client,
    Server,
}

impl DtlsRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Client => "client",
            Self::Server => "server",
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "auto" => Some(Self::Auto),
            "client" => Some(Self::Client),
            "server" => Some(Self::Server),
            _ => None,
        }
    }
}

impl AsRef<str> for DtlsRole {
    fn as_ref(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Client => "client",
            Self::Server => "server",
        }
    }
}

impl fmt::Display for DtlsRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// DTLS fingerprint algorithms currently supported by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DtlsFingerprintAlgorithm {
    Sha256,
}

impl DtlsFingerprintAlgorithm {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha-256",
        }
    }

    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        if token.eq_ignore_ascii_case("sha-256") {
            return Some(Self::Sha256);
        }
        None
    }
}

impl AsRef<str> for DtlsFingerprintAlgorithm {
    fn as_ref(&self) -> &str {
        match self {
            Self::Sha256 => "sha-256",
        }
    }
}

impl fmt::Display for DtlsFingerprintAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// DTLS-SRTP protection profile identifiers for `use_srtp`.
///
/// Reference: RFC 5764 section 4.1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum DtlsSrtpProtectionProfile {
    Aes128CmHmacSha1_80 = 0x0001,
    Aes128CmHmacSha1_32 = 0x0002,
}

impl DtlsSrtpProtectionProfile {
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        match self {
            Self::Aes128CmHmacSha1_80 => 0x0001,
            Self::Aes128CmHmacSha1_32 => 0x0002,
        }
    }
}

/// SCTP transport dictionary defaults preserved by the current WebRTC bootstrap payload.
pub mod data_channel {
    pub const SCTP_PORT: u16 = 5_000;
    pub const OUTGOING_STREAMS: u16 = 1_024;
    pub const INCOMING_STREAMS: u16 = 1_024;
    pub const MAX_MESSAGE_SIZE: u32 = 262_144;
}

/// RTP header-extension URIs commonly needed by WebRTC endpoints.
pub mod rtp_header_extension_uri {
    macro_rules! rtp_header_extension_urn {
        ($suffix:literal) => {
            concat!("urn:ietf:params:rtp-hdrext:", $suffix)
        };
    }

    macro_rules! rtp_header_extension_sdes_urn {
        ($suffix:literal) => {
            concat!("urn:ietf:params:rtp-hdrext:sdes:", $suffix)
        };
    }

    /// MID RTP header extension URI.
    ///
    /// Reference: RFC 8843 section 15.2.
    pub const MID: &str = rtp_header_extension_sdes_urn!("mid");

    /// Audio level RTP header extension URI.
    ///
    /// Reference: RFC 6464 section 3.
    pub const SSRC_AUDIO_LEVEL: &str = rtp_header_extension_urn!("ssrc-audio-level");

    /// Mixer-to-client audio level RTP header extension URI.
    ///
    /// Reference: RFC 6465 section 4.
    pub const CSRC_AUDIO_LEVEL: &str = rtp_header_extension_urn!("csrc-audio-level");

    /// RTP stream ID extension URI.
    ///
    /// Reference: RFC 8852.
    pub const RTP_STREAM_ID: &str = rtp_header_extension_sdes_urn!("rtp-stream-id");

    /// Repaired RTP stream ID extension URI.
    ///
    /// Reference: RFC 8852.
    pub const REPAIRED_RTP_STREAM_ID: &str =
        rtp_header_extension_sdes_urn!("repaired-rtp-stream-id");

    /// Absolute send time RTP header extension URI.
    ///
    /// The RTP header-extension framework identify extensions by URI string,
    /// and that URI is signaled verbatim in SDP `a=extmap` lines. Even though
    /// this identifier looks like an HTTP URL, it just is a protocol name.
    ///
    /// Current WebRTC stacks use this exact literal, so we preserve it for interoperability.
    ///
    /// Reference: <https://www.webrtc.org/experiments/rtp-hdrext/abs-send-time>
    pub const ABS_SEND_TIME: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time";

    /// Transport-wide sequence number RTP header extension URI.
    ///
    /// This value is likewise the negotiated wire identifier carried in SDP
    /// `a=extmap` lines. It is not dereferenced as a network resource; the
    /// exact string itself is the interoperability key used to match the
    /// extension.
    ///
    /// Browsers and other rtc ecosystems commonly advertise the
    /// historical `...-01` draft URI literal, so we keeps that deployed
    /// identifier instead of normalizing it to a different name.
    ///
    /// Reference:
    /// <https://www.ietf.org/archive/id/draft-holmer-rmcat-transport-wide-cc-extensions-01.txt>
    pub const TRANSPORT_WIDE_CC_DRAFT_01: &str =
        "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";
}

/// RTP header-extension URIs commonly needed by WebRTC endpoints.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RtpHeaderExtensionUri {
    Mid,
    AbsSendTime,
    TransportWideCcDraft01,
    SsrcAudioLevel,
    Other(String),
}

impl RtpHeaderExtensionUri {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Mid => rtp_header_extension_uri::MID,
            Self::AbsSendTime => rtp_header_extension_uri::ABS_SEND_TIME,
            Self::TransportWideCcDraft01 => rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
            Self::SsrcAudioLevel => rtp_header_extension_uri::SSRC_AUDIO_LEVEL,
            Self::Other(uri) => uri.as_str(),
        }
    }
}

impl From<&str> for RtpHeaderExtensionUri {
    fn from(value: &str) -> Self {
        match value {
            rtp_header_extension_uri::MID => Self::Mid,
            rtp_header_extension_uri::ABS_SEND_TIME => Self::AbsSendTime,
            rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01 => Self::TransportWideCcDraft01,
            rtp_header_extension_uri::SSRC_AUDIO_LEVEL => Self::SsrcAudioLevel,
            _ => Self::Other(value.to_owned()),
        }
    }
}

impl From<String> for RtpHeaderExtensionUri {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl AsRef<str> for RtpHeaderExtensionUri {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RtpHeaderExtensionUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// RTCP SDES item type defined for MID.
///
/// Reference: RFC 8843 section 15.1.
pub const RTCP_SDES_ITEM_MID: u8 = 15;

/// Maybe needelessly verbose, may remove tests later or only keep a few
#[cfg(test)]
mod tests {
    use super::{
        DtlsSrtpProtectionProfile, RTCP_SDES_ITEM_MID, RTP_PROFILE_SAVPF, data_channel, ice,
        media_kind, rtcp_feedback, rtp_header_extension_uri, sdp,
    };

    #[test]
    fn ice_constants_match_expected_literals() {
        assert_eq!(ice::component::RTP, 1);
        assert_eq!(ice::component::RTCP, 2);
        assert_eq!(ice::candidate_type::HOST, "host");
        assert_eq!(ice::candidate_type::SERVER_REFLEXIVE, "srflx");
        assert_eq!(ice::candidate_type::PEER_REFLEXIVE, "prflx");
        assert_eq!(ice::candidate_type::RELAYED, "relay");
        assert_eq!(ice::transport::UDP, "udp");
        assert_eq!(ice::transport::TCP, "tcp");
    }

    #[test]
    fn media_kind_feedback_and_sctp_literals_are_stable() {
        assert_eq!(media_kind::AUDIO, "audio");
        assert_eq!(media_kind::VIDEO, "video");
        assert_eq!(rtcp_feedback::kind::NACK, "nack");
        assert_eq!(rtcp_feedback::kind::CCM, "ccm");
        assert_eq!(rtcp_feedback::kind::GOOG_REMB, "goog-remb");
        assert_eq!(rtcp_feedback::kind::TRANSPORT_CC, "transport-cc");
        assert_eq!(rtcp_feedback::parameter::PLI, "pli");
        assert_eq!(rtcp_feedback::parameter::FIR, "fir");
        assert_eq!(sdp::direction::SEND_RECV, "sendrecv");
        assert_eq!(data_channel::SCTP_PORT, 5_000);
        assert_eq!(data_channel::OUTGOING_STREAMS, 1_024);
        assert_eq!(data_channel::INCOMING_STREAMS, 1_024);
        assert_eq!(data_channel::MAX_MESSAGE_SIZE, 262_144);
    }

    #[test]
    fn dtls_srtp_profiles_match_rfc5764_registry_values() {
        assert_eq!(
            DtlsSrtpProtectionProfile::Aes128CmHmacSha1_80.as_u16(),
            0x0001
        );
        assert_eq!(
            DtlsSrtpProtectionProfile::Aes128CmHmacSha1_32.as_u16(),
            0x0002
        );
    }

    #[test]
    fn webrtc_profile_and_mid_literals_are_stable() {
        assert_eq!(RTP_PROFILE_SAVPF, "RTP/SAVPF");
        assert_eq!(
            rtp_header_extension_uri::MID,
            "urn:ietf:params:rtp-hdrext:sdes:mid"
        );
        assert_eq!(
            rtp_header_extension_uri::ABS_SEND_TIME,
            "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time"
        );
        assert_eq!(
            rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
            "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
        );
        assert_eq!(RTCP_SDES_ITEM_MID, 15);
    }
}
