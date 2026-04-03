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

/// WebRTC RTP profile name.
///
/// Reference: RFC 8834 section 4.2.
pub const RTP_PROFILE_SAVPF: &str = "RTP/SAVPF";

/// ICE component IDs for RTP and RTCP.
///
/// Reference: RFC 8445 section 5.1.1.
pub const ICE_COMPONENT_RTP: u16 = 1;
pub const ICE_COMPONENT_RTCP: u16 = 2;

/// ICE candidate type literals used by SDP candidate attributes.
///
/// References:
/// - RFC 5245 section 15.1 candidate grammar (`typ host|srflx|prflx|relay`)
/// - RFC 8445 (semantic model preserved by the updated ICE specification)
pub const ICE_CANDIDATE_TYPE_HOST: &str = "host";
pub const ICE_CANDIDATE_TYPE_SERVER_REFLEXIVE: &str = "srflx";
pub const ICE_CANDIDATE_TYPE_PEER_REFLEXIVE: &str = "prflx";
pub const ICE_CANDIDATE_TYPE_RELAYED: &str = "relay";

/// Recommended ICE type-preference values.
///
/// Reference: RFC 8445 section 5.1.2.2.
pub const ICE_TYPE_PREFERENCE_HOST: u8 = 126;
pub const ICE_TYPE_PREFERENCE_PEER_REFLEXIVE: u8 = 110;
pub const ICE_TYPE_PREFERENCE_SERVER_REFLEXIVE: u8 = 100;
pub const ICE_TYPE_PREFERENCE_RELAYED: u8 = 0;

/// ICE transport token used in SDP candidate lines.
///
/// Reference: RFC 8445 section 5.1.1 and candidate grammar inherited from RFC 5245.
pub const ICE_TRANSPORT_UDP: &str = "udp";

/// SDP attribute names and literals used by WebRTC signaling.
pub mod sdp {
    /// `a=group:BUNDLE ...`
    ///
    /// Reference: RFC 8843.
    pub const GROUP_SEMANTICS_BUNDLE: &str = "BUNDLE";

    /// `a=rtcp-mux`
    ///
    /// Reference: RFC 5761.
    pub const ATTRIBUTE_RTCP_MUX: &str = "rtcp-mux";

    /// `a=setup:<role>`
    ///
    /// References: RFC 4145, RFC 5763.
    pub const ATTRIBUTE_SETUP: &str = "setup";
    pub const SETUP_ACTIVE: &str = "active";
    pub const SETUP_PASSIVE: &str = "passive";
    pub const SETUP_ACTPASS: &str = "actpass";
    pub const SETUP_HOLDCONN: &str = "holdconn";
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

/// RTP header-extension URIs commonly needed by WebRTC endpoints.
pub mod rtp_header_extension_uri {
    /// MID RTP header extension URI.
    ///
    /// Reference: RFC 8843 section 15.2.
    pub const MID: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";

    /// Audio level RTP header extension URI.
    ///
    /// Reference: RFC 6464 section 3.
    pub const SSRC_AUDIO_LEVEL: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";

    /// Mixer-to-client audio level RTP header extension URI.
    ///
    /// Reference: RFC 6465 section 4.
    pub const CSRC_AUDIO_LEVEL: &str = "urn:ietf:params:rtp-hdrext:csrc-audio-level";

    /// RTP stream ID extension URI.
    ///
    /// Reference: RFC 8852.
    pub const RTP_STREAM_ID: &str = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id";

    /// Repaired RTP stream ID extension URI.
    ///
    /// Reference: RFC 8852.
    pub const REPAIRED_RTP_STREAM_ID: &str =
        "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";
}

/// RTCP SDES item type defined for MID.
///
/// Reference: RFC 8843 section 15.1.
pub const RTCP_SDES_ITEM_MID: u8 = 15;

#[cfg(test)]
mod tests {
    use super::{
        DtlsSrtpProtectionProfile, ICE_CANDIDATE_TYPE_HOST, ICE_CANDIDATE_TYPE_PEER_REFLEXIVE,
        ICE_CANDIDATE_TYPE_RELAYED, ICE_CANDIDATE_TYPE_SERVER_REFLEXIVE, ICE_COMPONENT_RTCP,
        ICE_COMPONENT_RTP, RTCP_SDES_ITEM_MID, RTP_PROFILE_SAVPF, rtp_header_extension_uri,
    };

    #[test]
    fn ice_constants_match_expected_literals() {
        assert_eq!(ICE_COMPONENT_RTP, 1);
        assert_eq!(ICE_COMPONENT_RTCP, 2);
        assert_eq!(ICE_CANDIDATE_TYPE_HOST, "host");
        assert_eq!(ICE_CANDIDATE_TYPE_SERVER_REFLEXIVE, "srflx");
        assert_eq!(ICE_CANDIDATE_TYPE_PEER_REFLEXIVE, "prflx");
        assert_eq!(ICE_CANDIDATE_TYPE_RELAYED, "relay");
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
        assert_eq!(RTCP_SDES_ITEM_MID, 15);
    }
}
