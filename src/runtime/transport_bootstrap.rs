use std::net::IpAddr;

use o_sfu_router::MediaCapabilities;

use crate::rfc::webrtc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionTransportBootstrap {
    pub(crate) router_capabilities: MediaCapabilities,
    pub(crate) download_transport: TransportEndpointBootstrap,
    pub(crate) upload_transport: TransportEndpointBootstrap,
    pub(crate) publish_options_by_media_kind: TransportPublishOptionsByMediaKind,
}

impl SessionTransportBootstrap {
    #[must_use]
    pub(crate) fn new(
        router_capabilities: &MediaCapabilities,
        download_transport: TransportEndpointBootstrap,
        upload_transport: TransportEndpointBootstrap,
    ) -> Self {
        Self {
            router_capabilities: router_capabilities.clone(),
            download_transport,
            upload_transport,
            publish_options_by_media_kind: default_publish_options_by_media_kind(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportEndpointBootstrap {
    pub(crate) id: String,
    pub(crate) ice_parameters: TransportIceParameters,
    pub(crate) ice_candidates: Vec<TransportIceCandidate>,
    pub(crate) dtls_parameters: TransportDtlsParameters,
    pub(crate) sctp_parameters: TransportSctpParameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportIceParameters {
    pub(crate) username_fragment: String,
    pub(crate) password: String,
    pub(crate) ice_lite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportIceCandidate {
    pub(crate) foundation: String,
    pub(crate) priority: u64,
    pub(crate) ip: IpAddr,
    pub(crate) protocol: TransportIceProtocol,
    pub(crate) port: u16,
    pub(crate) candidate_type: TransportIceCandidateType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportIceProtocol {
    Udp,
}

impl TransportIceProtocol {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Udp => webrtc::IceTransport::Udp.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportIceCandidateType {
    Host,
}

impl TransportIceCandidateType {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Host => webrtc::IceCandidateType::Host.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportDtlsParameters {
    pub(crate) role: TransportDtlsRole,
    pub(crate) fingerprints: Vec<TransportDtlsFingerprint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportDtlsRole {
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportDtlsFingerprint {
    pub(crate) algorithm: TransportDtlsFingerprintAlgorithm,
    pub(crate) value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportDtlsFingerprintAlgorithm {
    Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportSctpParameters {
    pub(crate) port: u16,
    pub(crate) outgoing_streams: u16,
    pub(crate) incoming_streams: u16,
    pub(crate) max_message_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransportPublishOptionsByMediaKind {
    pub(crate) audio: TransportPublishOptions,
    pub(crate) video: TransportPublishOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportPublishOptions {
    pub(crate) stop_tracks: bool,
    pub(crate) zero_rtp_on_pause: bool,
}

#[must_use]
pub(crate) fn default_publish_options_by_media_kind() -> TransportPublishOptionsByMediaKind {
    TransportPublishOptionsByMediaKind {
        audio: TransportPublishOptions {
            stop_tracks: false,
            zero_rtp_on_pause: false,
        },
        video: TransportPublishOptions {
            stop_tracks: false,
            zero_rtp_on_pause: true,
        },
    }
}

#[must_use]
pub(crate) fn default_sctp_parameters() -> TransportSctpParameters {
    TransportSctpParameters {
        port: webrtc::data_channel::SCTP_PORT,
        outgoing_streams: webrtc::data_channel::OUTGOING_STREAMS,
        incoming_streams: webrtc::data_channel::INCOMING_STREAMS,
        max_message_size: u64::from(webrtc::data_channel::MAX_MESSAGE_SIZE),
    }
}
