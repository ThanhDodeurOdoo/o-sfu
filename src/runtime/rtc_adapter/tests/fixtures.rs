#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]
pub(super) use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    slice,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

pub(super) use o_sfu_router::{
    RtpCapabilities as RouterRtpCapabilities, RtpEncoding as RouterRtpEncoding,
    RtpParameters as RouterRtpParameters,
};
pub(super) use str0m::media::{MediaKind as Str0mMediaKind, Mid};
pub(super) use tokio::time::sleep;

pub(super) use super::super::{
    RtcTransportAdapter, commands::DebugPacketGate, shared_payload::SharedPayload, validation,
};
pub(super) use crate::{
    runtime::transport_adapter::{
        TransportAdapterError, TransportConnectDirection, TransportConnectRequest,
        TransportMediaId, TransportSessionKey,
    },
    runtime::transport_bootstrap::{
        SessionTransportBootstrap, TransportDtlsFingerprint, TransportDtlsFingerprintAlgorithm,
        TransportDtlsParameters, TransportDtlsRole, TransportEndpointBootstrap,
        TransportIceCandidate, TransportIceCandidateType, TransportIceParameters,
        TransportIceProtocol, TransportPublishOptions, TransportPublishOptionsByMediaKind,
        TransportSctpParameters,
    },
    runtime::transport_connect::{
        TransportConnectDtlsFingerprint, TransportConnectDtlsParameters,
        TransportConnectIceParameters,
    },
    signaling::shared::SessionId,
};

pub(super) const VALID_SDP_OFFER: &str = "v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
t=0 0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n";
pub(super) const FIREFOX_OFFER_AUDIO_ONLY: &str =
    include_str!("../testdata/firefox_offer_audio_only.sdp");
pub(super) const SAFARI_DATA_CHANNEL_OFFER: &str =
    include_str!("../testdata/safari_datachannel_offer.sdp");

pub(super) fn empty_router_capabilities() -> RouterRtpCapabilities {
    RouterRtpCapabilities::new(vec![], vec![])
}

pub(super) fn transport_key(
    channel_runtime_id: u64,
    connection_id: u64,
    session_id: SessionId,
) -> TransportSessionKey {
    transport_key_on_worker(channel_runtime_id, 0, connection_id, session_id)
}

pub(super) fn transport_key_on_worker(
    channel_runtime_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    session_id: SessionId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        channel_runtime_id,
        media_worker_id,
        connection_id,
        session_id,
    )
}

pub(super) fn sample_bootstrap_payload(
    candidate: TransportIceCandidate,
) -> SessionTransportBootstrap {
    SessionTransportBootstrap {
        router_capabilities: empty_router_capabilities(),
        download_transport: sample_transport_bootstrap("stc-rtc", candidate.clone()),
        upload_transport: sample_transport_bootstrap("cts-rtc", candidate),
        publish_options_by_media_kind: TransportPublishOptionsByMediaKind {
            audio: TransportPublishOptions {
                stop_tracks: false,
                zero_rtp_on_pause: false,
            },
            video: TransportPublishOptions {
                stop_tracks: false,
                zero_rtp_on_pause: false,
            },
        },
    }
}

pub(super) fn sample_sha256_dtls_parameters(role: &str) -> TransportConnectDtlsParameters {
    sample_sha256_dtls_parameters_with_value(
        role,
        "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
    )
}

pub(super) fn sample_sha256_dtls_parameters_with_value(
    role: &str,
    value: &str,
) -> TransportConnectDtlsParameters {
    TransportConnectDtlsParameters {
        role: role.to_owned(),
        fingerprints: vec![TransportConnectDtlsFingerprint {
            algorithm: String::from("sha-256"),
            value: value.to_owned(),
        }],
    }
}

pub(super) fn sample_candidate(protocol: TransportIceProtocol, port: u16) -> TransportIceCandidate {
    TransportIceCandidate {
        foundation: String::from("foundation"),
        priority: 2_113_937_151,
        ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
        protocol,
        port,
        candidate_type: TransportIceCandidateType::Host,
    }
}

pub(super) fn sample_transport_bootstrap(
    id: &str,
    candidate: TransportIceCandidate,
) -> TransportEndpointBootstrap {
    TransportEndpointBootstrap {
        id: id.to_owned(),
        ice_parameters: TransportIceParameters {
            username_fragment: String::from("ufrag"),
            password: String::from("pwd"),
            ice_lite: true,
        },
        ice_candidates: vec![candidate],
        dtls_parameters: TransportDtlsParameters {
            role: TransportDtlsRole::Auto,
            fingerprints: vec![TransportDtlsFingerprint {
                algorithm: TransportDtlsFingerprintAlgorithm::Sha256,
                value: String::from("AA:BB:CC"),
            }],
        },
        sctp_parameters: TransportSctpParameters {
            port: 5000,
            outgoing_streams: 1024,
            incoming_streams: 1024,
            max_message_size: 262_144,
        },
    }
}

pub(super) fn sample_ice_parameters(
    username_fragment: &str,
    password: &str,
) -> TransportConnectIceParameters {
    TransportConnectIceParameters {
        username_fragment: Some(username_fragment.to_owned()),
        password: Some(password.to_owned()),
    }
}

pub(super) fn sample_router_rtp_parameters(mid: &str, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc)],
    )
    .with_mid(mid.to_owned())
}

pub(super) fn sample_router_rtp_parameters_with_rid(
    mid: &str,
    ssrc: u32,
    rid: &str,
) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc).with_rid(rid)],
    )
    .with_mid(mid.to_owned())
}
