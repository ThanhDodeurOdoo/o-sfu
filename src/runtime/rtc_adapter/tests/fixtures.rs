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
pub(super) use serde_json::json;
pub(super) use str0m::media::{MediaKind as Str0mMediaKind, Mid};
pub(super) use tokio::time::sleep;

pub(super) use super::super::{RtcTransportAdapter, packet_loop::take_write_payload, validation};
pub(super) use crate::{
    runtime::transport_adapter::{
        TransportAdapterError, TransportConnectDirection, TransportConnectRequest,
        TransportSessionKey,
    },
    signaling::{
        current_protocol::CurrentTransportBootstrapPayload,
        shared::SessionId,
        webrtc::{
            DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, PublishOptions,
            PublishOptionsByMediaKind, RtpCapabilities as WireRtpCapabilities, SctpParameters,
            TransportBootstrap,
        },
    },
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
    candidate: IceCandidate,
) -> CurrentTransportBootstrapPayload {
    CurrentTransportBootstrapPayload {
        router_capabilities: WireRtpCapabilities(json!({
            "codecs": [],
            "headerExtensions": []
        })),
        download_transport: sample_transport_bootstrap("stc-rtc", candidate.clone()),
        upload_transport: sample_transport_bootstrap("cts-rtc", candidate),
        publish_options_by_media_kind: PublishOptionsByMediaKind {
            audio: PublishOptions(json!({ "stopTracks": false })),
            video: PublishOptions(json!({ "stopTracks": false })),
        },
    }
}

pub(super) fn sample_sha256_dtls_parameters(role: &str) -> DtlsParameters {
    sample_sha256_dtls_parameters_with_value(
        role,
        "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99",
    )
}

pub(super) fn sample_sha256_dtls_parameters_with_value(role: &str, value: &str) -> DtlsParameters {
    DtlsParameters {
        role: role.to_owned(),
        fingerprints: vec![DtlsFingerprint {
            algorithm: String::from("sha-256"),
            value: value.to_owned(),
        }],
    }
}

pub(super) fn sample_candidate(protocol: &str, port: u64) -> IceCandidate {
    IceCandidate {
        foundation: String::from("foundation"),
        priority: 2_113_937_151,
        ip: String::from("203.0.113.10"),
        protocol: protocol.to_owned(),
        port,
        candidate_type: String::from("host"),
    }
}

pub(super) fn sample_transport_bootstrap(id: &str, candidate: IceCandidate) -> TransportBootstrap {
    TransportBootstrap {
        id: id.to_owned(),
        ice_parameters: IceParameters(json!({
            "usernameFragment": "ufrag",
            "password": "pwd",
            "iceLite": true
        })),
        ice_candidates: vec![candidate],
        dtls_parameters: sample_sha256_dtls_parameters("auto"),
        sctp_parameters: SctpParameters(json!({
            "port": 5000,
            "OS": 1024,
            "MIS": 1024,
            "maxMessageSize": 262_144
        })),
    }
}

pub(super) fn sample_ice_parameters(username_fragment: &str, password: &str) -> IceParameters {
    IceParameters(json!({
        "usernameFragment": username_fragment,
        "password": password,
        "iceLite": false
    }))
}

pub(super) fn sample_router_rtp_parameters(mid: &str, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc)],
    )
    .with_mid(mid.to_owned())
}
