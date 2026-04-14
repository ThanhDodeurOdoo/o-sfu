use std::net::{IpAddr, Ipv4Addr};

use o_sfu_router::{MediaCapabilities, MediaCodecCapability, MediaKind};
use serde_json::{Map, Value, json};

use crate::{
    rfc::webrtc,
    runtime::transport_bootstrap::{
        SessionTransportBootstrap, TransportDtlsFingerprint, TransportDtlsFingerprintAlgorithm,
        TransportDtlsParameters, TransportDtlsRole, TransportEndpointBootstrap,
        TransportIceCandidate, TransportIceCandidateType, TransportIceParameters,
        TransportIceProtocol, TransportPublishOptions, TransportPublishOptionsByMediaKind,
        TransportSctpParameters,
    },
    signaling::{
        current_protocol::CurrentTransportBootstrapPayload,
        webrtc::{
            DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, PublishOptions,
            PublishOptionsByMediaKind, RtpCapabilities, SctpParameters, TransportBootstrap,
            serialize_codec_settings, serialize_rtcp_feedback,
        },
    },
};

const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

pub(super) fn transport_bootstrap_payload(
    router_capabilities: &o_sfu_router::RtpCapabilities,
) -> SessionTransportBootstrap {
    SessionTransportBootstrap::new(
        router_capabilities,
        stub_transport_bootstrap(STUB_STC_TRANSPORT_ID),
        stub_transport_bootstrap(STUB_CTS_TRANSPORT_ID),
    )
}

pub(super) fn legacy_transport_bootstrap_payload(
    payload: &SessionTransportBootstrap,
) -> CurrentTransportBootstrapPayload {
    CurrentTransportBootstrapPayload {
        router_capabilities: to_wire_rtp_capabilities(&payload.router_capabilities),
        download_transport: legacy_transport_bootstrap(&payload.download_transport),
        upload_transport: legacy_transport_bootstrap(&payload.upload_transport),
        publish_options_by_media_kind: legacy_publish_options_by_media_kind(
            &payload.publish_options_by_media_kind,
        ),
    }
}

fn stub_transport_bootstrap(id: &str) -> TransportEndpointBootstrap {
    TransportEndpointBootstrap {
        id: id.to_owned(),
        ice_parameters: TransportIceParameters {
            username_fragment: String::from("ufrag"),
            password: String::from("pwd"),
            ice_lite: true,
        },
        ice_candidates: vec![TransportIceCandidate {
            foundation: String::from("foundation"),
            priority: 1,
            ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            protocol: TransportIceProtocol::Udp,
            port: 40_000,
            candidate_type: TransportIceCandidateType::Host,
        }],
        dtls_parameters: TransportDtlsParameters {
            role: TransportDtlsRole::Auto,
            fingerprints: vec![TransportDtlsFingerprint {
                algorithm: TransportDtlsFingerprintAlgorithm::Sha256,
                value: String::from("AA:BB:CC"),
            }],
        },
        sctp_parameters: default_sctp_parameters(),
    }
}

fn default_sctp_parameters() -> TransportSctpParameters {
    super::super::transport_bootstrap::default_sctp_parameters()
}

fn legacy_transport_bootstrap(payload: &TransportEndpointBootstrap) -> TransportBootstrap {
    TransportBootstrap {
        id: payload.id.clone(),
        ice_parameters: IceParameters(json!({
            "usernameFragment": payload.ice_parameters.username_fragment,
            "password": payload.ice_parameters.password,
            "iceLite": payload.ice_parameters.ice_lite
        })),
        ice_candidates: payload
            .ice_candidates
            .iter()
            .map(legacy_ice_candidate)
            .collect(),
        dtls_parameters: DtlsParameters {
            role: payload.dtls_parameters.role.as_str().to_owned(),
            fingerprints: payload
                .dtls_parameters
                .fingerprints
                .iter()
                .map(legacy_dtls_fingerprint)
                .collect(),
        },
        sctp_parameters: legacy_sctp_parameters(&payload.sctp_parameters),
    }
}

fn legacy_ice_candidate(candidate: &TransportIceCandidate) -> IceCandidate {
    IceCandidate {
        foundation: candidate.foundation.clone(),
        priority: candidate.priority,
        ip: candidate.ip.to_string(),
        protocol: candidate.protocol.as_str().to_owned(),
        port: u64::from(candidate.port),
        candidate_type: candidate.candidate_type.as_str().to_owned(),
    }
}

fn legacy_dtls_fingerprint(fingerprint: &TransportDtlsFingerprint) -> DtlsFingerprint {
    DtlsFingerprint {
        algorithm: fingerprint.algorithm.as_str().to_owned(),
        value: fingerprint.value.clone(),
    }
}

fn legacy_sctp_parameters(parameters: &TransportSctpParameters) -> SctpParameters {
    SctpParameters(json!({
        "port": parameters.port,
        "OS": parameters.outgoing_streams,
        "MIS": parameters.incoming_streams,
        "maxMessageSize": parameters.max_message_size
    }))
}

fn legacy_publish_options_by_media_kind(
    options: &TransportPublishOptionsByMediaKind,
) -> PublishOptionsByMediaKind {
    PublishOptionsByMediaKind {
        audio: legacy_publish_options(options.audio),
        video: legacy_publish_options(options.video),
    }
}

fn legacy_publish_options(options: TransportPublishOptions) -> PublishOptions {
    let mut payload = Map::new();
    payload.insert("stopTracks".to_owned(), json!(options.stop_tracks));
    if options.zero_rtp_on_pause {
        payload.insert(
            "zeroRtpOnPause".to_owned(),
            json!(options.zero_rtp_on_pause),
        );
    }
    PublishOptions(Value::Object(payload))
}

fn to_wire_rtp_capabilities(router_capabilities: &MediaCapabilities) -> RtpCapabilities {
    let codecs = router_capabilities
        .codecs()
        .map(serialize_codec_capability)
        .collect::<Vec<_>>();
    let header_extensions = router_capabilities
        .header_extensions()
        .flat_map(serialize_header_extensions)
        .collect::<Vec<_>>();
    RtpCapabilities(json!({
        "codecs": codecs,
        "headerExtensions": header_extensions,
    }))
}

fn serialize_codec_capability(codec: &MediaCodecCapability) -> Value {
    let kind = media_kind_label(codec.media_kind());
    let mut codec_json = Map::new();
    codec_json.insert("kind".to_owned(), json!(kind));
    codec_json.insert(
        "mimeType".to_owned(),
        json!(format!("{kind}/{}", codec.codec().as_str())),
    );
    codec_json.insert("clockRate".to_owned(), json!(codec.clock_rate()));
    if let Some(payload_type) = codec.payload_type() {
        codec_json.insert("preferredPayloadType".to_owned(), json!(payload_type));
    }
    if let Some(channels) = codec.channels() {
        codec_json.insert("channels".to_owned(), json!(channels));
    }
    codec_json.insert(
        "parameters".to_owned(),
        serialize_codec_settings(codec.settings()),
    );
    codec_json.insert(
        "rtcpFeedback".to_owned(),
        Value::Array(codec.rtcp_feedback().map(serialize_rtcp_feedback).collect()),
    );
    Value::Object(codec_json)
}

fn serialize_header_extensions(header_extension: &o_sfu_router::RtpHeaderExtension) -> [Value; 2] {
    [
        serialize_header_extension(MediaKind::Audio, header_extension),
        serialize_header_extension(MediaKind::Video, header_extension),
    ]
}

fn serialize_header_extension(
    media_kind: MediaKind,
    header_extension: &o_sfu_router::RtpHeaderExtension,
) -> Value {
    json!({
        "kind": media_kind_label(media_kind),
        "uri": header_extension.uri_kind().as_str(),
        "preferredId": header_extension.id().value(),
        "preferredEncrypt": header_extension.encrypt(),
        "direction": webrtc::sdp::direction::SEND_RECV,
    })
}

fn media_kind_label(media_kind: MediaKind) -> &'static str {
    match media_kind {
        MediaKind::Audio => webrtc::media_kind::AUDIO,
        MediaKind::Video => webrtc::media_kind::VIDEO,
    }
}
