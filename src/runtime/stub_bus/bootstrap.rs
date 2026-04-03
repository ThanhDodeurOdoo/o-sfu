use o_sfu_router::{MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCodecCapability};
use serde_json::{Map, Value, json};

use crate::rfc::webrtc;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    webrtc::{
        DtlsParameters, IceCandidate, IceParameters, PublishOptions, PublishOptionsByMediaKind,
        RtpCapabilities, SctpParameters, TransportBootstrap,
    },
};

const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

const RTCP_FEEDBACK_NACK: &str = "nack";
const RTCP_FEEDBACK_PLI: &str = "pli";
const RTCP_FEEDBACK_CCM: &str = "ccm";
const RTCP_FEEDBACK_FIR: &str = "fir";
const RTCP_FEEDBACK_GOOG_REMB: &str = "goog-remb";
const RTCP_FEEDBACK_TRANSPORT_CC: &str = "transport-cc";
const RTP_HEADER_DIRECTION_SEND_RECV: &str = "sendrecv";

pub(super) fn transport_bootstrap_payload(
    router_capabilities: &o_sfu_router::RtpCapabilities,
) -> CurrentTransportBootstrapPayload {
    CurrentTransportBootstrapPayload {
        router_capabilities: to_wire_rtp_capabilities(router_capabilities),
        download_transport: stub_transport_bootstrap(STUB_STC_TRANSPORT_ID),
        upload_transport: stub_transport_bootstrap(STUB_CTS_TRANSPORT_ID),
        publish_options_by_media_kind: PublishOptionsByMediaKind {
            audio: PublishOptions(json!({
                "stopTracks": false
            })),
            video: PublishOptions(json!({
                "stopTracks": false,
                "zeroRtpOnPause": true
            })),
        },
    }
}

fn to_wire_rtp_capabilities(
    router_capabilities: &o_sfu_router::RtpCapabilities,
) -> RtpCapabilities {
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

fn serialize_codec_capability(codec: &RtpCodecCapability) -> Value {
    let kind = media_kind_label(codec.media_kind());
    let mut codec_json = Map::new();
    codec_json.insert("kind".to_owned(), json!(kind));
    codec_json.insert(
        "mimeType".to_owned(),
        json!(format!("{kind}/{}", codec.codec_name())),
    );
    codec_json.insert("clockRate".to_owned(), json!(codec.clock_rate()));
    if let Some(payload_type) = codec.preferred_payload_type() {
        codec_json.insert("preferredPayloadType".to_owned(), json!(payload_type));
    }
    if let Some(channels) = codec.channels() {
        codec_json.insert("channels".to_owned(), json!(channels));
    }
    codec_json.insert(
        "parameters".to_owned(),
        Value::Object(
            codec
                .parameters()
                .map(|(key, value)| (key.to_owned(), json!(value)))
                .collect(),
        ),
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
        "uri": header_extension.uri(),
        "preferredId": header_extension.id(),
        "preferredEncrypt": header_extension.encrypt(),
        "direction": RTP_HEADER_DIRECTION_SEND_RECV,
    })
}

fn serialize_rtcp_feedback(feedback: &RtcpFeedback) -> Value {
    let (feedback_type, parameter) = rtcp_feedback_wire_parts(feedback);
    let mut feedback_json = Map::new();
    feedback_json.insert("type".to_owned(), Value::String(feedback_type));
    if let Some(parameter) = parameter {
        feedback_json.insert("parameter".to_owned(), Value::String(parameter));
    }
    Value::Object(feedback_json)
}

fn rtcp_feedback_wire_parts(feedback: &RtcpFeedback) -> (String, Option<String>) {
    match feedback.kind() {
        RtcpFeedbackKind::Nack => (
            RTCP_FEEDBACK_NACK.to_owned(),
            feedback.parameter().map(str::to_owned),
        ),
        RtcpFeedbackKind::NackPli => (
            RTCP_FEEDBACK_NACK.to_owned(),
            Some(RTCP_FEEDBACK_PLI.to_owned()),
        ),
        RtcpFeedbackKind::CcmFir => (
            RTCP_FEEDBACK_CCM.to_owned(),
            Some(RTCP_FEEDBACK_FIR.to_owned()),
        ),
        RtcpFeedbackKind::GoogRemb => (RTCP_FEEDBACK_GOOG_REMB.to_owned(), None),
        RtcpFeedbackKind::TransportCc => (RTCP_FEEDBACK_TRANSPORT_CC.to_owned(), None),
        RtcpFeedbackKind::Other(name) => (name.clone(), feedback.parameter().map(str::to_owned)),
    }
}

fn media_kind_label(media_kind: MediaKind) -> &'static str {
    match media_kind {
        MediaKind::Audio => "audio",
        MediaKind::Video => "video",
    }
}

fn stub_transport_bootstrap(id: &str) -> TransportBootstrap {
    TransportBootstrap {
        id: id.to_owned(),
        ice_parameters: IceParameters(json!({
            "usernameFragment": "ufrag",
            "password": "pwd",
            "iceLite": true
        })),
        ice_candidates: vec![IceCandidate(json!({
            "foundation": "foundation",
            "priority": 1,
            "ip": "203.0.113.10",
            "protocol": webrtc::ICE_TRANSPORT_UDP,
            "port": 40000,
            "type": webrtc::ICE_CANDIDATE_TYPE_HOST
        }))],
        dtls_parameters: DtlsParameters(json!({
            "role": "auto",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": "AA:BB:CC"
            }]
        })),
        sctp_parameters: SctpParameters(json!({
            "port": 5000,
            "OS": 1024,
            "MIS": 1024,
            "maxMessageSize": 262_144
        })),
    }
}
