use o_sfu_router::{MediaKind, RtcpFeedback, RtcpFeedbackKind, RtpCodecCapability};
use serde_json::{Map, Value, json};

use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    webrtc::{
        PublishOptions, PublishOptionsByMediaKind, RtpCapabilities, SctpParameters,
        TransportBootstrap,
    },
};

const RTCP_FEEDBACK_NACK: &str = "nack";
const RTCP_FEEDBACK_PLI: &str = "pli";
const RTCP_FEEDBACK_CCM: &str = "ccm";
const RTCP_FEEDBACK_FIR: &str = "fir";
const RTCP_FEEDBACK_GOOG_REMB: &str = "goog-remb";
const RTCP_FEEDBACK_TRANSPORT_CC: &str = "transport-cc";
const RTP_HEADER_DIRECTION_SEND_RECV: &str = "sendrecv";
const SCTP_PORT: u16 = 5_000;
const SCTP_OS: u16 = 1_024;
const SCTP_MIS: u16 = 1_024;
const SCTP_MAX_MESSAGE_SIZE: u32 = 262_144;

pub(super) fn transport_bootstrap_payload(
    router_capabilities: &o_sfu_router::RtpCapabilities,
    download_transport: TransportBootstrap,
    upload_transport: TransportBootstrap,
) -> CurrentTransportBootstrapPayload {
    CurrentTransportBootstrapPayload {
        router_capabilities: to_wire_rtp_capabilities(router_capabilities),
        download_transport,
        upload_transport,
        publish_options_by_media_kind: default_publish_options_by_media_kind(),
    }
}

pub(super) fn default_publish_options_by_media_kind() -> PublishOptionsByMediaKind {
    PublishOptionsByMediaKind {
        audio: PublishOptions(json!({
            "stopTracks": false
        })),
        video: PublishOptions(json!({
            "stopTracks": false,
            "zeroRtpOnPause": true
        })),
    }
}

pub(super) fn default_sctp_parameters() -> SctpParameters {
    SctpParameters(json!({
        "port": SCTP_PORT,
        "OS": SCTP_OS,
        "MIS": SCTP_MIS,
        "maxMessageSize": SCTP_MAX_MESSAGE_SIZE
    }))
}

pub(super) fn to_wire_rtp_capabilities(
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
