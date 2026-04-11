use o_sfu_router::{
    CodecSetting, MediaCapabilities, MediaCodecCapability, MediaKind, RtcpFeedback,
    RtcpFeedbackKind,
};
use serde_json::{Map, Value, json};

use crate::rfc::webrtc;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    webrtc::{
        PublishOptions, PublishOptionsByMediaKind, RtpCapabilities, SctpParameters,
        TransportBootstrap,
    },
};

pub(super) fn transport_bootstrap_payload(
    router_capabilities: &MediaCapabilities,
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
        "port": webrtc::data_channel::SCTP_PORT,
        "OS": webrtc::data_channel::OUTGOING_STREAMS,
        "MIS": webrtc::data_channel::INCOMING_STREAMS,
        "maxMessageSize": webrtc::data_channel::MAX_MESSAGE_SIZE
    }))
}

pub(super) fn to_wire_rtp_capabilities(router_capabilities: &MediaCapabilities) -> RtpCapabilities {
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
    if let Some(payload_type) = codec.preferred_payload_type() {
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

fn serialize_rtcp_feedback(feedback: &RtcpFeedback) -> Value {
    let (feedback_type, parameter) = rtcp_feedback_wire_parts(feedback);
    let mut feedback_json = Map::new();
    feedback_json.insert("type".to_owned(), json!(feedback_type));
    if let Some(parameter) = parameter {
        feedback_json.insert("parameter".to_owned(), json!(parameter));
    }
    Value::Object(feedback_json)
}

fn rtcp_feedback_wire_parts(feedback: &RtcpFeedback) -> (&str, Option<&str>) {
    match feedback.kind() {
        RtcpFeedbackKind::Nack => (webrtc::rtcp_feedback::kind::NACK, feedback.parameter()),
        RtcpFeedbackKind::NackPli => (
            webrtc::rtcp_feedback::kind::NACK,
            Some(webrtc::rtcp_feedback::parameter::PLI),
        ),
        RtcpFeedbackKind::CcmFir => (
            webrtc::rtcp_feedback::kind::CCM,
            Some(webrtc::rtcp_feedback::parameter::FIR),
        ),
        RtcpFeedbackKind::GoogRemb => (webrtc::rtcp_feedback::kind::GOOG_REMB, None),
        RtcpFeedbackKind::TransportCc => (webrtc::rtcp_feedback::kind::TRANSPORT_CC, None),
        RtcpFeedbackKind::Other(name) => (name.as_str(), feedback.parameter()),
    }
}

fn serialize_codec_settings<'a>(settings: impl Iterator<Item = &'a CodecSetting>) -> Value {
    Value::Object(
        settings
            .map(|setting| {
                (
                    setting.key().to_owned(),
                    json!(setting.wire_value().as_ref()),
                )
            })
            .collect(),
    )
}

fn media_kind_label(media_kind: MediaKind) -> &'static str {
    match media_kind {
        MediaKind::Audio => webrtc::media_kind::AUDIO,
        MediaKind::Video => webrtc::media_kind::VIDEO,
    }
}
