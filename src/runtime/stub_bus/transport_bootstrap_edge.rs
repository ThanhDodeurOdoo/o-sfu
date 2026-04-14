use o_sfu_router::{MediaCapabilities, MediaCodecCapability, MediaKind};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{
    rfc::webrtc,
    runtime::transport_bootstrap::{
        SessionTransportBootstrap, TransportDtlsFingerprint, TransportEndpointBootstrap,
        TransportIceCandidate, TransportPublishOptions, TransportPublishOptionsByMediaKind,
        TransportSctpParameters,
    },
    signaling::webrtc::{
        DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, PublishOptions,
        PublishOptionsByMediaKind, RtpCapabilities, SctpParameters, TransportBootstrap,
        serialize_codec_settings, serialize_rtcp_feedback,
    },
};

const LEGACY_TRANSPORT_BOOTSTRAP_REQUEST_NAME: &str = "INIT_TRANSPORTS";

pub(super) fn request_value(
    payload: &SessionTransportBootstrap,
) -> Result<Value, serde_json::Error> {
    serde_json::to_value(LegacyTransportBootstrapRequest {
        name: LEGACY_TRANSPORT_BOOTSTRAP_REQUEST_NAME,
        payload: legacy_transport_bootstrap_payload(payload),
    })
}

fn legacy_transport_bootstrap_payload(
    payload: &SessionTransportBootstrap,
) -> LegacyTransportBootstrapPayload {
    LegacyTransportBootstrapPayload {
        router_capabilities: to_wire_rtp_capabilities(&payload.router_capabilities),
        download_transport: legacy_transport_bootstrap(&payload.download_transport),
        upload_transport: legacy_transport_bootstrap(&payload.upload_transport),
        publish_options_by_media_kind: legacy_publish_options_by_media_kind(
            &payload.publish_options_by_media_kind,
        ),
    }
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

#[derive(Serialize)]
struct LegacyTransportBootstrapRequest {
    name: &'static str,
    payload: LegacyTransportBootstrapPayload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyTransportBootstrapPayload {
    #[serde(rename = "capabilities")]
    router_capabilities: RtpCapabilities,
    #[serde(rename = "stcConfig")]
    download_transport: TransportBootstrap,
    #[serde(rename = "ctsConfig")]
    upload_transport: TransportBootstrap,
    #[serde(rename = "producerOptionsByKind")]
    publish_options_by_media_kind: PublishOptionsByMediaKind,
}

#[cfg(test)]
mod tests {
    use std::io::Error as IoError;

    use serde_json::json;

    use super::request_value;
    use crate::{runtime::stub_bus::bootstrap, signaling::current_protocol::CurrentServerRequest};

    #[test]
    fn transport_bootstrap_request_value_uses_legacy_wire_shape() -> serde_json::Result<()> {
        let request = serde_json::from_value::<CurrentServerRequest>(request_value(
            &bootstrap::transport_bootstrap_payload(&o_sfu_router::RtpCapabilities::default()),
        )?)?;

        let payload = match request {
            CurrentServerRequest::BootstrapTransports(payload) => payload,
            _unexpected => {
                return Err(serde_json::Error::io(IoError::other(
                    "expected INIT_TRANSPORTS request",
                )));
            }
        };
        assert_eq!(payload.download_transport.id, "stc-stub");
        assert_eq!(payload.upload_transport.id, "cts-stub");
        assert_eq!(
            payload.download_transport.ice_parameters.0.get("iceLite"),
            Some(&json!(true))
        );
        assert_eq!(
            payload
                .publish_options_by_media_kind
                .video
                .0
                .get("zeroRtpOnPause"),
            Some(&json!(true))
        );
        Ok(())
    }
}
