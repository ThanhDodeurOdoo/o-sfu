use serde_json::Value;

use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusRequestId},
    current_protocol::CurrentClientMessage,
};

use super::{
    publish_request_edge::LegacyPublishTrackRequest,
    recording_request_edge::LegacyRecordingControlRequest,
    transport_connect_edge::LegacyTransportConnectRequest,
};

#[derive(Debug)]
pub(super) enum DomainCommand {
    Response {
        response_to: CurrentBusRequestId,
        payload: Value,
    },
    LegacyTransportConnect {
        request_id: CurrentBusRequestId,
        request: LegacyTransportConnectRequest,
    },
    PublishTrack {
        request_id: CurrentBusRequestId,
        request: LegacyPublishTrackRequest,
    },
    RecordingControl {
        request_id: CurrentBusRequestId,
        request: LegacyRecordingControlRequest,
    },
    InvalidRequest {
        request_id: CurrentBusRequestId,
    },
    Message(CurrentClientMessage),
    InvalidMessage,
}

pub(super) fn decode_envelope(envelope: CurrentBusEnvelope) -> DomainCommand {
    let CurrentBusEnvelope {
        message,
        need_response,
        response_to,
    } = envelope;
    match (response_to, need_response) {
        (Some(response_to), _) => DomainCommand::Response {
            response_to,
            payload: message,
        },
        (None, Some(request_id)) => {
            if let Some(result) = LegacyTransportConnectRequest::decode_wire(&message) {
                return match result {
                    Ok(request) => DomainCommand::LegacyTransportConnect {
                        request_id,
                        request,
                    },
                    Err(()) => DomainCommand::InvalidRequest { request_id },
                };
            }
            if let Some(result) = LegacyPublishTrackRequest::decode_wire(&message) {
                return match result {
                    Ok(request) => DomainCommand::PublishTrack {
                        request_id,
                        request,
                    },
                    Err(()) => DomainCommand::InvalidRequest { request_id },
                };
            }
            if let Some(result) = LegacyRecordingControlRequest::decode_wire(&message) {
                return match result {
                    Ok(request) => DomainCommand::RecordingControl {
                        request_id,
                        request,
                    },
                    Err(()) => DomainCommand::InvalidRequest { request_id },
                };
            }
            DomainCommand::InvalidRequest { request_id }
        }
        (None, None) => match serde_json::from_value::<CurrentClientMessage>(message) {
            Ok(message) => DomainCommand::Message(message),
            Err(_error) => DomainCommand::InvalidMessage,
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DomainCommand, decode_envelope};
    use crate::{
        runtime::transport_adapter::TransportConnectDirection,
        signaling::{
            current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
            protocol::RecordingOptions,
            shared::StreamType,
            webrtc::MediaKind as SignalingMediaKind,
        },
    };

    #[test]
    fn connect_request_envelope_decodes_to_semantic_domain_command() {
        let request_id = CurrentBusRequestId::new(CurrentBusOrigin::Client, 1, 2);
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": {
                    "dtlsParameters": {
                        "role": "client",
                        "fingerprints": []
                    },
                },
            }),
            need_response: Some(request_id.clone()),
            response_to: None,
        };

        let command = decode_envelope(envelope);

        assert!(matches!(
            command,
            DomainCommand::LegacyTransportConnect {
                request_id: actual_request_id,
                request,
            } if actual_request_id == request_id
                && request.direction() == TransportConnectDirection::Upload
        ));
    }

    #[test]
    fn recording_request_envelope_decodes_to_semantic_command() {
        let request_id = CurrentBusRequestId::new(CurrentBusOrigin::Client, 5, 7);
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "START_RECORDING",
                "payload": {
                    "audio": true
                }
            }),
            need_response: Some(request_id.clone()),
            response_to: None,
        };

        let command = decode_envelope(envelope);

        assert!(matches!(
            command,
            DomainCommand::RecordingControl {
                request_id: actual_request_id,
                request: super::LegacyRecordingControlRequest::Start(RecordingOptions {
                    audio: Some(true),
                    video: None,
                    transcription: None,
                }),
            } if actual_request_id == request_id
        ));
    }

    #[test]
    fn publish_request_envelope_decodes_to_semantic_publish_command() {
        let request_id = CurrentBusRequestId::new(CurrentBusOrigin::Client, 2, 3);
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "INIT_PRODUCER",
                "payload": {
                    "type": "camera",
                    "kind": "video",
                    "rtpParameters": {
                        "codecs": [{
                            "mimeType": "video/VP8",
                            "payloadType": 96,
                            "clockRate": 90000,
                            "parameters": {},
                            "rtcpFeedback": [{ "type": "transport-cc" }]
                        }],
                        "headerExtensions": [],
                        "encodings": [{ "ssrc": 11111 }]
                    }
                }
            }),
            need_response: Some(request_id.clone()),
            response_to: None,
        };

        let command = decode_envelope(envelope);

        assert!(matches!(
            command,
            DomainCommand::PublishTrack {
                request_id: actual_request_id,
                request,
            } if actual_request_id == request_id
                && request.stream_type() == StreamType::Camera
                && request.media_kind() == SignalingMediaKind::Video
        ));
    }

    #[test]
    fn invalid_request_envelope_keeps_request_identity() {
        let request_id = CurrentBusRequestId::new(CurrentBusOrigin::Client, 4, 8);
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": "wrong-shape",
            }),
            need_response: Some(request_id.clone()),
            response_to: None,
        };

        let command = decode_envelope(envelope);

        assert!(matches!(
            command,
            DomainCommand::InvalidRequest {
                request_id: actual_request_id,
            } if actual_request_id == request_id
        ));
    }

    #[test]
    fn invalid_message_envelope_becomes_invalid_message_command() {
        let envelope = CurrentBusEnvelope {
            message: json!({
                "name": "UPDATE_INFO",
                "payload": false,
            }),
            need_response: None,
            response_to: None,
        };

        assert!(matches!(
            decode_envelope(envelope),
            DomainCommand::InvalidMessage
        ));
    }
}
