use axum::extract::ws::Message;
use serde::Deserialize;
use serde_json::Value;
use tracing::trace;

use super::{
    publish_request_edge::LegacyPublishTrackRequest,
    recording_request_edge::LegacyRecordingControlRequest,
    transport_connect_edge::LegacyTransportConnectRequest,
    wire::{LegacyBatch, LegacyEnvelope, LegacyRequestId},
};
use crate::signaling::{
    protocol::WebSocketCloseCode,
    shared::{DownloadStates, JsonPayload, SessionId, SessionInfo, StreamType},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LegacyClientMessage {
    Broadcast(JsonPayload),
    UpdateSessionInfo {
        info: SessionInfo,
        need_refresh: bool,
    },
    Publish {
        stream_type: StreamType,
        active: bool,
    },
    Subscribe {
        session_id: SessionId,
        states: DownloadStates,
    },
}

#[derive(Debug)]
pub(super) enum DomainCommand {
    Response {
        response_to: LegacyRequestId,
        payload: Value,
    },
    LegacyTransportConnect {
        request_id: LegacyRequestId,
        request: LegacyTransportConnectRequest,
    },
    PublishTrack {
        request_id: LegacyRequestId,
        request: LegacyPublishTrackRequest,
    },
    RecordingControl {
        request_id: LegacyRequestId,
        request: LegacyRecordingControlRequest,
    },
    InvalidRequest {
        request_id: LegacyRequestId,
    },
    Message(LegacyClientMessage),
    InvalidMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyDownloadStateChangePayload {
    session_id: SessionId,
    states: DownloadStates,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacySessionInfoUpdatePayload {
    info: SessionInfo,
    #[serde(rename = "needRefresh", skip_serializing_if = "Option::is_none")]
    need_refresh: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyUploadStateChangePayload {
    #[serde(rename = "type")]
    stream_type: StreamType,
    active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "name", content = "payload")]
enum LegacyClientMessageWire {
    #[serde(rename = "BROADCAST")]
    Broadcast(JsonPayload),
    #[serde(rename = "CONSUMPTION_CHANGE")]
    Subscribe(LegacyDownloadStateChangePayload),
    #[serde(rename = "C_INFO_CHANGE")]
    UpdateSessionInfo(LegacySessionInfoUpdatePayload),
    #[serde(rename = "PRODUCTION_CHANGE")]
    Publish(LegacyUploadStateChangePayload),
}

impl From<LegacyClientMessageWire> for LegacyClientMessage {
    fn from(value: LegacyClientMessageWire) -> Self {
        match value {
            LegacyClientMessageWire::Broadcast(payload) => Self::Broadcast(payload),
            LegacyClientMessageWire::Subscribe(payload) => Self::Subscribe {
                session_id: payload.session_id,
                states: payload.states,
            },
            LegacyClientMessageWire::UpdateSessionInfo(payload) => Self::UpdateSessionInfo {
                info: payload.info,
                need_refresh: payload.need_refresh.unwrap_or(false),
            },
            LegacyClientMessageWire::Publish(payload) => Self::Publish {
                stream_type: payload.stream_type,
                active: payload.active,
            },
        }
    }
}

pub(super) fn decode_frame(
    message: Message,
) -> Result<Option<Vec<DomainCommand>>, WebSocketCloseCode> {
    let Some(batch) = parse_batch(message)? else {
        return Ok(None);
    };
    Ok(Some(batch.into_iter().map(decode_envelope).collect()))
}

fn parse_batch(message: Message) -> Result<Option<LegacyBatch>, WebSocketCloseCode> {
    trace!("parsing websocket bus frame");
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| WebSocketCloseCode::ProtocolError)?,
        Message::Close(_) => return Ok(None),
        Message::Ping(_) | Message::Pong(_) => return Ok(Some(Vec::new())),
    };
    serde_json::from_str::<LegacyBatch>(&payload)
        .map(Some)
        .map_err(|_error| WebSocketCloseCode::ProtocolError)
}

fn decode_envelope(envelope: LegacyEnvelope) -> DomainCommand {
    let LegacyEnvelope {
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
        (None, None) => match serde_json::from_value::<LegacyClientMessageWire>(message) {
            Ok(message) => DomainCommand::Message(message.into()),
            Err(_error) => DomainCommand::InvalidMessage,
        },
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;
    use serde_json::json;

    use super::{DomainCommand, LegacyClientMessage, decode_frame};
    use crate::{
        runtime::{stub_bus::wire::LegacyRequestId, transport_adapter::TransportConnectDirection},
        signaling::{shared::StreamType, webrtc::MediaKind as SignalingMediaKind},
    };

    fn encode_batch(batch: &serde_json::Value) -> Message {
        Message::Text(batch.to_string().into())
    }

    #[test]
    fn connect_request_frame_decodes_to_semantic_domain_command() {
        let request_id = LegacyRequestId::client(1, 2);
        let command = decode_frame(encode_batch(&json!([{
            "message": {
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": {
                    "dtlsParameters": {
                        "role": "client",
                        "fingerprints": []
                    }
                }
            },
            "needResponse": request_id.as_str(),
        }])));

        assert!(matches!(
            command,
            Ok(Some(commands))
                if matches!(
                    commands.as_slice(),
                    [DomainCommand::LegacyTransportConnect {
                        request_id: actual_request_id,
                        request,
                    }] if actual_request_id == &request_id
                        && request.direction() == TransportConnectDirection::Upload
                )
        ));
    }

    #[test]
    fn recording_request_frame_decodes_to_semantic_command() {
        let request_id = LegacyRequestId::client(5, 7);
        let command = decode_frame(encode_batch(&json!([{
            "message": {
                "name": "START_RECORDING",
                "payload": {
                    "audio": true
                }
            },
            "needResponse": request_id.as_str(),
        }])));

        assert!(matches!(
            command,
            Ok(Some(commands))
                if matches!(
                    commands.as_slice(),
                    [DomainCommand::RecordingControl {
                        request_id: actual_request_id,
                        request: super::LegacyRecordingControlRequest::Start(options),
                    }] if actual_request_id == &request_id && options.audio == Some(true)
                )
        ));
    }

    #[test]
    fn publish_request_frame_decodes_to_semantic_publish_command() {
        let request_id = LegacyRequestId::client(2, 3);
        let command = decode_frame(encode_batch(&json!([{
            "message": {
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
            },
            "needResponse": request_id.as_str(),
        }])));

        assert!(matches!(
            command,
            Ok(Some(commands))
                if matches!(
                    commands.as_slice(),
                    [DomainCommand::PublishTrack {
                        request_id: actual_request_id,
                        request,
                    }] if actual_request_id == &request_id
                        && request.stream_type() == StreamType::Camera
                        && request.media_kind() == SignalingMediaKind::Video
                )
        ));
    }

    #[test]
    fn invalid_request_frame_keeps_request_identity() {
        let request_id = LegacyRequestId::client(4, 8);
        let command = decode_frame(encode_batch(&json!([{
            "message": {
                "name": "CONNECT_CTS_TRANSPORT",
                "payload": "wrong-shape",
            },
            "needResponse": request_id.as_str(),
        }])));

        assert!(matches!(
            command,
            Ok(Some(commands))
                if matches!(
                    commands.as_slice(),
                    [DomainCommand::InvalidRequest {
                        request_id: actual_request_id,
                    }] if actual_request_id == &request_id
                )
        ));
    }

    #[test]
    fn invalid_message_frame_becomes_invalid_message_command() {
        let command = decode_frame(encode_batch(&json!([{
            "message": {
                "name": "UPDATE_INFO",
                "payload": false,
            }
        }])));

        assert!(matches!(
            command,
            Ok(Some(commands))
                if matches!(commands.as_slice(), [DomainCommand::InvalidMessage])
        ));
    }

    #[test]
    fn message_frame_decodes_to_semantic_client_message() {
        let command = decode_frame(encode_batch(&json!([{
            "message": {
                "name": "C_INFO_CHANGE",
                "payload": {
                    "info": {
                        "isTalking": true
                    },
                    "needRefresh": true
                }
            }
        }])));

        assert!(matches!(
            command,
            Ok(Some(commands))
                if matches!(
                    commands.as_slice(),
                    [DomainCommand::Message(LegacyClientMessage::UpdateSessionInfo {
                        need_refresh: true,
                        ..
                    })]
                )
        ));
    }
}
