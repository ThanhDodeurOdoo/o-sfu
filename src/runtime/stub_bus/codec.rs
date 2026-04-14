use std::collections::BTreeMap;

use axum::extract::ws::Message;
use futures_util::SinkExt;
use serde_json::{Value, json};
use tracing::trace;

use super::wire::{LegacyBatch, LegacyEnvelope, LegacyRequestId};
use crate::runtime::{
    channel::{ChannelEventMessage, ChannelEventRequest},
    websocket_server::WsWriter,
};
use crate::signaling::{
    bundle_api::bundle_session_info_key,
    ortc_mapper,
    protocol::WebSocketCloseCode,
    shared::{SessionId, SessionInfo},
    webrtc::RtpParameters,
};

pub(crate) async fn send_server_message_batch(
    writer: &mut WsWriter,
    message: &Value,
) -> Result<(), WebSocketCloseCode> {
    trace!(server_message = ?message, "encoding server message batch");
    send_batch(
        writer,
        vec![LegacyEnvelope {
            message: message.clone(),
            need_response: None,
            response_to: None,
        }],
    )
    .await
}

pub(crate) async fn send_server_request_batch(
    writer: &mut WsWriter,
    request: &Value,
) -> Result<(), WebSocketCloseCode> {
    trace!(server_request = ?request, "encoding server request batch");
    send_batch(
        writer,
        vec![LegacyEnvelope {
            message: request.clone(),
            need_response: None,
            response_to: None,
        }],
    )
    .await
}

pub(super) async fn send_response_value(
    writer: &mut WsWriter,
    request_id: LegacyRequestId,
    response: Value,
) -> Result<(), WebSocketCloseCode> {
    send_batch(
        writer,
        vec![LegacyEnvelope {
            message: response,
            need_response: None,
            response_to: Some(request_id),
        }],
    )
    .await
}

pub(super) async fn send_request_value(
    writer: &mut WsWriter,
    request_id: LegacyRequestId,
    message: Value,
) -> Result<(), WebSocketCloseCode> {
    send_batch(
        writer,
        vec![LegacyEnvelope {
            message,
            need_response: Some(request_id),
            response_to: None,
        }],
    )
    .await
}

pub(crate) fn legacy_server_message(message: ChannelEventMessage) -> Option<Value> {
    match message {
        ChannelEventMessage::Broadcast { sender_id, message } => Some(json!({
            "name": "BROADCAST",
            "payload": {
                "senderId": sender_id,
                "message": message,
            },
        })),
        ChannelEventMessage::SessionJoined { .. } => None,
        ChannelEventMessage::SessionDeparted { session_id } => Some(json!({
            "name": "SESSION_LEAVE",
            "payload": {
                "sessionId": session_id,
            },
        })),
        ChannelEventMessage::SessionInfoChanged(snapshot) => Some(json!({
            "name": "S_INFO_CHANGE",
            "payload": legacy_session_info_snapshot(snapshot),
        })),
        ChannelEventMessage::RecordingStateChanged(state) => Some(json!({
            "name": "CH_INFO_CHANGE",
            "payload": state,
        })),
    }
}

pub(crate) fn legacy_server_request(request: ChannelEventRequest) -> Value {
    match request {
        ChannelEventRequest::BootstrapRemoteTrack(payload) => json!({
            "name": "INIT_CONSUMER",
            "payload": {
                "id": payload.consumer_id(),
                "kind": payload.media_kind(),
                "producerId": payload.producer_id(),
                "rtpParameters": RtpParameters(ortc_mapper::serialize_rtp_parameters(
                    payload.rtp_parameters(),
                )),
                "sessionId": payload.session_id(),
                "active": payload.active(),
                "type": payload.stream_type(),
            },
        }),
    }
}

fn legacy_session_info_snapshot(
    snapshot: BTreeMap<SessionId, SessionInfo>,
) -> BTreeMap<String, SessionInfo> {
    snapshot
        .into_iter()
        .map(|(session_id, info)| (bundle_session_info_key(&session_id), info))
        .collect()
}

pub(super) async fn send_batch(
    writer: &mut WsWriter,
    batch: LegacyBatch,
) -> Result<(), WebSocketCloseCode> {
    trace!(batch_len = batch.len(), "sending websocket bus batch");
    let payload = serde_json::to_string(&batch).map_err(|_error| WebSocketCloseCode::Error)?;
    writer
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_error| WebSocketCloseCode::Error)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Error as IoError;

    use serde_json::json;

    use super::legacy_server_message;
    use crate::{
        runtime::channel::ChannelEventMessage,
        signaling::{
            current_protocol::{
                CurrentBroadcastPayload, CurrentServerMessage, CurrentSessionDeparturePayload,
            },
            shared::{RecordingState, RecordingStateUpdate, SessionId, SessionInfo},
        },
    };

    fn sample_session_info(is_talking: bool) -> SessionInfo {
        SessionInfo {
            is_talking: Some(is_talking),
            is_camera_on: Some(true),
            is_screen_sharing_on: Some(false),
            is_self_muted: Some(false),
            is_deaf: Some(false),
            is_raising_hand: Some(false),
        }
    }

    #[test]
    fn legacy_broadcast_message_value_preserves_current_wire_shape() -> serde_json::Result<()> {
        let Some(wire) = legacy_server_message(ChannelEventMessage::Broadcast {
            sender_id: SessionId::Integer(11),
            message: json!({ "text": "hello" }),
        }) else {
            return Err(serde_json::Error::io(IoError::other(
                "broadcast should serialize",
            )));
        };

        let parsed = serde_json::from_value::<CurrentServerMessage>(wire)?;
        assert_eq!(
            parsed,
            CurrentServerMessage::Broadcast(CurrentBroadcastPayload {
                sender_id: SessionId::Integer(11),
                message: json!({ "text": "hello" }),
            })
        );
        Ok(())
    }

    #[test]
    fn legacy_session_departure_value_preserves_current_wire_shape() -> serde_json::Result<()> {
        let Some(wire) = legacy_server_message(ChannelEventMessage::SessionDeparted {
            session_id: SessionId::Integer(19),
        }) else {
            return Err(serde_json::Error::io(IoError::other(
                "departure should serialize",
            )));
        };

        let parsed = serde_json::from_value::<CurrentServerMessage>(wire)?;
        assert_eq!(
            parsed,
            CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload {
                session_id: SessionId::Integer(19),
            })
        );
        Ok(())
    }

    #[test]
    fn legacy_session_info_value_preserves_current_wire_shape() -> serde_json::Result<()> {
        let Some(wire) =
            legacy_server_message(ChannelEventMessage::SessionInfoChanged(BTreeMap::from([
                (SessionId::Integer(3), sample_session_info(true)),
                (
                    SessionId::String("partner-7".to_owned()),
                    sample_session_info(false),
                ),
            ])))
        else {
            return Err(serde_json::Error::io(IoError::other(
                "session-info snapshot should serialize",
            )));
        };

        let parsed = serde_json::from_value::<CurrentServerMessage>(wire)?;
        let CurrentServerMessage::SessionInfoChanged(snapshot) = parsed else {
            return Err(serde_json::Error::io(IoError::other(
                "expected S_INFO_CHANGE",
            )));
        };
        assert_eq!(snapshot.get("3"), Some(&sample_session_info(true)));
        assert_eq!(snapshot.get("partner-7"), Some(&sample_session_info(false)));
        Ok(())
    }

    #[test]
    fn legacy_recording_state_value_preserves_current_wire_shape() -> serde_json::Result<()> {
        let Some(wire) = legacy_server_message(ChannelEventMessage::RecordingStateChanged(
            RecordingStateUpdate {
                state: RecordingState {
                    recording: Some(true),
                    audio: Some(true),
                    transcription: Some(false),
                    video: Some(false),
                },
                stop_code: None,
            },
        )) else {
            return Err(serde_json::Error::io(IoError::other(
                "recording change should serialize",
            )));
        };

        let parsed = serde_json::from_value::<CurrentServerMessage>(wire)?;
        let CurrentServerMessage::ChannelStateChanged(update) = parsed else {
            return Err(serde_json::Error::io(IoError::other(
                "expected CH_INFO_CHANGE",
            )));
        };
        assert_eq!(update.state.recording, Some(true));
        assert_eq!(update.state.audio, Some(true));
        Ok(())
    }

    #[test]
    fn session_joined_does_not_emit_legacy_wire_message() {
        assert!(
            legacy_server_message(ChannelEventMessage::SessionJoined {
                session_id: SessionId::Integer(1),
                info: sample_session_info(false),
            })
            .is_none()
        );
    }
}
