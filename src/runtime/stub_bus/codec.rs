use std::collections::BTreeMap;

use axum::extract::ws::Message;
use futures_util::SinkExt;
use tracing::trace;

use crate::runtime::{
    channel::{ChannelEventMessage, ChannelEventRequest},
    websocket_server::WsWriter,
};
use crate::signaling::{
    bundle_api::bundle_session_info_key,
    current_bus::{CurrentBusBatch, CurrentBusEnvelope},
    current_protocol::{
        CurrentBroadcastPayload, CurrentRemoteTrackBootstrapPayload, CurrentServerMessage,
        CurrentServerRequest, CurrentSessionDeparturePayload, CurrentSessionInfoSnapshotById,
    },
    protocol::WebSocketCloseCode,
    shared::{SessionId, SessionInfo},
};

pub(crate) async fn send_server_message_batch(
    writer: &mut WsWriter,
    message: &CurrentServerMessage,
) -> Result<(), WebSocketCloseCode> {
    trace!(server_message = ?message, "encoding server message batch");
    let value = serde_json::to_value(message).map_err(|_error| WebSocketCloseCode::Error)?;
    send_batch(
        writer,
        vec![CurrentBusEnvelope {
            message: value,
            need_response: None,
            response_to: None,
        }],
    )
    .await
}

pub(crate) async fn send_server_request_batch(
    writer: &mut WsWriter,
    request: &CurrentServerRequest,
) -> Result<(), WebSocketCloseCode> {
    trace!(server_request = ?request, "encoding server request batch");
    let value = serde_json::to_value(request).map_err(|_error| WebSocketCloseCode::Error)?;
    send_batch(
        writer,
        vec![CurrentBusEnvelope {
            message: value,
            need_response: None,
            response_to: None,
        }],
    )
    .await
}

pub(crate) fn legacy_server_message(message: ChannelEventMessage) -> Option<CurrentServerMessage> {
    match message {
        ChannelEventMessage::Broadcast { sender_id, message } => {
            Some(CurrentServerMessage::Broadcast(CurrentBroadcastPayload {
                sender_id,
                message,
            }))
        }
        ChannelEventMessage::SessionJoined { .. } => None,
        ChannelEventMessage::SessionDeparted { session_id } => Some(
            CurrentServerMessage::SessionDeparted(CurrentSessionDeparturePayload { session_id }),
        ),
        ChannelEventMessage::SessionInfoChanged(snapshot) => Some(
            CurrentServerMessage::SessionInfoChanged(legacy_session_info_snapshot(snapshot)),
        ),
        ChannelEventMessage::RecordingStateChanged(state) => {
            Some(CurrentServerMessage::ChannelStateChanged(state))
        }
    }
}

pub(crate) fn legacy_server_request(request: ChannelEventRequest) -> CurrentServerRequest {
    match request {
        ChannelEventRequest::BootstrapRemoteTrack(payload) => {
            CurrentServerRequest::BootstrapRemoteTrack(CurrentRemoteTrackBootstrapPayload {
                id: payload.consumer_id(),
                media_kind: payload.media_kind(),
                source_id: payload.producer_id(),
                rtp_parameters: payload.rtp_parameters().clone(),
                session_id: payload.session_id().clone(),
                active: payload.active(),
                stream_type: payload.stream_type(),
            })
        }
    }
}

fn legacy_session_info_snapshot(
    snapshot: BTreeMap<SessionId, SessionInfo>,
) -> CurrentSessionInfoSnapshotById {
    snapshot
        .into_iter()
        .map(|(session_id, info)| (bundle_session_info_key(&session_id), info))
        .collect()
}

pub(super) fn parse_batch(message: Message) -> Result<Option<CurrentBusBatch>, WebSocketCloseCode> {
    trace!("parsing websocket bus frame");
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| WebSocketCloseCode::ProtocolError)?,
        Message::Close(_) => return Ok(None),
        Message::Ping(_) | Message::Pong(_) => return Ok(Some(Vec::new())),
    };
    serde_json::from_str::<CurrentBusBatch>(&payload)
        .map(Some)
        .map_err(|_error| WebSocketCloseCode::ProtocolError)
}

pub(super) async fn send_batch(
    writer: &mut WsWriter,
    batch: CurrentBusBatch,
) -> Result<(), WebSocketCloseCode> {
    trace!(batch_len = batch.len(), "sending websocket bus batch");
    let payload = serde_json::to_string(&batch).map_err(|_error| WebSocketCloseCode::Error)?;
    writer
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_error| WebSocketCloseCode::Error)
}
