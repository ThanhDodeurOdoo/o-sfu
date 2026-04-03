use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use tracing::trace;

use crate::signaling::{
    current_bus::{CurrentBusBatch, CurrentBusEnvelope},
    current_protocol::{CurrentServerMessage, CurrentWebSocketCloseCode},
};

pub(crate) type WsWriter = SplitSink<WebSocket, Message>;

pub(crate) async fn send_server_message_batch(
    writer: &mut WsWriter,
    message: &CurrentServerMessage,
) -> Result<(), CurrentWebSocketCloseCode> {
    trace!(server_message = ?message, "encoding server message batch");
    let value = serde_json::to_value(message).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
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

pub(super) fn parse_batch(
    message: Message,
) -> Result<Option<CurrentBusBatch>, CurrentWebSocketCloseCode> {
    trace!("parsing websocket bus frame");
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| CurrentWebSocketCloseCode::Error)?,
        Message::Close(_) => return Ok(None),
        Message::Ping(_) | Message::Pong(_) => return Ok(Some(Vec::new())),
    };
    serde_json::from_str::<CurrentBusBatch>(&payload)
        .map(Some)
        .map_err(|_error| CurrentWebSocketCloseCode::Error)
}

pub(super) async fn send_batch(
    writer: &mut WsWriter,
    batch: CurrentBusBatch,
) -> Result<(), CurrentWebSocketCloseCode> {
    trace!(batch_len = batch.len(), "sending websocket bus batch");
    let payload =
        serde_json::to_string(&batch).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
    writer
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_error| CurrentWebSocketCloseCode::Error)
}
