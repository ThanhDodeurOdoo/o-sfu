use axum::extract::ws::Message;
use futures_util::SinkExt;

use crate::runtime::websocket_server::WsWriter;
use crate::signaling::protocol::{
    ClientEnvelope, EnvelopeBatch, RequestId, ServerEnvelope, ServerMessage, ServerRequest,
    ServerResponse, WebSocketCloseCode,
};

pub(super) fn decode_client_batch(
    payload: &str,
) -> Result<Vec<ClientEnvelope>, WebSocketCloseCode> {
    let batch = serde_json::from_str::<EnvelopeBatch>(payload)
        .map_err(|_error| WebSocketCloseCode::ProtocolError)?;
    batch
        .into_iter()
        .map(|envelope| {
            ClientEnvelope::decode(envelope).map_err(|_error| WebSocketCloseCode::ProtocolError)
        })
        .collect()
}

pub(super) async fn send_server_messages(
    writer: &mut WsWriter,
    messages: Vec<ServerMessage>,
) -> Result<usize, WebSocketCloseCode> {
    if messages.is_empty() {
        return Ok(0);
    }
    let mut batch = Vec::with_capacity(messages.len());
    for message in messages {
        batch.push(
            ServerEnvelope::Message(message)
                .into_envelope()
                .map_err(|_error| WebSocketCloseCode::Error)?,
        );
    }
    send_serialized_batch(writer, &batch).await?;
    Ok(batch.len())
}

pub(super) async fn send_server_request(
    writer: &mut WsWriter,
    request_id: RequestId,
    request: ServerRequest,
) -> Result<(), WebSocketCloseCode> {
    let batch = vec![
        ServerEnvelope::Request {
            request_id,
            request,
        }
        .into_envelope()
        .map_err(|_error| WebSocketCloseCode::Error)?,
    ];
    send_serialized_batch(writer, &batch).await
}

pub(super) async fn send_server_response(
    writer: &mut WsWriter,
    response_to: RequestId,
    response: ServerResponse,
) -> Result<(), WebSocketCloseCode> {
    let batch = vec![
        ServerEnvelope::Response {
            response_to,
            response,
        }
        .into_envelope()
        .map_err(|_error| WebSocketCloseCode::Error)?,
    ];
    send_serialized_batch(writer, &batch).await
}

async fn send_serialized_batch(
    writer: &mut WsWriter,
    batch: &EnvelopeBatch,
) -> Result<(), WebSocketCloseCode> {
    let frame = serde_json::to_string(batch).map_err(|_error| WebSocketCloseCode::Error)?;
    writer
        .send(Message::Text(frame.into()))
        .await
        .map_err(|_error| WebSocketCloseCode::Error)
}
