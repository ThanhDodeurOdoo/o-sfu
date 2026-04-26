use axum::extract::ws::Message;
use futures_util::SinkExt;
use o_sfu_protocol::signaling::{
    Envelope, EnvelopeBatch, RequestId, ServerEnvelope, ServerMessage, ServerRequest,
    ServerResponse, WebSocketCloseCode,
};

use crate::{application::outcomes::UserSignal, runtime::websocket_server::WsWriter};

pub(super) async fn send_server_messages(
    writer: &mut WsWriter,
    messages: Vec<ServerMessage>,
) -> Result<usize, WebSocketCloseCode> {
    let signals = messages.into_iter().map(UserSignal::from).collect();
    send_user_signals(writer, signals).await
}

pub(super) async fn send_server_request(
    writer: &mut WsWriter,
    request_id: RequestId,
    request: ServerRequest,
) -> Result<(), WebSocketCloseCode> {
    send_user_signals(writer, vec![UserSignal::request(request_id, request)])
        .await
        .map(|_sent| ())
}

pub(super) async fn send_server_response(
    writer: &mut WsWriter,
    response_to: RequestId,
    response: ServerResponse,
) -> Result<(), WebSocketCloseCode> {
    send_user_signals(writer, vec![UserSignal::response(response_to, response)])
        .await
        .map(|_sent| ())
}

async fn send_user_signals(
    writer: &mut WsWriter,
    signals: Vec<UserSignal>,
) -> Result<usize, WebSocketCloseCode> {
    if signals.is_empty() {
        return Ok(0);
    }
    let mut batch = Vec::with_capacity(signals.len());
    for signal in signals {
        batch.push(user_signal_envelope(signal)?);
    }
    send_serialized_batch(writer, &batch).await?;
    Ok(batch.len())
}

fn user_signal_envelope(signal: UserSignal) -> Result<Envelope, WebSocketCloseCode> {
    let envelope = match signal {
        UserSignal::Message(message) => Ok(ServerEnvelope::Message(message)),
        UserSignal::Request {
            request_id,
            request,
        } => Ok(ServerEnvelope::Request {
            request_id,
            request,
        }),
        UserSignal::Response {
            response_to,
            response,
        } => Ok(ServerEnvelope::Response {
            response_to,
            response,
        }),
    }?;
    envelope
        .into_envelope()
        .map_err(|_error| WebSocketCloseCode::Error)
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
