use axum::extract::ws::Message;
use futures_util::SinkExt;
use o_sfu_protocol::signaling::{Envelope, EnvelopeBatch, ServerEnvelope, WebSocketCloseCode};

use crate::{
    application::outcomes::{CallOutcome, UserSignal},
    runtime::websocket_server::WsWriter,
};

pub(super) async fn send_call_outcome(
    writer: &mut WsWriter,
    outcome: CallOutcome,
) -> Result<usize, WebSocketCloseCode> {
    send_user_signals(writer, outcome.into_signals()).await
}

async fn send_user_signals(
    writer: &mut WsWriter,
    signals: Vec<UserSignal>,
) -> Result<usize, WebSocketCloseCode> {
    if signals.is_empty() {
        return Ok(0);
    }
    let signal_count = signals.len();
    let mut pending_messages = Vec::new();
    for signal in signals {
        match signal {
            UserSignal::Message(message) => {
                pending_messages.push(user_signal_envelope(UserSignal::Message(message))?);
            }
            UserSignal::Request {
                request_id,
                request,
            } => {
                send_pending_messages(writer, &mut pending_messages).await?;
                let envelope = user_signal_envelope(UserSignal::Request {
                    request_id,
                    request,
                })?;
                send_serialized_batch(writer, &[envelope]).await?;
            }
            UserSignal::Response {
                response_to,
                response,
            } => {
                send_pending_messages(writer, &mut pending_messages).await?;
                let envelope = user_signal_envelope(UserSignal::Response {
                    response_to,
                    response,
                })?;
                send_serialized_batch(writer, &[envelope]).await?;
            }
        }
    }
    send_pending_messages(writer, &mut pending_messages).await?;
    Ok(signal_count)
}

async fn send_pending_messages(
    writer: &mut WsWriter,
    pending_messages: &mut EnvelopeBatch,
) -> Result<(), WebSocketCloseCode> {
    if pending_messages.is_empty() {
        return Ok(());
    }
    send_serialized_batch(writer, pending_messages).await?;
    pending_messages.clear();
    Ok(())
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
    batch: &[Envelope],
) -> Result<(), WebSocketCloseCode> {
    let frame = serde_json::to_string(batch).map_err(|_error| WebSocketCloseCode::Error)?;
    writer
        .send(Message::Text(frame.into()))
        .await
        .map_err(|_error| WebSocketCloseCode::Error)
}
