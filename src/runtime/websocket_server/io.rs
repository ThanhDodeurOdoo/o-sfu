use std::{future::Future, time::Duration};

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{
    SinkExt,
    stream::{SplitSink, SplitStream},
};
use o_sfu_protocol::wire::{
    ClientEnvelope, Envelope, EnvelopeBatch, EnvelopeBatchDecodeError, EnvelopeDecodeError,
    MAX_ENVELOPE_BATCH_LEN, ServerEnvelope, WebSocketCloseCode, decode_envelope_batch,
};
use tokio::time::timeout;

use crate::application::user_session::UserOutput;

pub(crate) type WsWriter = SplitSink<WebSocket, Message>;
pub(crate) type WsReader = SplitStream<WebSocket>;

pub const MAX_CLIENT_FRAME_BYTES: usize = 256 * 1024;

pub const MAX_CLIENT_BATCH_ENVELOPES: usize = MAX_ENVELOPE_BATCH_LEN;

const OUTBOUND_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientBatchDecodeFailureKind {
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientBatchDecodeError {
    FrameTooLarge { actual: usize, limit: usize },
    BatchTooLarge { actual: usize, limit: usize },
    InvalidJson,
    InvalidRoutingMetadata,
    InvalidEnvelope(EnvelopeDecodeError),
}

impl ClientBatchDecodeError {
    #[must_use]
    pub const fn kind(&self) -> ClientBatchDecodeFailureKind {
        match self {
            Self::InvalidEnvelope(EnvelopeDecodeError::UnknownTag(_)) => {
                ClientBatchDecodeFailureKind::UnsupportedFeature
            }
            Self::FrameTooLarge { .. }
            | Self::BatchTooLarge { .. }
            | Self::InvalidJson
            | Self::InvalidRoutingMetadata
            | Self::InvalidEnvelope(
                EnvelopeDecodeError::InvalidPayload(_) | EnvelopeDecodeError::UnexpectedPayload(_),
            ) => ClientBatchDecodeFailureKind::InvalidInput,
        }
    }
}

/// # Errors
///
/// Returns an error when the frame exceeds the byte limit, the batch exceeds
/// the envelope limit, the payload is not valid envelope JSON, the route
/// metadata is mixed, or any decoded envelope violates the protocol signaling
/// contract.
pub fn decode_client_batch(payload: &str) -> Result<Vec<ClientEnvelope>, ClientBatchDecodeError> {
    if payload.len() > MAX_CLIENT_FRAME_BYTES {
        return Err(ClientBatchDecodeError::FrameTooLarge {
            actual: payload.len(),
            limit: MAX_CLIENT_FRAME_BYTES,
        });
    }
    let batch =
        decode_envelope_batch(payload, MAX_CLIENT_BATCH_ENVELOPES).map_err(
            |error| match error {
                EnvelopeBatchDecodeError::InvalidJson => ClientBatchDecodeError::InvalidJson,
                EnvelopeBatchDecodeError::BatchTooLarge { actual, limit } => {
                    ClientBatchDecodeError::BatchTooLarge { actual, limit }
                }
                EnvelopeBatchDecodeError::InvalidRoutingMetadata => {
                    ClientBatchDecodeError::InvalidRoutingMetadata
                }
            },
        )?;
    batch
        .into_iter()
        .map(|envelope| {
            ClientEnvelope::decode(envelope).map_err(ClientBatchDecodeError::InvalidEnvelope)
        })
        .collect()
}

pub(super) async fn send_user_output_bounded(
    writer: &mut WsWriter,
    output: UserOutput,
) -> Result<usize, WebSocketCloseCode> {
    with_outbound_write_timeout(send_user_signals(writer, output)).await
}

pub(super) async fn send_message_bounded(
    writer: &mut WsWriter,
    message: Message,
) -> Result<(), WebSocketCloseCode> {
    with_outbound_write_timeout(async {
        writer
            .send(message)
            .await
            .map_err(|_error| WebSocketCloseCode::Error)
    })
    .await
}

pub(super) async fn close_writer_bounded(writer: &mut WsWriter, code: WebSocketCloseCode) {
    let _closed = with_outbound_write_timeout(async {
        let _result = writer
            .send(Message::Close(Some(CloseFrame {
                code: u16::from(code),
                reason: "".into(),
            })))
            .await;
        Ok(())
    })
    .await;
}

/// plain messages are batched until a synchronous request or response has to
/// cross the socket so control-flow envelopes stay in order
pub(super) async fn send_user_signals(
    writer: &mut WsWriter,
    signals: UserOutput,
) -> Result<usize, WebSocketCloseCode> {
    if signals.is_empty() {
        return Ok(0);
    }
    let mut batch_count = 0;
    let mut pending_messages = Vec::with_capacity(signals.len().min(MAX_ENVELOPE_BATCH_LEN));
    for signal in signals {
        match signal {
            ServerEnvelope::Message(_) => {
                pending_messages.push(
                    signal
                        .into_envelope()
                        .map_err(|_error| WebSocketCloseCode::Error)?,
                );
            }
            ServerEnvelope::Request { .. } | ServerEnvelope::Response { .. } => {
                batch_count += send_pending_messages(writer, &mut pending_messages).await?;
                let envelope = signal
                    .into_envelope()
                    .map_err(|_error| WebSocketCloseCode::Error)?;
                send_serialized_batch(writer, &[envelope]).await?;
                batch_count += 1;
            }
        }
    }
    batch_count += send_pending_messages(writer, &mut pending_messages).await?;
    Ok(batch_count)
}

async fn send_pending_messages(
    writer: &mut WsWriter,
    pending_messages: &mut EnvelopeBatch,
) -> Result<usize, WebSocketCloseCode> {
    if pending_messages.is_empty() {
        return Ok(0);
    }
    let mut batch_count = 0;
    for batch in pending_messages.chunks(MAX_ENVELOPE_BATCH_LEN) {
        send_serialized_batch(writer, batch).await?;
        batch_count += 1;
    }
    pending_messages.clear();
    Ok(batch_count)
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

async fn with_outbound_write_timeout<T>(
    operation: impl Future<Output = Result<T, WebSocketCloseCode>>,
) -> Result<T, WebSocketCloseCode> {
    match timeout(OUTBOUND_WRITE_TIMEOUT, operation).await {
        Ok(result) => result,
        Err(_elapsed) => Err(WebSocketCloseCode::Error),
    }
}

#[cfg(test)]
#[path = "TESTS/io.rs"]
mod tests;
