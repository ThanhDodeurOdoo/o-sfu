//! websocket socket I/O boundary
//!
//! this module owns client frame decoding and the shared outbound write budget
//! so handshake startup and steady-state output cannot drift into different
//! backpressure behavior

use std::{future::Future, time::Duration};

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, stream::SplitSink};
use o_sfu_protocol::wire::{
    ClientEnvelope, Envelope, EnvelopeBatch, EnvelopeBatchDecodeError, EnvelopeDecodeError,
    ServerEnvelope, WebSocketCloseCode, decode_envelope_batch,
};
use tokio::time::timeout;

use crate::application::user_session::{UserOutput, UserSignal};

pub(crate) type WsWriter = SplitSink<WebSocket, Message>;

/// maximum accepted client text frame size before protocol rejection
pub const MAX_CLIENT_FRAME_BYTES: usize = 256 * 1024;

/// maximum envelopes accepted from one client websocket frame
pub const MAX_CLIENT_BATCH_ENVELOPES: usize = 64;

/// maximum time one outbound websocket operation may hold the task
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

pub(super) async fn send_user_output(
    writer: &mut WsWriter,
    output: UserOutput,
) -> Result<usize, WebSocketCloseCode> {
    send_user_signals(writer, output.into_signals()).await
}

/// flushes startup or loop output under the websocket writer budget
///
/// callers use this after admission so a stalled socket cannot keep room
/// membership alive without making forward progress
pub(super) async fn send_user_output_bounded(
    writer: &mut WsWriter,
    output: UserOutput,
) -> Result<usize, WebSocketCloseCode> {
    with_outbound_write_timeout(send_user_output(writer, output)).await
}

/// writes a protocol-level websocket message under the shared backpressure budget
///
/// this is for keepalive and control frames that must not wait behind a stalled
/// peer indefinitely
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

/// performs best-effort websocket close without its own timeout
///
/// callers use this when they already own the cancellation boundary or are
/// leaving a session after the read half finished
pub(crate) async fn close_writer(writer: &mut WsWriter, close_code: WebSocketCloseCode) {
    let _result = writer
        .send(Message::Close(Some(CloseFrame {
            code: u16::from(close_code),
            reason: "".into(),
        })))
        .await;
}

/// performs best-effort websocket close under the shared writer budget
///
/// this is for rejection and loop paths where a slow peer must not keep the
/// task alive
pub(super) async fn close_writer_bounded(writer: &mut WsWriter, code: WebSocketCloseCode) {
    let _closed = with_outbound_write_timeout(async {
        close_writer(writer, code).await;
        Ok(())
    })
    .await;
}

/// serializes user-facing signals while preserving request and response ordering
///
/// plain messages are batched until a synchronous request or response has to
/// cross the socket so control-flow envelopes stay in order
pub(super) async fn send_user_signals(
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
        UserSignal::Message(message) => ServerEnvelope::Message(message),
        UserSignal::Request {
            request_id,
            request,
        } => ServerEnvelope::Request {
            request_id,
            request,
        },
        UserSignal::Response {
            response_to,
            response,
        } => ServerEnvelope::Response {
            response_to,
            response,
        },
    };
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

/// applies the websocket writer budget to one outbound operation
///
/// elapsed writes are reported as protocol errors because the transport can no
/// longer make forward progress
async fn with_outbound_write_timeout<T>(
    operation: impl Future<Output = Result<T, WebSocketCloseCode>>,
) -> Result<T, WebSocketCloseCode> {
    match timeout(OUTBOUND_WRITE_TIMEOUT, operation).await {
        Ok(result) => result,
        Err(_elapsed) => Err(WebSocketCloseCode::Error),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_client_batch,
    };

    #[test]
    fn decode_client_batch_classifies_generated_failures() -> serde_json::Result<()> {
        let oversized_batch = serde_json::to_string(
            &(0..=MAX_CLIENT_BATCH_ENVELOPES)
                .map(|_| json!({ "t": "info", "p": {} }))
                .collect::<Vec<_>>(),
        )?;
        let cases = [
            (
                "not-json".to_owned(),
                ClientBatchDecodeFailureKind::InvalidInput,
            ),
            (
                serde_json::to_string(&[json!({ "t": "not-a-real-message", "p": {} })])?,
                ClientBatchDecodeFailureKind::UnsupportedFeature,
            ),
            (
                serde_json::to_string(&[json!({
                    "t": "ping",
                    "q": "1",
                    "r": "2",
                })])?,
                ClientBatchDecodeFailureKind::InvalidInput,
            ),
            (
                serde_json::to_string(&[json!({ "t": "broadcast" })])?,
                ClientBatchDecodeFailureKind::InvalidInput,
            ),
            (oversized_batch, ClientBatchDecodeFailureKind::InvalidInput),
        ];

        for (payload, expected_kind) in cases {
            assert_eq!(
                decode_client_batch(&payload)
                    .err()
                    .map(|error| error.kind()),
                Some(expected_kind)
            );
        }
        Ok(())
    }

    #[test]
    fn decode_client_batch_rejects_oversized_batch_before_routing_metadata()
    -> serde_json::Result<()> {
        let oversized_batch = serde_json::to_string(
            &(0..=MAX_CLIENT_BATCH_ENVELOPES)
                .map(|_| json!({ "t": "info", "p": {}, "q": "1", "r": "2" }))
                .collect::<Vec<_>>(),
        )?;

        assert_eq!(
            decode_client_batch(&oversized_batch),
            Err(ClientBatchDecodeError::BatchTooLarge {
                actual: MAX_CLIENT_BATCH_ENVELOPES + 1,
                limit: MAX_CLIENT_BATCH_ENVELOPES,
            })
        );
        Ok(())
    }

    #[test]
    fn decode_client_batch_rejects_oversized_frame_before_json_decode() {
        let oversized_payload = "x".repeat(MAX_CLIENT_FRAME_BYTES + 1);

        let error = decode_client_batch(&oversized_payload);
        assert!(matches!(
            error,
            Err(ClientBatchDecodeError::FrameTooLarge {
                actual,
                limit: MAX_CLIENT_FRAME_BYTES,
            }) if actual == MAX_CLIENT_FRAME_BYTES + 1
        ));
    }
}
