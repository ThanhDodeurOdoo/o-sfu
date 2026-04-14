use axum::extract::ws::Message;
use futures_util::SinkExt;

use crate::runtime::websocket_server::WsWriter;
use crate::signaling::protocol::{
    ClientEnvelope, EnvelopeBatch, EnvelopeDecodeError, RequestId, ServerEnvelope, ServerMessage,
    ServerRequest, ServerResponse, WebSocketCloseCode,
};

pub(in crate::runtime::websocket_server) const MAX_CLIENT_FRAME_BYTES: usize = 256 * 1024;
pub(in crate::runtime::websocket_server) const MAX_CLIENT_BATCH_ENVELOPES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::websocket_server) enum ClientBatchDecodeFailureKind {
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::websocket_server) enum ClientBatchDecodeError {
    FrameTooLarge { actual: usize, limit: usize },
    BatchTooLarge { actual: usize, limit: usize },
    InvalidJson,
    InvalidEnvelope(EnvelopeDecodeError),
}

impl ClientBatchDecodeError {
    #[must_use]
    pub(in crate::runtime::websocket_server) const fn kind(&self) -> ClientBatchDecodeFailureKind {
        match self {
            Self::InvalidEnvelope(EnvelopeDecodeError::UnknownTag(_)) => {
                ClientBatchDecodeFailureKind::UnsupportedFeature
            }
            Self::FrameTooLarge { .. }
            | Self::BatchTooLarge { .. }
            | Self::InvalidJson
            | Self::InvalidEnvelope(
                EnvelopeDecodeError::InvalidRoutingMetadata
                | EnvelopeDecodeError::InvalidPayload(_)
                | EnvelopeDecodeError::UnexpectedPayload(_),
            ) => ClientBatchDecodeFailureKind::InvalidInput,
        }
    }
}

pub(in crate::runtime::websocket_server) fn decode_client_batch(
    payload: &str,
) -> Result<Vec<ClientEnvelope>, ClientBatchDecodeError> {
    if payload.len() > MAX_CLIENT_FRAME_BYTES {
        return Err(ClientBatchDecodeError::FrameTooLarge {
            actual: payload.len(),
            limit: MAX_CLIENT_FRAME_BYTES,
        });
    }
    let batch = serde_json::from_str::<EnvelopeBatch>(payload)
        .map_err(|_error| ClientBatchDecodeError::InvalidJson)?;
    if batch.len() > MAX_CLIENT_BATCH_ENVELOPES {
        return Err(ClientBatchDecodeError::BatchTooLarge {
            actual: batch.len(),
            limit: MAX_CLIENT_BATCH_ENVELOPES,
        });
    }
    batch
        .into_iter()
        .map(|envelope| {
            ClientEnvelope::decode(envelope).map_err(ClientBatchDecodeError::InvalidEnvelope)
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_client_batch,
    };

    #[test]
    fn decode_client_batch_classifies_generated_failures() {
        let oversized_batch = serde_json::to_string(
            &(0..=MAX_CLIENT_BATCH_ENVELOPES)
                .map(|_| json!({ "t": "info", "p": {} }))
                .collect::<Vec<_>>(),
        );
        assert!(oversized_batch.is_ok());
        let Some(oversized_batch) = oversized_batch.ok() else {
            return;
        };
        let cases = [
            (
                "not-json".to_owned(),
                ClientBatchDecodeFailureKind::InvalidInput,
            ),
            (
                serde_json::to_string(&vec![json!({ "t": "not-a-real-message", "p": {} })])
                    .unwrap_or_default(),
                ClientBatchDecodeFailureKind::UnsupportedFeature,
            ),
            (
                serde_json::to_string(&vec![json!({
                    "t": "ping",
                    "q": "1",
                    "r": "2",
                })])
                .unwrap_or_default(),
                ClientBatchDecodeFailureKind::InvalidInput,
            ),
            (
                serde_json::to_string(&vec![json!({ "t": "broadcast" })]).unwrap_or_default(),
                ClientBatchDecodeFailureKind::InvalidInput,
            ),
            (oversized_batch, ClientBatchDecodeFailureKind::InvalidInput),
        ];

        for (payload, expected_kind) in cases {
            let error = decode_client_batch(&payload);
            assert!(error.is_err());
            let Some(error) = error.err() else {
                return;
            };
            assert_eq!(error.kind(), expected_kind);
        }
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
