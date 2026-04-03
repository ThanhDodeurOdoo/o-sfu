use std::sync::Arc;

use axum::extract::ws::Message;
use serde_json::Value;
use tracing::{debug, trace};

use super::channel::Channel;
use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackResponse,
        CurrentServerRequest, CurrentWebSocketCloseCode,
    },
    shared::SessionId,
};

mod bootstrap;
mod codec;

pub(crate) use codec::{WsWriter, send_server_message_batch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StubBusOutcome {
    Continue,
    Break,
    Close(CurrentWebSocketCloseCode),
}

#[derive(Debug)]
pub(super) struct StubBusSession {
    session_id: SessionId,
    channel: Arc<Channel>,
    next_request_counter: u64,
    next_producer_counter: u64,
}

impl StubBusSession {
    #[must_use]
    pub(super) fn new(session_id: SessionId, channel: Arc<Channel>) -> Self {
        Self {
            session_id,
            channel,
            next_request_counter: 0,
            next_producer_counter: 0,
        }
    }

    pub(super) async fn send_transport_bootstrap(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        debug!("sending stub transport bootstrap");
        self.send_request(
            writer,
            CurrentServerRequest::BootstrapTransports(bootstrap::stub_transport_bootstrap_payload()),
        )
        .await
        .map_err(|_error| ())
    }

    pub(super) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> StubBusOutcome {
        let batch = match codec::parse_batch(message) {
            Ok(Some(batch)) => batch,
            Ok(None) => return StubBusOutcome::Break,
            Err(close_code) => return StubBusOutcome::Close(close_code),
        };
        trace!(batch_len = batch.len(), "dispatching client bus batch");
        for envelope in batch {
            match self.handle_envelope(writer, envelope).await {
                Ok(()) => {}
                Err(outcome) => return outcome,
            }
        }
        StubBusOutcome::Continue
    }

    async fn handle_envelope(
        &mut self,
        writer: &mut WsWriter,
        envelope: CurrentBusEnvelope,
    ) -> Result<(), StubBusOutcome> {
        let CurrentBusEnvelope {
            message,
            need_response,
            response_to,
        } = envelope;
        if response_to.is_some() {
            debug!("ignoring client response frame");
            return Ok(());
        }
        if let Some(request_id) = need_response {
            debug!(request_id = %request_id.as_str(), "dispatching client bus request");
            let response = self.dispatch_request(message);
            self.send_response(writer, request_id, response)
                .await
                .map_err(|_error| StubBusOutcome::Break)?;
            return Ok(());
        }
        debug!("dispatching client bus message");
        self.dispatch_message(message).await;
        Ok(())
    }

    fn dispatch_request(&mut self, message: Value) -> Value {
        let Ok(request) = serde_json::from_value::<CurrentClientRequest>(message) else {
            debug!("failed to decode client bus request, returning empty object");
            return empty_object();
        };
        self.handle_request(&request)
    }

    async fn dispatch_message(&self, message: Value) {
        let Ok(message) = serde_json::from_value::<CurrentClientMessage>(message) else {
            debug!("failed to decode client bus message");
            return;
        };
        self.handle_message(message).await;
    }

    fn next_request_id(&mut self) -> CurrentBusRequestId {
        let request_id = CurrentBusRequestId::new(
            CurrentBusOrigin::Server,
            bootstrap::STUB_SERVER_BUS_ID,
            self.next_request_counter,
        );
        self.next_request_counter += 1;
        request_id
    }

    async fn send_request(
        &mut self,
        writer: &mut WsWriter,
        request: CurrentServerRequest,
    ) -> Result<(), CurrentWebSocketCloseCode> {
        let message =
            serde_json::to_value(request).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
        let batch = vec![CurrentBusEnvelope {
            message,
            need_response: Some(self.next_request_id()),
            response_to: None,
        }];
        codec::send_batch(writer, batch).await
    }

    async fn send_response(
        &self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> Result<(), CurrentWebSocketCloseCode> {
        codec::send_batch(
            writer,
            vec![CurrentBusEnvelope {
                message: response,
                need_response: None,
                response_to: Some(request_id),
            }],
        )
        .await
    }

    fn handle_request(&mut self, request: &CurrentClientRequest) -> Value {
        match request {
            CurrentClientRequest::ConnectUploadTransport(_)
            | CurrentClientRequest::ConnectDownloadTransport(_) => {
                debug!("handled stub transport connect request");
                empty_object()
            }
            CurrentClientRequest::PublishTrack(_) => self.handle_publish_request(),
            CurrentClientRequest::StartRecording(_) | CurrentClientRequest::StopRecording => {
                debug!("handled stub recording request");
                Value::Bool(false)
            }
        }
    }

    fn handle_publish_request(&mut self) -> Value {
        self.next_producer_counter += 1;
        debug!(
            producer_id = self.next_producer_counter,
            "handled stub publish request"
        );
        serde_json::to_value(CurrentPublishTrackResponse {
            id: format!("stub-producer-{}", self.next_producer_counter),
        })
        .unwrap_or_else(|_error| empty_object())
    }

    async fn handle_message(&self, message: CurrentClientMessage) {
        match message {
            CurrentClientMessage::Broadcast(payload) => {
                debug!("relaying broadcast message to channel peers");
                self.channel.broadcast(&self.session_id, payload).await;
            }
            CurrentClientMessage::UpdateSessionInfo(payload) => {
                debug!("relaying session info update to channel peers");
                self.channel
                    .update_session_info(
                        &self.session_id,
                        payload.info,
                        payload.need_refresh.unwrap_or(false),
                    )
                    .await;
            }
            CurrentClientMessage::UpdateUploadState(_)
            | CurrentClientMessage::UpdateDownloadState(_) => {
                debug!("ignoring stub upload/download state update");
            }
        }
    }
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}
