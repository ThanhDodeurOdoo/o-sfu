use std::sync::Arc;

use axum::extract::ws::Message;
use serde_json::Value;

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
            return Ok(());
        }
        if let Some(request_id) = need_response {
            let response = self.dispatch_request(message);
            self.send_response(writer, request_id, response)
                .await
                .map_err(|_error| StubBusOutcome::Break)?;
            return Ok(());
        }
        self.dispatch_message(message).await;
        Ok(())
    }

    fn dispatch_request(&mut self, message: Value) -> Value {
        match serde_json::from_value::<CurrentClientRequest>(message) {
            Ok(
                CurrentClientRequest::ConnectUploadTransport(_)
                | CurrentClientRequest::ConnectDownloadTransport(_),
            ) => empty_object(),
            Ok(CurrentClientRequest::PublishTrack(_)) => {
                self.next_producer_counter += 1;
                match serde_json::to_value(CurrentPublishTrackResponse {
                    id: format!("stub-producer-{}", self.next_producer_counter),
                }) {
                    Ok(value) => value,
                    Err(_error) => empty_object(),
                }
            }
            Ok(CurrentClientRequest::StartRecording(_) | CurrentClientRequest::StopRecording) => {
                Value::Bool(false)
            }
            Err(_error) => empty_object(),
        }
    }

    async fn dispatch_message(&self, message: Value) {
        match serde_json::from_value::<CurrentClientMessage>(message) {
            Ok(CurrentClientMessage::Broadcast(payload)) => {
                self.channel.broadcast(&self.session_id, payload).await;
            }
            Ok(CurrentClientMessage::UpdateSessionInfo(payload)) => {
                self.channel
                    .update_session_info(
                        &self.session_id,
                        payload.info,
                        payload.need_refresh.unwrap_or(false),
                    )
                    .await;
            }
            Ok(
                CurrentClientMessage::UpdateUploadState(_)
                | CurrentClientMessage::UpdateDownloadState(_),
            ) => {}
            Err(_error) => {}
        }
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
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}
