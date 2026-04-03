use std::sync::Arc;

use axum::extract::ws::Message;
use serde_json::Value;
use tracing::{debug, trace};

use super::channel::Channel;
use crate::runtime::metrics::RuntimeMetrics;
use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackResponse,
        CurrentServerRequest, CurrentWebSocketCloseCode,
    },
    shared::SessionId,
    webrtc::RtpCapabilities,
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
    metrics: Arc<RuntimeMetrics>,
    next_request_counter: u64,
    next_producer_counter: u64,
    pending_transport_bootstrap_request_id: Option<CurrentBusRequestId>,
}

impl StubBusSession {
    #[must_use]
    pub(super) fn new(
        session_id: SessionId,
        channel: Arc<Channel>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            session_id,
            channel,
            metrics,
            next_request_counter: 0,
            next_producer_counter: 0,
            pending_transport_bootstrap_request_id: None,
        }
    }

    pub(super) async fn send_transport_bootstrap(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        let router_capabilities = self.channel.router_rtp_capabilities().await;
        debug!("sending transport bootstrap");
        let bootstrap_payload = bootstrap::transport_bootstrap_payload(&router_capabilities);
        let request_id = self
            .send_request(
                writer,
                CurrentServerRequest::BootstrapTransports(bootstrap_payload),
            )
            .await
            .map_err(|_error| ())?;
        self.pending_transport_bootstrap_request_id = Some(request_id);
        Ok(())
    }

    async fn handle_response(&mut self, response_to: CurrentBusRequestId, message: Value) -> bool {
        let is_transport_bootstrap_response = self
            .pending_transport_bootstrap_request_id
            .as_ref()
            .is_some_and(|request_id| request_id == &response_to);
        if !is_transport_bootstrap_response {
            return false;
        }
        self.pending_transport_bootstrap_request_id = None;
        let Ok(capabilities) = serde_json::from_value::<RtpCapabilities>(message) else {
            debug!(
                response_to = %response_to.as_str(),
                "failed to decode transport bootstrap response capabilities"
            );
            return true;
        };
        if self
            .channel
            .set_client_rtp_capabilities(&self.session_id, capabilities)
            .await
        {
            debug!(
                response_to = %response_to.as_str(),
                "stored client RTP capabilities from bootstrap response"
            );
        } else {
            debug!(
                response_to = %response_to.as_str(),
                "ignored bootstrap response because session is no longer active"
            );
        }
        true
    }

    fn dispatch_request(&mut self, message: Value) -> Value {
        let Ok(request) = serde_json::from_value::<CurrentClientRequest>(message) else {
            self.metrics.record_ws_bus_client_request_decode_failure();
            debug!("failed to decode client bus request, returning empty object");
            return empty_object();
        };
        self.handle_request(&request)
    }

    async fn dispatch_message(&self, message: Value) {
        let Ok(message) = serde_json::from_value::<CurrentClientMessage>(message) else {
            self.metrics.record_ws_bus_client_message_decode_failure();
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
    ) -> Result<CurrentBusRequestId, CurrentWebSocketCloseCode> {
        let request_id = self.next_request_id();
        let message =
            serde_json::to_value(request).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
        let batch = vec![CurrentBusEnvelope {
            message,
            need_response: Some(request_id.clone()),
            response_to: None,
        }];
        let result = codec::send_batch(writer, batch).await;
        if result.is_ok() {
            self.metrics.record_ws_bus_batch_sent(1);
            Ok(request_id)
        } else {
            self.metrics.record_ws_bus_send_failure();
            Err(CurrentWebSocketCloseCode::Error)
        }
    }

    async fn send_response(
        &self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> Result<(), CurrentWebSocketCloseCode> {
        let result = codec::send_batch(
            writer,
            vec![CurrentBusEnvelope {
                message: response,
                need_response: None,
                response_to: Some(request_id),
            }],
        )
        .await;
        if result.is_ok() {
            self.metrics.record_ws_bus_batch_sent(1);
        } else {
            self.metrics.record_ws_bus_send_failure();
        }
        result
    }

    pub(super) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> StubBusOutcome {
        let batch = match codec::parse_batch(message) {
            Ok(Some(batch)) => {
                self.metrics.record_ws_bus_batch_received(batch.len());
                batch
            }
            Ok(None) => return StubBusOutcome::Break,
            Err(close_code) => {
                self.metrics.record_ws_bus_parse_failure();
                return StubBusOutcome::Close(close_code);
            }
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
        match (response_to, need_response) {
            (Some(response_to), _) => {
                self.handle_client_response_frame(response_to, message)
                    .await
            }
            (None, Some(request_id)) => {
                self.handle_client_request_frame(writer, request_id, message)
                    .await
            }
            (None, None) => self.handle_client_message_frame(message).await,
        }
    }

    async fn handle_client_response_frame(
        &mut self,
        response_to: CurrentBusRequestId,
        message: Value,
    ) -> Result<(), StubBusOutcome> {
        if !self.handle_response(response_to, message).await {
            self.metrics.record_ws_bus_client_response_ignored();
            debug!("ignoring client response frame");
        }
        Ok(())
    }

    async fn handle_client_request_frame(
        &mut self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
        message: Value,
    ) -> Result<(), StubBusOutcome> {
        self.metrics.record_ws_bus_client_request();
        debug!(request_id = %request_id.as_str(), "dispatching client bus request");
        let response = self.dispatch_request(message);
        self.send_response(writer, request_id, response)
            .await
            .map_err(|_error| StubBusOutcome::Break)
    }

    async fn handle_client_message_frame(&self, message: Value) -> Result<(), StubBusOutcome> {
        self.metrics.record_ws_bus_client_message();
        debug!("dispatching client bus message");
        self.dispatch_message(message).await;
        Ok(())
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
        self.metrics.record_ws_bus_stub_publish_request();
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
