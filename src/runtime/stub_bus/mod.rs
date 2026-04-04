use std::sync::{Arc, Mutex};

use axum::extract::ws::Message;
use serde_json::Value;
use tracing::{debug, trace};

use super::channel::Channel;
use crate::runtime::{
    metrics::RuntimeMetrics,
    transport_adapter::{
        RuntimeTransportAdapter, TransportAdapterError, TransportConnectDirection,
    },
};
use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackPayload,
        CurrentPublishTrackResponse, CurrentServerRequest, CurrentTransportBootstrapPayload,
        CurrentTransportConnectPayload, CurrentWebSocketCloseCode,
    },
    shared::SessionId,
    webrtc::{DtlsParameters, RtpCapabilities},
};

mod bootstrap;
mod codec;

pub(crate) use codec::{WsWriter, send_server_message_batch, send_server_request_batch};

const STUB_SERVER_BUS_ID: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StubBusOutcome {
    Continue,
    Break,
    Close(CurrentWebSocketCloseCode),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StubWebRtcEvent {
    BootstrapRequested,
    SessionClosed {
        session_id: SessionId,
    },
    TransportConnectRequested {
        session_id: SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: DtlsParameters,
    },
    TransportConnected {
        session_id: SessionId,
        direction: TransportConnectDirection,
    },
    TransportConnectRejected {
        session_id: SessionId,
        direction: TransportConnectDirection,
    },
}

#[derive(Debug, Clone, Default)]
pub(super) struct StubWebRtcAdapter {
    events: Arc<Mutex<Vec<StubWebRtcEvent>>>,
}

#[allow(
    clippy::unused_async,
    reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
)]
impl StubWebRtcAdapter {
    fn record_event(&self, event: StubWebRtcEvent) {
        match self.events.lock() {
            Ok(mut events) => {
                events.push(event);
            }
            Err(poisoned) => {
                poisoned.into_inner().push(event);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_events(&self) -> Vec<StubWebRtcEvent> {
        match self.events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl StubWebRtcAdapter {
    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn transport_bootstrap_payload(
        &self,
        _session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        self.record_event(StubWebRtcEvent::BootstrapRequested);
        Ok(bootstrap::transport_bootstrap_payload(router_capabilities))
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        _sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::TransportConnectRequested {
            session_id: session_id.clone(),
            direction,
            dtls_parameters: dtls_parameters.clone(),
        });
        if dtls_parameters.role.is_empty() || dtls_parameters.fingerprints.is_empty() {
            self.record_event(StubWebRtcEvent::TransportConnectRejected {
                session_id: session_id.clone(),
                direction,
            });
            return Err(TransportAdapterError::TransportUnavailable);
        }
        self.record_event(StubWebRtcEvent::TransportConnected {
            session_id: session_id.clone(),
            direction,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn close_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::SessionClosed {
            session_id: session_id.clone(),
        });
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct StubBusSession {
    session_id: SessionId,
    channel: Arc<Channel>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
    next_request_counter: u64,
    pending_transport_bootstrap_request_id: Option<CurrentBusRequestId>,
}

impl StubBusSession {
    #[must_use]
    pub(super) fn new(
        session_id: SessionId,
        channel: Arc<Channel>,
        metrics: Arc<RuntimeMetrics>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self {
            session_id,
            channel,
            metrics,
            transport_adapter,
            next_request_counter: 0,
            pending_transport_bootstrap_request_id: None,
        }
    }

    pub(super) async fn send_transport_bootstrap(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        let router_capabilities = self.channel.router_rtp_capabilities().await;
        let Ok(bootstrap_payload) = self
            .transport_adapter
            .transport_bootstrap_payload(&self.session_id, &router_capabilities)
            .await
        else {
            return Err(());
        };
        debug!("sending transport bootstrap");
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

    async fn dispatch_request(&self, message: Value) -> Value {
        let Ok(request) = serde_json::from_value::<CurrentClientRequest>(message) else {
            self.metrics.record_ws_bus_client_request_decode_failure();
            debug!("failed to decode client bus request, returning empty object");
            return empty_object();
        };
        self.handle_request(&request).await
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
            STUB_SERVER_BUS_ID,
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
        &self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
        message: Value,
    ) -> Result<(), StubBusOutcome> {
        self.metrics.record_ws_bus_client_request();
        debug!(request_id = %request_id.as_str(), "dispatching client bus request");
        let response = self.dispatch_request(message).await;
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

    async fn handle_request(&self, request: &CurrentClientRequest) -> Value {
        match request {
            CurrentClientRequest::ConnectUploadTransport(payload) => {
                self.handle_transport_connect_request(payload, TransportConnectDirection::Upload)
                    .await
            }
            CurrentClientRequest::ConnectDownloadTransport(payload) => {
                self.handle_transport_connect_request(payload, TransportConnectDirection::Download)
                    .await
            }
            CurrentClientRequest::PublishTrack(payload) => {
                self.handle_publish_request(payload).await
            }
            CurrentClientRequest::StartRecording(_) | CurrentClientRequest::StopRecording => {
                debug!("handled stub recording request");
                Value::Bool(false)
            }
        }
    }

    async fn handle_transport_connect_request(
        &self,
        payload: &CurrentTransportConnectPayload,
        direction: TransportConnectDirection,
    ) -> Value {
        if self
            .transport_adapter
            .connect_transport(
                &self.session_id,
                direction,
                &payload.dtls_parameters,
                payload.sdp_offer.as_deref(),
            )
            .await
            .is_err()
        {
            debug!(?direction, "transport adapter failed to connect transport");
            return empty_object();
        }
        if !self
            .channel
            .set_transport_connected(&self.session_id, direction)
            .await
        {
            debug!(
                ?direction,
                "channel no longer tracks session during transport connect"
            );
            return empty_object();
        }
        debug!(?direction, "handled transport connect request");
        empty_object()
    }

    async fn handle_publish_request(&self, payload: &CurrentPublishTrackPayload) -> Value {
        self.metrics.record_ws_bus_stub_publish_request();
        let producer_id = self
            .channel
            .publish_track(
                &self.session_id,
                payload.stream_type,
                payload.media_kind,
                payload.rtp_parameters.clone(),
            )
            .await;
        let Some(producer_id) = producer_id else {
            debug!("channel rejected publish request");
            return empty_object();
        };
        debug!(producer_id, "handled publish request");
        serde_json::to_value(CurrentPublishTrackResponse { id: producer_id })
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
