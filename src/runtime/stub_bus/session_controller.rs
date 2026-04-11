use std::sync::Arc;

use serde_json::Value;
use tracing::debug;

use super::{
    STUB_SERVER_BUS_ID, codec, codec::WsWriter, empty_object, signaling_edge::DomainCommand,
};
use crate::runtime::{
    channel::Channel,
    metrics::RuntimeMetrics,
    transport_adapter::{RuntimeTransportAdapter, TransportConnectDirection, TransportSessionKey},
};
use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackPayload,
        CurrentPublishTrackResponse, CurrentServerRequest, CurrentTransportConnectPayload,
    },
    native_protocol::NativeWebSocketCloseCode,
    shared::SessionId,
    webrtc::RtpCapabilities,
};

#[derive(Debug)]
pub(super) struct SessionController {
    session_id: SessionId,
    connection_id: u64,
    channel: Arc<Channel>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
    next_request_counter: u64,
    pending_transport_bootstrap_request_id: Option<CurrentBusRequestId>,
    pending_ping_request_id: Option<CurrentBusRequestId>,
}

impl SessionController {
    pub(super) fn new(
        session_id: SessionId,
        connection_id: u64,
        channel: Arc<Channel>,
        metrics: Arc<RuntimeMetrics>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            channel,
            metrics,
            transport_adapter,
            next_request_counter: 0,
            pending_transport_bootstrap_request_id: None,
            pending_ping_request_id: None,
        }
    }

    pub(super) fn record_batch_received(&self, batch_len: usize) {
        self.metrics.record_ws_bus_batch_received(batch_len);
    }

    pub(super) fn record_parse_failure(&self) {
        self.metrics.record_ws_bus_parse_failure();
    }

    pub(super) async fn send_transport_bootstrap(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), ()> {
        let router_capabilities = self.channel.router_rtp_capabilities().await;
        let transport_session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        let Ok(bootstrap_payload) = self
            .transport_adapter
            .transport_bootstrap_payload(&transport_session_key, &router_capabilities)
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

    pub(super) fn awaiting_ping_response(&self) -> bool {
        self.pending_ping_request_id.is_some()
    }

    pub(super) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), NativeWebSocketCloseCode> {
        if self.pending_ping_request_id.is_some() {
            return Ok(());
        }
        let request_id = self
            .send_request(writer, CurrentServerRequest::Ping)
            .await?;
        self.pending_ping_request_id = Some(request_id);
        debug!("sent websocket bus ping request");
        Ok(())
    }

    pub(super) async fn handle_command(
        &mut self,
        writer: &mut WsWriter,
        command: DomainCommand,
    ) -> Result<(), super::StubBusOutcome> {
        match command {
            DomainCommand::Response {
                response_to,
                payload,
            } => {
                self.handle_client_response_frame(response_to, payload)
                    .await
            }
            DomainCommand::Request {
                request_id,
                request,
            } => {
                self.handle_client_request_frame(writer, request_id, request)
                    .await
            }
            DomainCommand::InvalidRequest { request_id } => {
                self.handle_invalid_client_request_frame(writer, request_id)
                    .await
            }
            DomainCommand::Message(message) => self.handle_client_message_frame(message).await,
            DomainCommand::InvalidMessage => {
                self.handle_invalid_client_message_frame();
                Ok(())
            }
        }
    }

    async fn handle_client_response_frame(
        &mut self,
        response_to: CurrentBusRequestId,
        message: Value,
    ) -> Result<(), super::StubBusOutcome> {
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
        request: CurrentClientRequest,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_request();
        debug!(request_id = %request_id.as_str(), "dispatching client bus request");
        let response = self.execute_request(&request).await;
        self.send_response(writer, request_id, response)
            .await
            .map_err(|_error| super::StubBusOutcome::Break)
    }

    async fn handle_invalid_client_request_frame(
        &self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_request_decode_failure();
        debug!("failed to decode client bus request, returning empty object");
        self.send_response(writer, request_id, empty_object())
            .await
            .map_err(|_error| super::StubBusOutcome::Break)
    }

    async fn handle_client_message_frame(
        &self,
        message: CurrentClientMessage,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_message();
        debug!("dispatching client bus message");
        self.execute_message(message).await;
        Ok(())
    }

    fn handle_invalid_client_message_frame(&self) {
        self.metrics.record_ws_bus_client_message_decode_failure();
        debug!("failed to decode client bus message");
    }

    async fn handle_response(&mut self, response_to: CurrentBusRequestId, message: Value) -> bool {
        if self
            .handle_transport_bootstrap_response(&response_to, message.clone())
            .await
        {
            return true;
        }
        self.handle_ping_response(&response_to)
    }

    async fn handle_transport_bootstrap_response(
        &mut self,
        response_to: &CurrentBusRequestId,
        message: Value,
    ) -> bool {
        let is_transport_bootstrap_response = self
            .pending_transport_bootstrap_request_id
            .as_ref()
            .is_some_and(|request_id| request_id == response_to);
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

    fn handle_ping_response(&mut self, response_to: &CurrentBusRequestId) -> bool {
        let is_ping_response = self
            .pending_ping_request_id
            .as_ref()
            .is_some_and(|request_id| request_id == response_to);
        if !is_ping_response {
            return false;
        }
        self.pending_ping_request_id = None;
        debug!(
            response_to = %response_to.as_str(),
            "received websocket bus ping response"
        );
        true
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
    ) -> Result<CurrentBusRequestId, NativeWebSocketCloseCode> {
        let request_id = self.next_request_id();
        let message =
            serde_json::to_value(request).map_err(|_error| NativeWebSocketCloseCode::Error)?;
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
            Err(NativeWebSocketCloseCode::Error)
        }
    }

    async fn send_response(
        &self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> Result<(), NativeWebSocketCloseCode> {
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

    async fn execute_request(&self, request: &CurrentClientRequest) -> Value {
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

    #[allow(
        clippy::cognitive_complexity,
        reason = "transport connect + late-join bootstrap is a linear sequence that reads better in one method"
    )]
    async fn handle_transport_connect_request(
        &self,
        payload: &CurrentTransportConnectPayload,
        direction: TransportConnectDirection,
    ) -> Value {
        if self
            .transport_adapter
            .connect_transport(
                &self.transport_session_key(),
                direction,
                &payload.dtls_parameters,
                payload.ice_parameters.as_ref(),
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
        if direction == TransportConnectDirection::Download {
            self.channel
                .bootstrap_late_join_consumers(&self.session_id, &self.transport_adapter)
                .await;
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
                &self.transport_adapter,
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

    #[allow(
        clippy::cognitive_complexity,
        reason = "message dispatch is a flat compatibility match that keeps the bus controller readable"
    )]
    async fn execute_message(&self, message: CurrentClientMessage) {
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
            CurrentClientMessage::UpdateUploadState(payload) => {
                debug!(
                    stream_type = ?payload.stream_type,
                    active = payload.active,
                    "relaying production change to channel"
                );
                self.channel
                    .update_upload_state(
                        &self.session_id,
                        payload.stream_type,
                        payload.active,
                        &self.transport_adapter,
                    )
                    .await;
            }
            CurrentClientMessage::UpdateDownloadState(payload) => {
                debug!(
                    target_session = ?payload.session_id,
                    "relaying consumption change to channel"
                );
                self.channel
                    .update_download_state(
                        &self.session_id,
                        &payload.session_id,
                        &payload.states,
                        &self.transport_adapter,
                    )
                    .await;
            }
        }
    }

    fn transport_session_key(&self) -> TransportSessionKey {
        self.channel
            .transport_session_key(&self.session_id, self.connection_id)
    }
}
