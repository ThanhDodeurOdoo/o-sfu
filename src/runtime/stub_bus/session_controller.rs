use std::sync::Arc;

use serde_json::{Value, json};
use tracing::debug;

use super::{
    STUB_SERVER_BUS_ID, codec, empty_object,
    signaling_edge::{DomainCommand, LegacyClientMessage},
    transport_bootstrap_edge,
    wire::LegacyRequestId,
};
use crate::runtime::{
    channel::Channel,
    metrics::RuntimeMetrics,
    rtc_adapter::TransportSessionHealth,
    transport_adapter::{RuntimeTransportAdapter, TransportConnectDirection, TransportSessionKey},
    websocket_server::WsWriter,
};
use crate::signaling::{
    ortc_mapper,
    protocol::{RecordingOptions, WebSocketCloseCode},
    shared::SessionId,
    webrtc::RtpCapabilities,
};
use o_sfu_router::MediaCapabilities;

const LEGACY_PING_REQUEST_NAME: &str = "PING";

#[derive(Debug)]
pub(super) struct SessionController {
    session_id: SessionId,
    connection_id: u64,
    channel: Arc<Channel>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
    next_request_counter: u64,
    pending_transport_bootstrap_request_id: Option<LegacyRequestId>,
    pending_ping_request_id: Option<LegacyRequestId>,
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
            .send_request_value(
                writer,
                transport_bootstrap_edge::request_value(&bootstrap_payload).map_err(|_error| ())?,
            )
            .await
            .map_err(|_error| ())?;
        self.pending_transport_bootstrap_request_id = Some(request_id);
        Ok(())
    }

    pub(super) fn awaiting_ping_response(&self) -> bool {
        self.pending_ping_request_id.is_some()
    }

    pub(super) fn transport_close_code(&self) -> Option<WebSocketCloseCode> {
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        self.transport_adapter
            .session_transport_health(&session_key)
            .and_then(|health| match health {
                TransportSessionHealth::Disconnected => Some(WebSocketCloseCode::Error),
                TransportSessionHealth::Connected => None,
            })
    }

    pub(super) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        if self.pending_ping_request_id.is_some() {
            return Ok(());
        }
        let request_id = self
            .send_request_value(writer, legacy_ping_request_value())
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
            DomainCommand::LegacyTransportConnect {
                request_id,
                request,
            } => {
                self.handle_legacy_transport_connect_frame(writer, request_id, request)
                    .await
            }
            DomainCommand::PublishTrack {
                request_id,
                request,
            } => {
                self.handle_publish_track_request_frame(writer, request_id, request)
                    .await
            }
            DomainCommand::RecordingControl {
                request_id,
                request,
            } => {
                self.handle_recording_control_request_frame(writer, request_id, request)
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
        response_to: LegacyRequestId,
        message: Value,
    ) -> Result<(), super::StubBusOutcome> {
        if !self.handle_response(response_to, message).await {
            self.metrics.record_ws_bus_client_response_ignored();
            debug!("ignoring client response frame");
        }
        Ok(())
    }

    async fn handle_publish_track_request_frame(
        &self,
        writer: &mut WsWriter,
        request_id: LegacyRequestId,
        request: super::publish_request_edge::LegacyPublishTrackRequest,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_request();
        debug!(
            request_id = %request_id.as_str(),
            "dispatching legacy publish request"
        );
        let response = self.handle_publish_request(request).await;
        self.send_response(writer, request_id, response)
            .await
            .map_err(|_error| super::StubBusOutcome::Break)
    }

    async fn handle_recording_control_request_frame(
        &self,
        writer: &mut WsWriter,
        request_id: LegacyRequestId,
        request: super::recording_request_edge::LegacyRecordingControlRequest,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_request();
        debug!(
            request_id = %request_id.as_str(),
            "dispatching legacy recording control request"
        );
        let response = self.handle_recording_control_request(request).await;
        self.send_response(writer, request_id, response)
            .await
            .map_err(|_error| super::StubBusOutcome::Break)
    }

    async fn handle_legacy_transport_connect_frame(
        &self,
        writer: &mut WsWriter,
        request_id: LegacyRequestId,
        request: super::transport_connect_edge::LegacyTransportConnectRequest,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_request();
        debug!(
            request_id = %request_id.as_str(),
            direction = ?request.direction(),
            "dispatching legacy transport connect request"
        );
        let response = self.handle_transport_connect_request(request).await;
        self.send_response(writer, request_id, response)
            .await
            .map_err(|_error| super::StubBusOutcome::Break)
    }

    async fn handle_invalid_client_request_frame(
        &self,
        writer: &mut WsWriter,
        request_id: LegacyRequestId,
    ) -> Result<(), super::StubBusOutcome> {
        self.metrics.record_ws_bus_client_request_decode_failure();
        debug!("failed to decode client bus request, returning empty object");
        self.send_response(writer, request_id, empty_object())
            .await
            .map_err(|_error| super::StubBusOutcome::Break)
    }

    async fn handle_client_message_frame(
        &self,
        message: LegacyClientMessage,
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

    async fn handle_response(&mut self, response_to: LegacyRequestId, message: Value) -> bool {
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
        response_to: &LegacyRequestId,
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
        let Some(parsed_capabilities) =
            parse_transport_bootstrap_capabilities(response_to.as_str(), message)
        else {
            return true;
        };
        if self
            .channel
            .apply_client_rtp_capabilities(
                &self.session_id,
                self.connection_id,
                parsed_capabilities,
                &self.transport_adapter,
            )
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

    fn handle_ping_response(&mut self, response_to: &LegacyRequestId) -> bool {
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

    fn next_request_id(&mut self) -> LegacyRequestId {
        let request_id = LegacyRequestId::server(STUB_SERVER_BUS_ID, self.next_request_counter);
        self.next_request_counter += 1;
        request_id
    }

    async fn send_request_value(
        &mut self,
        writer: &mut WsWriter,
        message: Value,
    ) -> Result<LegacyRequestId, WebSocketCloseCode> {
        let request_id = self.next_request_id();
        let result = codec::send_request_value(writer, request_id.clone(), message).await;
        if result.is_ok() {
            self.metrics.record_ws_bus_batch_sent(1);
            Ok(request_id)
        } else {
            self.metrics.record_ws_bus_send_failure();
            Err(WebSocketCloseCode::Error)
        }
    }

    async fn send_response(
        &self,
        writer: &mut WsWriter,
        request_id: LegacyRequestId,
        response: Value,
    ) -> Result<(), WebSocketCloseCode> {
        let result = codec::send_response_value(writer, request_id, response).await;
        if result.is_ok() {
            self.metrics.record_ws_bus_batch_sent(1);
        } else {
            self.metrics.record_ws_bus_send_failure();
        }
        result
    }

    async fn handle_start_recording_request(&self, options: RecordingOptions) -> Value {
        debug!("handling recording start request");
        Value::Bool(
            self.channel
                .start_recording(&self.session_id, options)
                .await,
        )
    }

    async fn handle_stop_recording_request(&self) -> Value {
        debug!("handling recording stop request");
        Value::Bool(self.channel.stop_recording(&self.session_id).await)
    }

    async fn handle_transport_connect_request(
        &self,
        request: super::transport_connect_edge::LegacyTransportConnectRequest,
    ) -> Value {
        let direction = request.direction();
        if self
            .transport_adapter
            .connect_transport(
                &self.transport_session_key(),
                request.transport_connect_request(),
            )
            .await
            .is_err()
        {
            debug!(?direction, "transport adapter failed to connect transport");
            return empty_object();
        }
        if !self.apply_legacy_transport_ready(direction).await {
            debug!(
                ?direction,
                "channel no longer tracks session during transport connect"
            );
            return empty_object();
        }
        debug!(?direction, "handled transport connect request");
        empty_object()
    }

    async fn handle_recording_control_request(
        &self,
        request: super::recording_request_edge::LegacyRecordingControlRequest,
    ) -> Value {
        match request {
            super::recording_request_edge::LegacyRecordingControlRequest::Start(options) => {
                self.handle_start_recording_request(options).await
            }
            super::recording_request_edge::LegacyRecordingControlRequest::Stop => {
                self.handle_stop_recording_request().await
            }
        }
    }

    async fn handle_publish_request(
        &self,
        request: super::publish_request_edge::LegacyPublishTrackRequest,
    ) -> Value {
        self.metrics.record_ws_bus_stub_publish_request();
        let producer_id = self
            .channel
            .publish_track(
                &self.session_id,
                request.stream_type(),
                request.media_kind(),
                request.into_producer_rtp_parameters(),
                &self.transport_adapter,
            )
            .await;
        let Some(producer_id) = producer_id else {
            debug!("channel rejected publish request");
            return empty_object();
        };
        debug!(producer_id, "handled publish request");
        json!({ "id": producer_id })
    }

    #[allow(
        clippy::cognitive_complexity,
        reason = "message dispatch is a flat compatibility match that keeps the bus controller readable"
    )]
    async fn execute_message(&self, message: LegacyClientMessage) {
        match message {
            LegacyClientMessage::Broadcast(payload) => {
                debug!("relaying broadcast message to channel peers");
                self.channel.broadcast(&self.session_id, payload).await;
            }
            LegacyClientMessage::UpdateSessionInfo { info, need_refresh } => {
                debug!("relaying session info update to channel peers");
                self.channel
                    .update_session_info(&self.session_id, info, need_refresh)
                    .await;
            }
            LegacyClientMessage::Publish {
                stream_type,
                active,
            } => {
                debug!(
                    ?stream_type,
                    active, "relaying compatibility publish state change to channel"
                );
                self.channel
                    .set_publication_active(
                        &self.session_id,
                        stream_type,
                        active,
                        &self.transport_adapter,
                    )
                    .await;
            }
            LegacyClientMessage::Subscribe { session_id, states } => {
                debug!(
                    target_session = ?session_id,
                    "relaying compatibility subscription change to channel"
                );
                self.channel
                    .update_subscription(
                        &self.session_id,
                        &session_id,
                        &states,
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

    async fn apply_legacy_transport_ready(&self, direction: TransportConnectDirection) -> bool {
        match direction {
            TransportConnectDirection::Upload => {
                self.channel
                    .apply_publish_transport_ready(
                        &self.session_id,
                        self.connection_id,
                        &self.transport_adapter,
                    )
                    .await
            }
            TransportConnectDirection::Download => {
                self.channel
                    .apply_consume_transport_ready(
                        &self.session_id,
                        self.connection_id,
                        &self.transport_adapter,
                    )
                    .await
            }
        }
    }
}

fn legacy_ping_request_value() -> Value {
    json!({ "name": LEGACY_PING_REQUEST_NAME })
}

fn parse_transport_bootstrap_capabilities(
    response_to: &str,
    message: Value,
) -> Option<MediaCapabilities> {
    let Ok(capabilities) = serde_json::from_value::<RtpCapabilities>(message) else {
        debug!(
            response_to,
            "failed to decode transport bootstrap response capabilities"
        );
        return None;
    };
    let Some(parsed_capabilities) = ortc_mapper::parse_rtp_capabilities(&capabilities.0) else {
        debug!(
            response_to,
            "failed to parse transport bootstrap response capabilities"
        );
        return None;
    };
    Some(parsed_capabilities)
}
