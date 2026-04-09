use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::extract::ws::Message;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::sleep;
use tracing::{debug, trace};

use super::channel::Channel;
use crate::runtime::{
    metrics::RuntimeMetrics,
    transport_adapter::{
        RuntimeTransportAdapter, TransportAdapterError, TransportConnectDirection,
        TransportMediaId, TransportSessionKey,
    },
};
use crate::signaling::{
    current_bus::{CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackPayload,
        CurrentPublishTrackResponse, CurrentServerRequest, CurrentTransportBootstrapPayload,
        CurrentTransportConnectPayload, CurrentWebSocketCloseCode,
    },
    shared::{SessionId, StreamType},
    webrtc::{DtlsParameters, IceParameters, MediaKind, RtpCapabilities},
};
use o_sfu_router::RtpParameters as RouterRtpParameters;

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
    PublishMediaRequested {
        session_id: SessionId,
        stream_type: StreamType,
        media_kind: MediaKind,
    },
    ConsumeMediaRequested {
        consumer_session_id: SessionId,
        source_session_id: SessionId,
        media_kind: MediaKind,
    },
    MediaRemoved {
        session_id: SessionId,
        transport_media_id: TransportMediaId,
    },
    ProducerActivityUpdated {
        session_id: SessionId,
        active: bool,
    },
    ConsumerActivityUpdated {
        consumer_session_id: SessionId,
        source_session_id: SessionId,
        active: bool,
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
pub(crate) struct StubWebRtcAdapter {
    events: Arc<Mutex<Vec<StubWebRtcEvent>>>,
    next_media_id: Arc<AtomicU64>,
    delays: Arc<Mutex<StubWebRtcAdapterDelays>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct StubWebRtcAdapterDelays {
    publish_media: Option<Duration>,
    consume_media: Option<Duration>,
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

    fn delay_for_publish_media(&self) -> Option<Duration> {
        match self.delays.lock() {
            Ok(delays) => delays.publish_media,
            Err(poisoned) => poisoned.into_inner().publish_media,
        }
    }

    fn delay_for_consume_media(&self) -> Option<Duration> {
        match self.delays.lock() {
            Ok(delays) => delays.consume_media,
            Err(poisoned) => poisoned.into_inner().consume_media,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_events(&self) -> Vec<StubWebRtcEvent> {
        match self.events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn set_publish_media_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.publish_media = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().publish_media = delay;
            }
        }
    }

    #[cfg(test)]
    pub(super) fn set_consume_media_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.consume_media = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().consume_media = delay;
            }
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
        _session_key: &TransportSessionKey,
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
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        _ice_parameters: Option<&IceParameters>,
        _sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::TransportConnectRequested {
            session_id: session_key.session_id().clone(),
            direction,
            dtls_parameters: dtls_parameters.clone(),
        });
        if dtls_parameters.role.is_empty() || dtls_parameters.fingerprints.is_empty() {
            self.record_event(StubWebRtcEvent::TransportConnectRejected {
                session_id: session_key.session_id().clone(),
                direction,
            });
            return Err(TransportAdapterError::TransportUnavailable);
        }
        self.record_event(StubWebRtcEvent::TransportConnected {
            session_id: session_key.session_id().clone(),
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
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::SessionClosed {
            session_id: session_key.session_id().clone(),
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::MediaRemoved {
            session_id: session_key.session_id().clone(),
            transport_media_id,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        stream_type: StreamType,
        media_kind: MediaKind,
        _rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(StubWebRtcEvent::PublishMediaRequested {
            session_id: session_key.session_id().clone(),
            stream_type,
            media_kind,
        });
        if let Some(delay) = self.delay_for_publish_media() {
            sleep(delay).await;
        }
        let id = self.next_media_id.fetch_add(1, Ordering::Relaxed);
        Ok(TransportMediaId::new(id))
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        _consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(StubWebRtcEvent::ConsumeMediaRequested {
            consumer_session_id: consumer_session_key.session_id().clone(),
            source_session_id: source_session_key.session_id().clone(),
            media_kind,
        });
        if let Some(delay) = self.delay_for_consume_media() {
            sleep(delay).await;
        }
        let id = self.next_media_id.fetch_add(1, Ordering::Relaxed);
        Ok(TransportMediaId::new(id))
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::ProducerActivityUpdated {
            session_id: session_key.session_id().clone(),
            active,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(super) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        _consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        _source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::ConsumerActivityUpdated {
            consumer_session_id: consumer_session_key.session_id().clone(),
            source_session_id: source_session_key.session_id().clone(),
            active,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct StubBusSession {
    session_id: SessionId,
    connection_id: u64,
    channel: Arc<Channel>,
    metrics: Arc<RuntimeMetrics>,
    transport_adapter: RuntimeTransportAdapter,
    next_request_counter: u64,
    pending_transport_bootstrap_request_id: Option<CurrentBusRequestId>,
    pending_ping_request_id: Option<CurrentBusRequestId>,
}

impl StubBusSession {
    #[must_use]
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

    async fn handle_response(&mut self, response_to: CurrentBusRequestId, message: Value) -> bool {
        if self
            .handle_transport_bootstrap_response(&response_to, message.clone())
            .await
        {
            return true;
        }
        self.handle_ping_response(&response_to)
    }

    pub(super) fn awaiting_ping_response(&self) -> bool {
        self.pending_ping_request_id.is_some()
    }

    pub(super) async fn send_ping(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), CurrentWebSocketCloseCode> {
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
                &self
                    .channel
                    .transport_session_key(&self.session_id, self.connection_id),
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
        reason = "message dispatch is a flat match over client message variants, each delegating to channel"
    )]
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
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}
