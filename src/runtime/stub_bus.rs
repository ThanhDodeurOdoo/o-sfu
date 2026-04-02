use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use serde_json::{Map, Value, json};

use super::channel::Channel;
use crate::signaling::{
    current_bus::{CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackResponse,
        CurrentServerMessage, CurrentServerRequest, CurrentTransportBootstrapPayload,
        CurrentWebSocketCloseCode,
    },
    shared::SessionId,
    webrtc::{
        DtlsParameters, IceCandidate, IceParameters, PublishOptions, PublishOptionsByMediaKind,
        RtpCapabilities, SctpParameters, TransportBootstrap,
    },
};

const STUB_SERVER_BUS_ID: u64 = 0;
const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

pub(super) type WsWriter = SplitSink<WebSocket, Message>;

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
            CurrentServerRequest::BootstrapTransports(stub_transport_bootstrap_payload()),
        )
        .await
        .map_err(|_error| ())
    }

    pub(super) async fn handle_frame(
        &mut self,
        writer: &mut WsWriter,
        message: Message,
    ) -> StubBusOutcome {
        let batch = match parse_batch(message) {
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
            ) => {
                // Production change and consumption change are deferred
                // until the router model carries producer/consumer state.
            }
            Err(_error) => {}
        }
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
    ) -> Result<(), CurrentWebSocketCloseCode> {
        let message =
            serde_json::to_value(request).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
        let batch = vec![CurrentBusEnvelope {
            message,
            need_response: Some(self.next_request_id()),
            response_to: None,
        }];
        send_batch(writer, batch).await
    }

    async fn send_response(
        &self,
        writer: &mut WsWriter,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> Result<(), CurrentWebSocketCloseCode> {
        send_batch(
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

/// Serialize a server message into a single-envelope Bus batch and send it.
pub(super) async fn send_server_message_batch(
    writer: &mut WsWriter,
    message: &CurrentServerMessage,
) -> Result<(), CurrentWebSocketCloseCode> {
    let value = serde_json::to_value(message).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
    send_batch(
        writer,
        vec![CurrentBusEnvelope {
            message: value,
            need_response: None,
            response_to: None,
        }],
    )
    .await
}

fn parse_batch(message: Message) -> Result<Option<CurrentBusBatch>, CurrentWebSocketCloseCode> {
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| CurrentWebSocketCloseCode::Error)?,
        Message::Close(_) => return Ok(None),
        Message::Ping(_) | Message::Pong(_) => return Ok(Some(Vec::new())),
    };
    serde_json::from_str::<CurrentBusBatch>(&payload)
        .map(Some)
        .map_err(|_error| CurrentWebSocketCloseCode::Error)
}

async fn send_batch(
    writer: &mut WsWriter,
    batch: CurrentBusBatch,
) -> Result<(), CurrentWebSocketCloseCode> {
    let payload =
        serde_json::to_string(&batch).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
    writer
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_error| CurrentWebSocketCloseCode::Error)
}

fn stub_transport_bootstrap_payload() -> CurrentTransportBootstrapPayload {
    CurrentTransportBootstrapPayload {
        router_capabilities: RtpCapabilities(json!({
            "codecs": [],
            "headerExtensions": []
        })),
        download_transport: stub_transport_bootstrap(STUB_STC_TRANSPORT_ID),
        upload_transport: stub_transport_bootstrap(STUB_CTS_TRANSPORT_ID),
        publish_options_by_media_kind: PublishOptionsByMediaKind {
            audio: PublishOptions(json!({
                "stopTracks": false
            })),
            video: PublishOptions(json!({
                "stopTracks": false,
                "zeroRtpOnPause": true
            })),
        },
    }
}

fn stub_transport_bootstrap(id: &str) -> TransportBootstrap {
    TransportBootstrap {
        id: id.to_owned(),
        ice_parameters: IceParameters(json!({
            "usernameFragment": "ufrag",
            "password": "pwd",
            "iceLite": true
        })),
        ice_candidates: vec![IceCandidate(json!({
            "foundation": "foundation",
            "priority": 1,
            "ip": "203.0.113.10",
            "protocol": "udp",
            "port": 40000,
            "type": "host"
        }))],
        dtls_parameters: DtlsParameters(json!({
            "role": "auto",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": "AA:BB:CC"
            }]
        })),
        sctp_parameters: SctpParameters(json!({
            "port": 5000,
            "OS": 1024,
            "MIS": 1024,
            "maxMessageSize": 262_144
        })),
    }
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}
