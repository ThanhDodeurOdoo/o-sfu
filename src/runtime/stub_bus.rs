use axum::extract::ws::{Message, WebSocket};
use serde_json::{Map, Value, json};

use crate::signaling::{
    current_bus::{CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    current_protocol::{
        CurrentClientMessage, CurrentClientRequest, CurrentPublishTrackResponse,
        CurrentServerRequest, CurrentTransportBootstrapPayload, CurrentWebSocketCloseCode,
    },
    webrtc::{
        DtlsParameters, IceCandidate, IceParameters, PublishOptions, PublishOptionsByMediaKind,
        RtpCapabilities, SctpParameters, TransportBootstrap,
    },
};

const STUB_SERVER_BUS_ID: u64 = 0;
const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StubBusOutcome {
    Continue,
    Break,
    Close(CurrentWebSocketCloseCode),
}

#[derive(Debug, Default)]
pub(super) struct StubBusSession {
    next_request_counter: u64,
    next_producer_counter: u64,
}

impl StubBusSession {
    #[must_use]
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) async fn send_transport_bootstrap(
        &mut self,
        socket: &mut WebSocket,
    ) -> Result<(), ()> {
        self.send_request(
            socket,
            CurrentServerRequest::BootstrapTransports(stub_transport_bootstrap_payload()),
        )
        .await
        .map_err(|_error| ())
    }

    pub(super) async fn handle_frame(
        &mut self,
        socket: &mut WebSocket,
        message: Message,
    ) -> StubBusOutcome {
        let batch = match parse_batch(message) {
            Ok(Some(batch)) => batch,
            Ok(None) => return StubBusOutcome::Break,
            Err(close_code) => return StubBusOutcome::Close(close_code),
        };
        for envelope in batch {
            match self.handle_envelope(socket, envelope).await {
                Ok(()) => {}
                Err(outcome) => return outcome,
            }
        }
        StubBusOutcome::Continue
    }

    async fn handle_envelope(
        &mut self,
        socket: &mut WebSocket,
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
            self.send_response(socket, request_id, response)
                .await
                .map_err(|_error| StubBusOutcome::Break)?;
            return Ok(());
        }
        Self::dispatch_message(message);
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

    fn dispatch_message(message: Value) {
        let _result = serde_json::from_value::<CurrentClientMessage>(message);
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
        socket: &mut WebSocket,
        request: CurrentServerRequest,
    ) -> Result<(), CurrentWebSocketCloseCode> {
        let message =
            serde_json::to_value(request).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
        let batch = vec![CurrentBusEnvelope {
            message,
            need_response: Some(self.next_request_id()),
            response_to: None,
        }];
        send_batch(socket, batch).await
    }

    async fn send_response(
        &self,
        socket: &mut WebSocket,
        request_id: CurrentBusRequestId,
        response: Value,
    ) -> Result<(), CurrentWebSocketCloseCode> {
        send_batch(
            socket,
            vec![CurrentBusEnvelope {
                message: response,
                need_response: None,
                response_to: Some(request_id),
            }],
        )
        .await
    }
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
    socket: &mut WebSocket,
    batch: CurrentBusBatch,
) -> Result<(), CurrentWebSocketCloseCode> {
    let payload =
        serde_json::to_string(&batch).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
    socket
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
