#![allow(
    dead_code,
    reason = "shared integration-test support is compiled by multiple test targets, each of which uses only a subset of the helpers"
)]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol::frame::coding::CloseCode},
};

use super::legacy_wire::{
    bus::{CurrentBusBatch, CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId},
    protocol::{
        CurrentClientMessage, CurrentServerMessage, CurrentServerRequest,
        CurrentWebSocketCredentials,
    },
};
use o_sfu::{
    config::{Config, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TransportBackend},
    runtime::testing::{TestServer, decode_native_welcome_batch},
    signaling::{
        auth::{
            HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims, WebSocketConnectClaims,
            sign,
        },
        http::{CHANNEL_PATH, ChannelResponse, CreateChannelQuery, DISCONNECT_PATH},
        protocol::{AuthPayload, ClientEnvelope, ClientMessage, WelcomePayload},
        shared::{SessionId, SessionPermissions},
    },
};

pub type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

pub const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub const TEST_CHANNEL_KEY: &str = "Y2hhbm5lbC1rZXk=";

pub fn test_config(authentication_timeout_ms: u64, channel_size: usize) -> Config {
    Config {
        auth_key: TEST_AUTH_KEY.to_owned(),
        bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
        authentication_timeout_ms,
        channel_size,
        session_timeout_ms: 10_000,
        ping_interval_ms: 60_000,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        rtc_media_worker_count: 1,
        transport_backend: TransportBackend::Stub,
    }
}

pub fn signed_connect_claims(
    key: &str,
    channel_uuid: &str,
    session_id: SessionId,
) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            sfu_channel_uuid: channel_uuid.to_owned(),
            session_id,
            label: Some("Alice".to_owned()),
            permissions: Some(SessionPermissions::default()),
        },
        key,
    )
    .ok()
}

pub fn signed_channel_claims(issuer: &str, key: Option<&str>) -> Option<String> {
    sign(
        &HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: Some(issuer.to_owned()),
                ..RegisteredJwtClaims::default()
            },
            key: key.map(str::to_owned),
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub fn signed_disconnect_claims(
    session_ids_by_channel: BTreeMap<String, Vec<SessionId>>,
) -> Option<String> {
    sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            session_ids_by_channel,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub async fn create_channel(
    server: &TestServer,
    issuer: &str,
    key: Option<&str>,
) -> Option<String> {
    let token = signed_channel_claims(issuer, key)?;
    let response = reqwest::Client::new()
        .get(format!("{}{CHANNEL_PATH}", server.http_base_url()))
        .bearer_auth(token)
        .header("x-forwarded-for", "127.0.0.1")
        .query(&CreateChannelQuery::default())
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload = response.json::<ChannelResponse>().await.ok()?;
    Some(payload.uuid)
}

pub async fn disconnect_sessions_via_http(
    server: &TestServer,
    session_ids_by_channel: BTreeMap<String, Vec<SessionId>>,
) -> Option<StatusCode> {
    let token = signed_disconnect_claims(session_ids_by_channel)?;
    let response = reqwest::Client::new()
        .post(format!("{}{DISCONNECT_PATH}", server.http_base_url()))
        .body(token)
        .send()
        .await
        .ok()?;
    Some(response.status())
}

pub async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.ws_url()).await.ok()?;
    Some(websocket.0)
}

pub struct FakeWebSocketClient {
    pub(crate) websocket: TestWebSocket,
}

impl FakeWebSocketClient {
    pub async fn connect(server: &TestServer) -> Option<Self> {
        Some(Self {
            websocket: connect_websocket(server).await?,
        })
    }

    pub async fn authenticate_with_credentials(
        server: &TestServer,
        credentials: &CurrentWebSocketCredentials,
    ) -> Option<Self> {
        Some(Self {
            websocket: authenticate_with_credentials(server, credentials).await?,
        })
    }

    pub async fn authenticate_with_jwt(server: &TestServer, token: &str) -> Option<Self> {
        Some(Self {
            websocket: authenticate_with_jwt(server, token).await?,
        })
    }

    pub async fn authenticate_and_bootstrap(
        server: &TestServer,
        token: &str,
    ) -> Option<(Self, WelcomePayload)> {
        let mut client = Self::authenticate_with_jwt(server, token).await?;
        let welcome = client.read_welcome().await?;
        client.acknowledge_transport_bootstrap().await?;
        Some((client, welcome))
    }

    pub async fn read_welcome(&mut self) -> Option<WelcomePayload> {
        read_welcome(&mut self.websocket).await
    }

    pub async fn acknowledge_transport_bootstrap(&mut self) -> Option<()> {
        acknowledge_transport_bootstrap(&mut self.websocket).await
    }

    pub async fn send_bus_message(&mut self, message: CurrentClientMessage) -> Option<()> {
        send_bus_message(&mut self.websocket, message).await
    }

    pub async fn read_server_message(&mut self) -> Option<CurrentServerMessage> {
        read_server_message(&mut self.websocket).await
    }

    pub async fn read_close_code(&mut self) -> Option<CloseCode> {
        read_close_code(&mut self.websocket).await
    }

    pub async fn close(mut self) -> Option<()> {
        self.websocket.close(None).await.ok()?;
        Some(())
    }

    pub async fn read_bus_batch(&mut self) -> Option<CurrentBusBatch> {
        read_bus_batch(&mut self.websocket).await
    }

    pub async fn send_bus_request<T>(&mut self, request: &T) -> Option<CurrentBusEnvelope>
    where
        T: serde::Serialize,
    {
        send_bus_request(&mut self.websocket, request).await
    }

    pub async fn respond_to_server_request(
        &mut self,
        request_id: &CurrentBusRequestId,
        response: Value,
    ) -> Option<()> {
        respond_to_server_request(&mut self.websocket, request_id, response).await
    }

    pub async fn read_server_request(
        &mut self,
    ) -> Option<(Option<CurrentBusRequestId>, CurrentServerRequest)> {
        read_server_request(&mut self.websocket).await
    }
}

pub async fn authenticate_with_credentials(
    server: &TestServer,
    credentials: &CurrentWebSocketCredentials,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_native_auth(AuthPayload {
        jwt: credentials.jwt.clone(),
        channel: credentials.channel_uuid.clone(),
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub async fn authenticate_with_jwt(server: &TestServer, token: &str) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = encode_native_auth(AuthPayload {
        jwt: token.to_owned(),
        channel: None,
    })?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub async fn acknowledge_transport_bootstrap(websocket: &mut TestWebSocket) -> Option<()> {
    let batch = read_bus_batch(websocket).await?;
    let envelope = batch.first()?;
    let response = serde_json::to_string(&vec![CurrentBusEnvelope {
        message: test_client_rtp_capabilities(),
        need_response: None,
        response_to: envelope.need_response.clone(),
    }])
    .ok()?;
    websocket
        .send(tungstenite::Message::Text(response.into()))
        .await
        .ok()?;
    Some(())
}

fn encode_native_auth(auth_payload: AuthPayload) -> Option<String> {
    let envelope = ClientEnvelope::Message(ClientMessage::Auth(auth_payload))
        .into_envelope()
        .ok()?;
    serde_json::to_string(&vec![envelope]).ok()
}

pub async fn read_welcome(websocket: &mut TestWebSocket) -> Option<WelcomePayload> {
    let payload = read_text_message(websocket).await?;
    decode_native_welcome_batch(&payload)
}

/// Realistc client RTP capabilities (corespond to router default)
fn test_client_rtp_capabilities() -> serde_json::Value {
    serde_json::json!({
        "codecs": test_client_rtp_capability_codecs(),
        "headerExtensions": test_client_rtp_capability_header_extensions()
    })
}

fn test_client_rtp_capability_codecs() -> serde_json::Value {
    serde_json::json!([
        {
            "mimeType": "audio/opus",
            "kind": "audio",
            "preferredPayloadType": 111,
            "clockRate": 48000,
            "channels": 2,
            "parameters": {
                "minptime": 10,
                "useinbandfec": 1
            },
            "rtcpFeedback": [{ "type": "transport-cc", "parameter": "" }]
        },
        {
            "mimeType": "video/VP8",
            "kind": "video",
            "preferredPayloadType": 96,
            "clockRate": 90000,
            "parameters": {},
            "rtcpFeedback": [
                { "type": "goog-remb", "parameter": "" },
                { "type": "transport-cc", "parameter": "" },
                { "type": "ccm", "parameter": "fir" },
                { "type": "nack", "parameter": "" },
                { "type": "nack", "parameter": "pli" }
            ]
        },
        {
            "mimeType": "video/rtx",
            "kind": "video",
            "preferredPayloadType": 97,
            "clockRate": 90000,
            "parameters": { "apt": 96 },
            "rtcpFeedback": []
        }
    ])
}

fn test_client_rtp_capability_header_extensions() -> serde_json::Value {
    serde_json::json!([
        {
            "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
            "preferredId": 1,
            "preferredEncrypt": false,
            "kind": "audio",
            "direction": "sendrecv"
        },
        {
            "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
            "preferredId": 4,
            "preferredEncrypt": false,
            "kind": "audio",
            "direction": "sendrecv"
        },
        {
            "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
            "preferredId": 5,
            "preferredEncrypt": false,
            "kind": "audio",
            "direction": "sendrecv"
        },
        {
            "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
            "preferredId": 10,
            "preferredEncrypt": false,
            "kind": "audio",
            "direction": "sendrecv"
        },
        {
            "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
            "preferredId": 1,
            "preferredEncrypt": false,
            "kind": "video",
            "direction": "sendrecv"
        },
        {
            "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
            "preferredId": 4,
            "preferredEncrypt": false,
            "kind": "video",
            "direction": "sendrecv"
        },
        {
            "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
            "preferredId": 5,
            "preferredEncrypt": false,
            "kind": "video",
            "direction": "sendrecv"
        },
        {
            "uri": "urn:3gpp:video-orientation",
            "preferredId": 11,
            "preferredEncrypt": false,
            "kind": "video",
            "direction": "sendrecv"
        },
        {
            "uri": "urn:ietf:params:rtp-hdrext:toffset",
            "preferredId": 12,
            "preferredEncrypt": false,
            "kind": "video",
            "direction": "sendrecv"
        }
    ])
}

pub async fn send_bus_message(
    websocket: &mut TestWebSocket,
    message: CurrentClientMessage,
) -> Option<()> {
    let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
        message: serde_json::to_value(message).ok()?,
        need_response: None,
        response_to: None,
    }])
    .ok()?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(())
}

pub async fn send_bus_request<T>(
    websocket: &mut TestWebSocket,
    request: &T,
) -> Option<CurrentBusEnvelope>
where
    T: serde::Serialize,
{
    let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
        message: serde_json::to_value(request).ok()?,
        need_response: Some(CurrentBusRequestId::new(CurrentBusOrigin::Client, 0, 0)),
        response_to: None,
    }])
    .ok()?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    let batch = read_bus_batch(websocket).await?;
    batch.first().cloned()
}

pub async fn respond_to_server_request(
    websocket: &mut TestWebSocket,
    request_id: &CurrentBusRequestId,
    response: Value,
) -> Option<()> {
    let payload = serde_json::to_string(&vec![CurrentBusEnvelope {
        message: response,
        need_response: None,
        response_to: Some(request_id.clone()),
    }])
    .ok()?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(())
}

pub async fn read_message(
    websocket: &mut TestWebSocket,
) -> Option<tungstenite::Result<tungstenite::Message>> {
    websocket.next().await
}

pub async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    let message = read_message(websocket).await?;
    let message = message.ok()?;
    match message {
        tungstenite::Message::Text(payload) => Some(payload.to_string()),
        _ => None,
    }
}

pub async fn read_bus_batch(websocket: &mut TestWebSocket) -> Option<CurrentBusBatch> {
    let payload = read_text_message(websocket).await?;
    serde_json::from_str(&payload).ok()
}

pub async fn read_server_message(websocket: &mut TestWebSocket) -> Option<CurrentServerMessage> {
    let batch = read_bus_batch(websocket).await?;
    let envelope = batch.first()?;
    serde_json::from_value(envelope.message.clone()).ok()
}

pub async fn read_server_request(
    websocket: &mut TestWebSocket,
) -> Option<(Option<CurrentBusRequestId>, CurrentServerRequest)> {
    let batch = read_bus_batch(websocket).await?;
    let envelope = batch.first()?;
    Some((
        envelope.need_response.clone(),
        serde_json::from_value(envelope.message.clone()).ok()?,
    ))
}

pub async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        if let tungstenite::Message::Close(frame) = message.ok()? {
            return frame.map(|frame| frame.code);
        }
    }
}

pub(crate) fn supported_client_rtp_capabilities() -> Value {
    test_client_rtp_capabilities()
}
