#![allow(
    dead_code,
    reason = "shared integration-test support is compiled by multiple test targets, each of which uses only a subset of the helpers"
)]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use futures_util::StreamExt;
use reqwest::StatusCode;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result as WebSocketResult, protocol::frame::coding::CloseCode},
};

use o_sfu::{
    config::{Config, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TransportBackend},
    runtime::testing::TestServer,
    signaling::{
        auth::{
            HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims, WebSocketConnectClaims,
            sign,
        },
        http::{CHANNEL_PATH, ChannelResponse, CreateChannelQuery, DISCONNECT_PATH},
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
        trust_proxy_headers: true,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        rtc_media_worker_count: 1,
        transport_backend: TransportBackend::Fake,
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

pub async fn read_message(websocket: &mut TestWebSocket) -> Option<WebSocketResult<Message>> {
    websocket.next().await
}

pub async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    let message = read_message(websocket).await?;
    let message = message.ok()?;
    match message {
        Message::Text(payload) => Some(payload.to_string()),
        _ => None,
    }
}

pub async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        if let Message::Close(frame) = message.ok()? {
            return frame.map(|frame| frame.code);
        }
    }
}
