#![allow(
    dead_code,
    reason = "shared integration-test support is compiled by multiple test targets, each of which uses only a subset of the helpers"
)]

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use futures_util::{SinkExt, StreamExt};
use o_sfu::{
    config::{
        Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
        TelemetryConfig,
    },
    testing::{
        auth::{
            HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims, WebSocketConnectClaims, sign,
        },
        http::{CHANNEL_PATH, CreateRoomQuery, DISCONNECT_PATH, METRICS_PATH, RoomResponse},
        server::TestServer,
    },
};
use o_sfu_protocol::shared::{UserId, UserPermissions};
use reqwest::StatusCode;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Result as WebSocketResult, protocol::frame::coding::CloseCode},
};

pub type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

pub const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
pub const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";

#[must_use]
pub fn test_config(authentication_timeout_ms: u64, room_size: usize) -> Config {
    Config {
        auth_key: TEST_AUTH_KEY.to_owned(),
        bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
        authentication_timeout_ms,
        room_size,
        user_timeout_ms: 10_000,
        ping_interval_ms: 60_000,
        trust_proxy_headers: true,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        diagnostics: DiagnosticsConfig::default(),
        telemetry: TelemetryConfig::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        max_bitrate_in_bps: 8_000_000,
        max_bitrate_out_bps: 10_000_000,
        rtc_media_worker_count: 1,
    }
}

#[must_use]
pub fn signed_connect_claims(key: &str, room_id: &str, user_id: UserId) -> Option<String> {
    sign(
        &WebSocketConnectClaims {
            registered: RegisteredJwtClaims::default(),
            room_id: room_id.to_owned(),
            user_id,
            label: Some("Alice".to_owned()),
            permissions: Some(UserPermissions::default()),
        },
        key,
    )
    .ok()
}

pub fn signed_room_claims(issuer: &str, key: Option<&str>) -> Option<String> {
    sign(
        &HttpRoomClaims {
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

#[must_use]
pub fn signed_disconnect_claims(user_ids_by_room: BTreeMap<String, Vec<UserId>>) -> Option<String> {
    sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            user_ids_by_room,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub async fn create_room(server: &TestServer, issuer: &str, key: Option<&str>) -> Option<String> {
    let token = signed_room_claims(issuer, key)?;
    let response = reqwest::Client::new()
        .get(format!("{}{CHANNEL_PATH}", server.http_base_url()))
        .bearer_auth(token)
        .header("x-forwarded-for", "127.0.0.1")
        .query(&CreateRoomQuery::default())
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload = response.json::<RoomResponse>().await.ok()?;
    Some(payload.uuid)
}

pub async fn disconnect_sessions_via_http(
    server: &TestServer,
    user_ids_by_room: BTreeMap<String, Vec<UserId>>,
) -> Option<StatusCode> {
    let token = signed_disconnect_claims(user_ids_by_room)?;
    let response = reqwest::Client::new()
        .post(format!("{}{DISCONNECT_PATH}", server.http_base_url()))
        .body(token)
        .send()
        .await
        .ok()?;
    Some(response.status())
}

pub async fn metrics_text(server: &TestServer) -> Option<String> {
    let response = reqwest::Client::new()
        .get(format!("{}{METRICS_PATH}", server.http_base_url()))
        .send()
        .await
        .ok()?;
    response.text().await.ok()
}

pub async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.ws_url()).await.ok()?;
    Some(websocket.0)
}

pub async fn read_message(websocket: &mut TestWebSocket) -> Option<WebSocketResult<Message>> {
    websocket.next().await
}

pub async fn read_text_message(websocket: &mut TestWebSocket) -> Option<String> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            Message::Text(payload) => return Some(payload.to_string()),
            Message::Ping(payload) => {
                websocket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) => {}
            Message::Binary(_) | Message::Close(_) | Message::Frame(_) => return None,
        }
    }
}

pub async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        match message.ok()? {
            Message::Close(frame) => return frame.map(|frame| frame.code),
            Message::Ping(payload) => {
                websocket.send(Message::Pong(payload)).await.ok()?;
            }
            Message::Pong(_) | Message::Text(_) | Message::Binary(_) | Message::Frame(_) => {}
        }
    }
}
