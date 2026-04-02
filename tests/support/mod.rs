use std::net::SocketAddr;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol::frame::coding::CloseCode},
};

use o_sfu::{
    config::Config,
    runtime::testing::TestServer,
    signaling::{
        auth::{RegisteredJwtClaims, WebSocketConnectClaims, sign},
        current_bus::{CurrentBusBatch, CurrentBusEnvelope},
        current_protocol::{
            CurrentClientMessage, CurrentServerMessage, CurrentStartupPayload,
            CurrentWebSocketCredentials,
        },
        http::CreateChannelQuery,
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

pub async fn create_channel(
    server: &TestServer,
    issuer: &str,
    key: Option<&str>,
) -> Option<String> {
    Some(
        server
            .create_channel(issuer, key, &CreateChannelQuery::default())
            .await,
    )
}

pub async fn connect_websocket(server: &TestServer) -> Option<TestWebSocket> {
    let websocket = connect_async(server.ws_url()).await.ok()?;
    Some(websocket.0)
}

pub async fn authenticate_with_credentials(
    server: &TestServer,
    credentials: &CurrentWebSocketCredentials,
) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = serde_json::to_string(credentials).ok()?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub async fn authenticate_with_jwt(server: &TestServer, token: &str) -> Option<TestWebSocket> {
    let mut websocket = connect_websocket(server).await?;
    let payload = serde_json::to_string(&serde_json::json!({ "jwt": token })).ok()?;
    websocket
        .send(tungstenite::Message::Text(payload.into()))
        .await
        .ok()?;
    Some(websocket)
}

pub async fn authenticate_and_read_startup(
    server: &TestServer,
    token: &str,
) -> Option<(TestWebSocket, CurrentStartupPayload)> {
    let mut websocket = authenticate_with_jwt(server, token).await?;
    let startup_json = read_text_message(&mut websocket).await?;
    let startup = serde_json::from_str::<CurrentStartupPayload>(&startup_json).ok()?;
    Some((websocket, startup))
}

pub async fn acknowledge_transport_bootstrap(websocket: &mut TestWebSocket) -> Option<()> {
    let batch = read_bus_batch(websocket).await?;
    let envelope = batch.first()?;
    let response = serde_json::to_string(&vec![CurrentBusEnvelope {
        message: serde_json::json!({
            "codecs": [],
            "headerExtensions": []
        }),
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

pub async fn read_close_code(websocket: &mut TestWebSocket) -> Option<CloseCode> {
    loop {
        let message = read_message(websocket).await?;
        if let tungstenite::Message::Close(frame) = message.ok()? {
            return frame.map(|frame| frame.code);
        }
    }
}
