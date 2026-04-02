use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt, stream::SplitStream};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::{
    RuntimeState,
    channel::{Channel, ChannelManagerJoinError, SessionOutbound},
    stub_bus::{StubBusOutcome, StubBusSession, WsWriter, send_server_message_batch},
};
use crate::signaling::{
    auth::{self, WebSocketConnectClaims},
    current_protocol::{
        CurrentStartupPayload, CurrentWebSocketCloseCode, CurrentWebSocketCredentials,
    },
};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AuthenticationPayload {
    Credentials(CurrentWebSocketCredentials),
    Jwt(String),
}

pub(super) async fn upgrade(
    State(state): State<RuntimeState>,
    websocket: WebSocketUpgrade,
) -> Response {
    websocket.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: RuntimeState) {
    let (mut ws_writer, mut ws_reader) = socket.split();

    let credentials = match receive_credentials(&state, &mut ws_reader).await {
        Ok(c) => c,
        Err(Some(code)) => {
            close_writer(&mut ws_writer, code).await;
            return;
        }
        Err(None) => return,
    };

    let (channel, claims) = match authenticate(&state, credentials).await {
        Ok(result) => result,
        Err(code) => {
            close_writer(&mut ws_writer, code).await;
            return;
        }
    };

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let (channel, connection_id) = match state
        .channels
        .join_session(
            channel.uuid(),
            claims.session_id.clone(),
            claims.label.clone(),
            claims.permissions.clone().unwrap_or_default(),
            outbound_tx,
            state.config.channel_size,
        )
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let code = match error {
                ChannelManagerJoinError::MissingChannel => {
                    CurrentWebSocketCloseCode::AuthenticationFailed
                }
                ChannelManagerJoinError::ChannelFull => CurrentWebSocketCloseCode::ChannelFull,
            };
            close_writer(&mut ws_writer, code).await;
            return;
        }
    };

    if send_startup(&channel, &mut ws_writer).await.is_err() {
        state
            .channels
            .leave_session(channel.uuid(), &claims.session_id, connection_id)
            .await;
        return;
    }

    let mut stub_bus = StubBusSession::new(claims.session_id.clone(), Arc::clone(&channel));
    if stub_bus
        .send_transport_bootstrap(&mut ws_writer)
        .await
        .is_err()
    {
        state
            .channels
            .leave_session(channel.uuid(), &claims.session_id, connection_id)
            .await;
        return;
    }

    run_message_loop(&mut ws_writer, &mut ws_reader, outbound_rx, &mut stub_bus).await;
    state
        .channels
        .leave_session(channel.uuid(), &claims.session_id, connection_id)
        .await;
}

type WsReader = SplitStream<WebSocket>;

async fn receive_credentials(
    state: &RuntimeState,
    reader: &mut WsReader,
) -> Result<CurrentWebSocketCredentials, Option<CurrentWebSocketCloseCode>> {
    match timeout(
        Duration::from_millis(state.config.authentication_timeout_ms),
        reader.next(),
    )
    .await
    {
        Err(_) => Err(Some(CurrentWebSocketCloseCode::Timeout)),
        Ok(None) => Err(None),
        Ok(Some(Err(_error))) => Err(Some(CurrentWebSocketCloseCode::Error)),
        Ok(Some(Ok(message))) => parse_credentials(message).map_err(Some),
    }
}

async fn send_startup(channel: &Channel, writer: &mut WsWriter) -> Result<(), ()> {
    let startup_payload = CurrentStartupPayload {
        available_features: channel.available_features(),
        recording_state: channel.recording_state().await,
    };
    let startup_json = serde_json::to_string(&startup_payload).map_err(|_error| ())?;
    writer
        .send(Message::Text(startup_json.into()))
        .await
        .map_err(|_error| ())
}

async fn run_message_loop(
    writer: &mut WsWriter,
    reader: &mut WsReader,
    mut outbound_rx: mpsc::UnboundedReceiver<SessionOutbound>,
    stub_bus: &mut StubBusSession,
) {
    loop {
        tokio::select! {
            msg = reader.next() => {
                match msg {
                    Some(Ok(message)) => {
                        match stub_bus.handle_frame(writer, message).await {
                            StubBusOutcome::Continue => {}
                            StubBusOutcome::Break => break,
                            StubBusOutcome::Close(code) => {
                                close_writer(writer, code).await;
                                break;
                            }
                        }
                    }
                    Some(Err(_error)) => break,
                    None => break,
                }
            }
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(SessionOutbound::Message(msg)) => {
                        if send_server_message_batch(writer, &msg).await.is_err() {
                            break;
                        }
                    }
                    Some(SessionOutbound::Close(code)) => {
                        close_writer(writer, code).await;
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}

fn parse_credentials(
    message: Message,
) -> Result<CurrentWebSocketCredentials, CurrentWebSocketCloseCode> {
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| CurrentWebSocketCloseCode::Error)?,
        Message::Close(_) => return Err(CurrentWebSocketCloseCode::Clean),
        Message::Ping(_) | Message::Pong(_) => return Err(CurrentWebSocketCloseCode::Error),
    };
    let payload: AuthenticationPayload =
        serde_json::from_str(&payload).map_err(|_error| CurrentWebSocketCloseCode::Error)?;
    Ok(match payload {
        AuthenticationPayload::Credentials(credentials) => credentials,
        AuthenticationPayload::Jwt(jwt) => CurrentWebSocketCredentials {
            channel_uuid: None,
            jwt,
        },
    })
}

async fn authenticate(
    state: &RuntimeState,
    credentials: CurrentWebSocketCredentials,
) -> Result<(Arc<Channel>, WebSocketConnectClaims), CurrentWebSocketCloseCode> {
    if let Some(channel_uuid) = credentials.channel_uuid.as_deref() {
        let Some(channel) = state.channels.get_by_uuid(channel_uuid).await else {
            return Err(CurrentWebSocketCloseCode::AuthenticationFailed);
        };
        let key = channel.key().unwrap_or(&state.config.auth_key);
        let claims = auth::verify::<WebSocketConnectClaims>(&credentials.jwt, key)
            .map_err(|_error| CurrentWebSocketCloseCode::AuthenticationFailed)?;
        if claims.sfu_channel_uuid != channel_uuid {
            return Err(CurrentWebSocketCloseCode::AuthenticationFailed);
        }
        return Ok((channel, claims));
    }
    let claims = auth::verify::<WebSocketConnectClaims>(&credentials.jwt, &state.config.auth_key)
        .map_err(|_error| CurrentWebSocketCloseCode::AuthenticationFailed)?;
    let Some(channel) = state.channels.get_by_uuid(&claims.sfu_channel_uuid).await else {
        return Err(CurrentWebSocketCloseCode::AuthenticationFailed);
    };
    if channel.key().is_some() {
        return Err(CurrentWebSocketCloseCode::AuthenticationFailed);
    }
    Ok((channel, claims))
}

async fn close_writer(writer: &mut WsWriter, close_code: CurrentWebSocketCloseCode) {
    let _result = writer
        .send(Message::Close(Some(CloseFrame {
            code: u16::from(close_code),
            reason: "".into(),
        })))
        .await;
}

#[cfg(test)]
mod tests;
