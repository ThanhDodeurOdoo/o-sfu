use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::{ConnectedSession, WsReader, close_writer};
use crate::runtime::{
    RuntimeState,
    channel::{Channel, ChannelManagerJoinError},
    stub_bus::{StubBusSession, WsWriter},
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

pub(super) async fn establish_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
) -> Option<ConnectedSession> {
    let credentials = match receive_credentials(state, reader).await {
        Ok(credentials) => credentials,
        Err(Some(code)) => {
            close_writer(writer, code).await;
            return None;
        }
        Err(None) => return None,
    };

    let (channel, claims) = match authenticate(state, credentials).await {
        Ok(result) => result,
        Err(code) => {
            close_writer(writer, code).await;
            return None;
        }
    };

    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let session_id = claims.session_id.clone();
    let (channel, connection_id) = match state
        .channels
        .join_session(
            channel.uuid(),
            session_id.clone(),
            claims.label,
            claims.permissions.unwrap_or_default(),
            outbound_tx,
            state.config.channel_size,
        )
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let close_code = match error {
                ChannelManagerJoinError::MissingChannel => {
                    CurrentWebSocketCloseCode::AuthenticationFailed
                }
                ChannelManagerJoinError::ChannelFull => CurrentWebSocketCloseCode::ChannelFull,
            };
            close_writer(writer, close_code).await;
            return None;
        }
    };

    if send_startup(&channel, writer).await.is_err() {
        state
            .channels
            .leave_session(channel.uuid(), &session_id, connection_id)
            .await;
        return None;
    }

    let mut stub_bus = StubBusSession::new(session_id.clone(), Arc::clone(&channel));
    if stub_bus.send_transport_bootstrap(writer).await.is_err() {
        state
            .channels
            .leave_session(channel.uuid(), &session_id, connection_id)
            .await;
        return None;
    }

    Some(ConnectedSession {
        channel,
        session_id,
        connection_id,
        outbound_rx,
        stub_bus,
    })
}

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
