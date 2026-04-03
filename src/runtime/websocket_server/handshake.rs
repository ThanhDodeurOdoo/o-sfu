use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{Span, field, info};

use super::{ConnectedSession, WsReader, close_writer};
use crate::runtime::{
    RuntimeState,
    channel::{Channel, ChannelManagerJoinError, SessionOutbound},
    stub_bus::{StubBusSession, WsWriter},
};
use crate::signaling::{
    auth::{self, WebSocketConnectClaims},
    current_protocol::{
        CurrentStartupPayload, CurrentWebSocketCloseCode, CurrentWebSocketCredentials,
    },
    shared::SessionId,
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
    let credentials = receive_credentials_or_reject(state, writer, reader).await?;
    let (channel, claims) = authenticate_session(state, writer, credentials).await?;
    let (session_id, outbound_rx, channel, connection_id) =
        join_authenticated_session(state, writer, channel, claims).await?;
    state.metrics.record_ws_session_joined();
    record_session_span(&channel, &session_id);
    let mut stub_bus = StubBusSession::new(
        session_id.clone(),
        Arc::clone(&channel),
        Arc::clone(&state.metrics),
        Arc::clone(&state.transport_adapter),
    );
    initialize_session(
        state,
        writer,
        &channel,
        &session_id,
        connection_id,
        &mut stub_bus,
    )
    .await?;
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
) -> Result<Option<CurrentWebSocketCredentials>, Option<CurrentWebSocketCloseCode>> {
    match timeout(
        Duration::from_millis(state.config.authentication_timeout_ms),
        reader.next(),
    )
    .await
    {
        Err(_) => Err(Some(CurrentWebSocketCloseCode::Timeout)),
        Ok(None) => Ok(None),
        Ok(Some(Err(_error))) => Err(Some(CurrentWebSocketCloseCode::Error)),
        Ok(Some(Ok(message))) => parse_credentials(message).map(Some).map_err(Some),
    }
}

async fn receive_credentials_or_reject(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
) -> Option<CurrentWebSocketCredentials> {
    match receive_credentials(state, reader).await {
        Ok(Some(credentials)) => {
            state.metrics.record_ws_handshake_credentials_received();
            Some(credentials)
        }
        Ok(None) => None,
        Err(close_code) => {
            reject_handshake(
                state,
                Some(writer),
                close_code,
                "rejecting websocket during credential receive",
            )
            .await
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

async fn authenticate_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    credentials: CurrentWebSocketCredentials,
) -> Option<(Arc<Channel>, WebSocketConnectClaims)> {
    match authenticate(state, credentials).await {
        Ok(result) => Some(result),
        Err(code) => {
            reject_handshake(
                state,
                Some(writer),
                Some(code),
                "rejecting websocket during authentication",
            )
            .await
        }
    }
}

async fn join_authenticated_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    channel: Arc<Channel>,
    claims: WebSocketConnectClaims,
) -> Option<(
    SessionId,
    mpsc::UnboundedReceiver<SessionOutbound>,
    Arc<Channel>,
    u64,
)> {
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let session_id = claims.session_id.clone();
    let join_result = state
        .channels
        .join_session(
            channel.uuid(),
            session_id.clone(),
            claims.label,
            claims.permissions.unwrap_or_default(),
            outbound_tx,
            state.config.channel_size,
        )
        .await;
    match join_result {
        Ok((channel, connection_id)) => Some((session_id, outbound_rx, channel, connection_id)),
        Err(error) => {
            let close_code = match error {
                ChannelManagerJoinError::ChannelFull => CurrentWebSocketCloseCode::ChannelFull,
                ChannelManagerJoinError::MissingChannel | ChannelManagerJoinError::RouterState => {
                    CurrentWebSocketCloseCode::AuthenticationFailed
                }
            };
            reject_handshake(
                state,
                Some(writer),
                Some(close_code),
                "rejecting websocket during session join",
            )
            .await
        }
    }
}

fn record_session_span(channel: &Channel, session_id: &SessionId) {
    let current_span = Span::current();
    current_span.record("channel_uuid", field::display(channel.uuid()));
    current_span.record("session_id", field::debug(session_id));
    info!("websocket session established");
}

async fn initialize_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    channel: &Arc<Channel>,
    session_id: &SessionId,
    connection_id: u64,
    stub_bus: &mut StubBusSession,
) -> Option<()> {
    if send_startup(channel, writer).await.is_err() {
        info!("failed to send startup payload");
        state.metrics.record_ws_startup_send_failure();
        cleanup_failed_session(state, channel, session_id, connection_id).await;
        return None;
    }
    if stub_bus.send_transport_bootstrap(writer).await.is_err() {
        info!("failed to send transport bootstrap");
        state.metrics.record_ws_transport_bootstrap_failure();
        cleanup_failed_session(state, channel, session_id, connection_id).await;
        return None;
    }
    Some(())
}

async fn cleanup_failed_session(
    state: &RuntimeState,
    channel: &Channel,
    session_id: &SessionId,
    connection_id: u64,
) {
    state
        .channels
        .leave_session(channel.uuid(), session_id, connection_id)
        .await;
}

async fn reject_handshake<T>(
    state: &RuntimeState,
    writer: Option<&mut WsWriter>,
    close_code: Option<CurrentWebSocketCloseCode>,
    message: &str,
) -> Option<T> {
    state.metrics.record_ws_handshake_rejection(close_code);
    if let Some(code) = close_code {
        info!(close_code = u16::from(code), "{message}");
        if let Some(writer) = writer {
            close_writer(writer, code).await;
        }
    }
    None
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
