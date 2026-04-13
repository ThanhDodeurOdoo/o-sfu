use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{Span, field, info};

use super::{
    close_writer,
    controller::{ConnectedSession, WsReader},
    session_protocol::SessionProtocol,
};
use crate::runtime::{
    RuntimeState,
    channel::{Channel, ChannelManagerJoinError, JoinSessionRequest, SessionOutbound},
    stub_bus::WsWriter,
};
use crate::signaling::{
    auth::{self, WebSocketConnectClaims},
    current_protocol::CurrentWebSocketCredentials,
    protocol::{
        AuthPayload, ClientEnvelope, ClientMessage, EnvelopeBatch, ServerMessage,
        WebSocketCloseCode, WelcomePayload,
    },
    shared::SessionId,
};

pub(super) async fn establish_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
) -> Option<ConnectedSession> {
    let auth_payload = receive_auth_or_reject(state, writer, reader).await?;
    let credentials = CurrentWebSocketCredentials {
        channel_uuid: auth_payload.channel,
        jwt: auth_payload.jwt,
    };
    let (channel, claims) = authenticate_session(state, writer, credentials).await?;
    let (session_id, outbound_rx, channel, connection_id) =
        join_authenticated_session(state, writer, channel, claims).await?;
    state.metrics.record_ws_session_joined();
    record_session_span(&channel, &session_id);
    let mut session_protocol = if state.config.enable_native_protocol
        && state
            .transport_adapter
            .uses_native_protocol_migration_path()
    {
        SessionProtocol::native(
            session_id.clone(),
            connection_id,
            Arc::clone(&channel),
            state.transport_adapter.clone(),
        )
    } else {
        SessionProtocol::legacy_stub_bus(
            session_id.clone(),
            connection_id,
            Arc::clone(&channel),
            Arc::clone(&state.metrics),
            state.transport_adapter.clone(),
        )
    };
    initialize_session(
        state,
        writer,
        &channel,
        &session_id,
        connection_id,
        &mut session_protocol,
    )
    .await?;
    Some(ConnectedSession {
        channel,
        session_id,
        connection_id,
        outbound_rx,
        session_protocol,
    })
}

async fn receive_auth(
    state: &RuntimeState,
    reader: &mut WsReader,
) -> Result<Option<AuthPayload>, Option<WebSocketCloseCode>> {
    match timeout(
        Duration::from_millis(state.config.authentication_timeout_ms),
        reader.next(),
    )
    .await
    {
        Err(_) => Err(Some(WebSocketCloseCode::AuthTimeout)),
        Ok(None) => Ok(None),
        Ok(Some(Err(_error))) => Err(Some(WebSocketCloseCode::Error)),
        Ok(Some(Ok(message))) => parse_auth_payload(message).map(Some).map_err(Some),
    }
}

async fn receive_auth_or_reject(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
) -> Option<AuthPayload> {
    match receive_auth(state, reader).await {
        Ok(Some(auth_payload)) => {
            state.metrics.record_ws_handshake_credentials_received();
            Some(auth_payload)
        }
        Ok(None) => None,
        Err(close_code) => {
            reject_handshake(
                state,
                Some(writer),
                close_code,
                "rejecting websocket during auth receive",
            )
            .await
        }
    }
}

fn parse_auth_payload(message: Message) -> Result<AuthPayload, WebSocketCloseCode> {
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| WebSocketCloseCode::ProtocolError)?,
        Message::Close(_) => return Err(WebSocketCloseCode::Clean),
        Message::Ping(_) | Message::Pong(_) => {
            return Err(WebSocketCloseCode::ProtocolError);
        }
    };
    let batch = serde_json::from_str::<EnvelopeBatch>(&payload)
        .map_err(|_error| WebSocketCloseCode::ProtocolError)?;
    if batch.len() != 1 {
        return Err(WebSocketCloseCode::ProtocolError);
    }
    let Some(envelope) = batch.into_iter().next() else {
        return Err(WebSocketCloseCode::ProtocolError);
    };
    match ClientEnvelope::decode(envelope).map_err(|_error| WebSocketCloseCode::ProtocolError)? {
        ClientEnvelope::Message(ClientMessage::Auth(auth_payload)) => Ok(auth_payload),
        ClientEnvelope::Message(_)
        | ClientEnvelope::Request { .. }
        | ClientEnvelope::Response { .. } => Err(WebSocketCloseCode::ProtocolError),
    }
}

async fn authenticate(
    state: &RuntimeState,
    credentials: CurrentWebSocketCredentials,
) -> Result<(Arc<Channel>, WebSocketConnectClaims), WebSocketCloseCode> {
    if let Some(channel_uuid) = credentials.channel_uuid.as_deref() {
        let Some(channel) = state.channels.get_by_uuid(channel_uuid).await else {
            return Err(WebSocketCloseCode::AuthFailed);
        };
        let key = channel.key().unwrap_or(&state.config.auth_key);
        let claims = auth::verify::<WebSocketConnectClaims>(&credentials.jwt, key)
            .map_err(|_error| WebSocketCloseCode::AuthFailed)?;
        if claims.sfu_channel_uuid != channel_uuid {
            return Err(WebSocketCloseCode::AuthFailed);
        }
        return Ok((channel, claims));
    }

    let claims = auth::verify::<WebSocketConnectClaims>(&credentials.jwt, &state.config.auth_key)
        .map_err(|_error| WebSocketCloseCode::AuthFailed)?;
    let Some(channel) = state.channels.get_by_uuid(&claims.sfu_channel_uuid).await else {
        return Err(WebSocketCloseCode::AuthFailed);
    };
    if channel.key().is_some() {
        return Err(WebSocketCloseCode::AuthFailed);
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
            JoinSessionRequest {
                session_id: session_id.clone(),
                label: claims.label,
                permissions: claims.permissions.unwrap_or_default(),
                sender: outbound_tx,
            },
            &state.transport_adapter,
        )
        .await;
    match join_result {
        Ok((channel, connection_id)) => Some((session_id, outbound_rx, channel, connection_id)),
        Err(error) => {
            let close_code = match error {
                ChannelManagerJoinError::ChannelFull => WebSocketCloseCode::ChannelFull,
                ChannelManagerJoinError::MissingChannel | ChannelManagerJoinError::RouterState => {
                    WebSocketCloseCode::AuthFailed
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
    session_protocol: &mut SessionProtocol,
) -> Option<()> {
    if send_welcome(channel, session_id, writer).await.is_err() {
        info!("failed to send welcome payload");
        state.metrics.record_ws_startup_send_failure();
        cleanup_failed_session(state, channel, session_id, connection_id).await;
        return None;
    }
    if session_protocol.initialize(writer).await.is_err() {
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
    let _ = state
        .channels
        .leave_session(
            channel.uuid(),
            session_id,
            connection_id,
            &state.transport_adapter,
        )
        .await;
    let _result = state
        .transport_adapter
        .close_session(&channel.transport_session_key(session_id, connection_id))
        .await;
}

async fn reject_handshake<T>(
    state: &RuntimeState,
    writer: Option<&mut WsWriter>,
    close_code: Option<WebSocketCloseCode>,
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

async fn send_welcome(
    channel: &Channel,
    session_id: &SessionId,
    writer: &mut WsWriter,
) -> Result<(), ()> {
    let welcome_payload = ServerMessage::Welcome(WelcomePayload {
        features: channel.available_features(),
        recording: channel.recording_state().await,
        peers: channel.peer_snapshots_except(session_id).await,
    });
    let welcome_envelope = welcome_payload.into_envelope().map_err(|_error| ())?;
    let welcome_json = serde_json::to_string(&vec![welcome_envelope]).map_err(|_error| ())?;
    writer
        .send(Message::Text(welcome_json.into()))
        .await
        .map_err(|_error| ())
}
