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
};
use crate::runtime::{
    RuntimeState,
    channel::{Channel, ChannelManagerJoinError, SessionOutbound},
    stub_bus::{StubBusSession, WsWriter},
};
use crate::signaling::{
    auth::{self, WebSocketConnectClaims},
    current_protocol::{CurrentWebSocketCloseCode, CurrentWebSocketCredentials},
    native_protocol::{
        NativeAuthPayload, NativeClientEnvelope, NativeClientMessage, NativeEnvelopeBatch,
        NativeServerMessage, NativeWelcomePayload,
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
    let mut stub_bus = StubBusSession::new(
        session_id.clone(),
        connection_id,
        Arc::clone(&channel),
        Arc::clone(&state.metrics),
        state.transport_adapter.clone(),
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

async fn receive_auth(
    state: &RuntimeState,
    reader: &mut WsReader,
) -> Result<Option<NativeAuthPayload>, Option<CurrentWebSocketCloseCode>> {
    match timeout(
        Duration::from_millis(state.config.authentication_timeout_ms),
        reader.next(),
    )
    .await
    {
        Err(_) => Err(Some(CurrentWebSocketCloseCode::Timeout)),
        Ok(None) => Ok(None),
        Ok(Some(Err(_error))) => Err(Some(CurrentWebSocketCloseCode::Error)),
        Ok(Some(Ok(message))) => parse_auth_payload(message).map(Some).map_err(Some),
    }
}

async fn receive_auth_or_reject(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
) -> Option<NativeAuthPayload> {
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

fn parse_auth_payload(message: Message) -> Result<NativeAuthPayload, CurrentWebSocketCloseCode> {
    let payload = match message {
        Message::Text(payload) => payload.to_string(),
        Message::Binary(payload) => String::from_utf8(payload.to_vec())
            .map_err(|_error| CurrentWebSocketCloseCode::Error)?,
        Message::Close(_) => return Err(CurrentWebSocketCloseCode::Clean),
        Message::Ping(_) | Message::Pong(_) => return Err(CurrentWebSocketCloseCode::Error),
    };
    let batch = serde_json::from_str::<NativeEnvelopeBatch>(&payload)
        .map_err(|_error| CurrentWebSocketCloseCode::Error)?;
    if batch.len() != 1 {
        return Err(CurrentWebSocketCloseCode::Error);
    }
    let Some(envelope) = batch.into_iter().next() else {
        return Err(CurrentWebSocketCloseCode::Error);
    };
    match NativeClientEnvelope::decode(envelope)
        .map_err(|_error| CurrentWebSocketCloseCode::Error)?
    {
        NativeClientEnvelope::Message(NativeClientMessage::Auth(auth_payload)) => Ok(auth_payload),
        NativeClientEnvelope::Message(_)
        | NativeClientEnvelope::Request { .. }
        | NativeClientEnvelope::Response { .. } => Err(CurrentWebSocketCloseCode::Error),
    }
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
    if send_welcome(channel, session_id, writer).await.is_err() {
        info!("failed to send welcome payload");
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
    let _ = state
        .channels
        .leave_session(channel.uuid(), session_id, connection_id)
        .await;
    let _result = state
        .transport_adapter
        .close_session(&channel.transport_session_key(session_id, connection_id))
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

async fn send_welcome(
    channel: &Channel,
    session_id: &SessionId,
    writer: &mut WsWriter,
) -> Result<(), ()> {
    let welcome_payload = NativeServerMessage::Welcome(NativeWelcomePayload {
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
