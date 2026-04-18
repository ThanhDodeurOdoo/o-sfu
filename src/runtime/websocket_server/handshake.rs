use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use o_sfu_protocol::{
    shared::{SessionId, SessionPermissions},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, ServerMessage, WebSocketCloseCode,
        WelcomePayload,
    },
};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{Span, debug, field, info, warn};

use super::{
    WsWriter, close_writer,
    controller::{ConnectedSession, WsReader},
    session_protocol::SessionProtocol,
};
use crate::runtime::{
    RuntimeState,
    channel::{Channel, ChannelManagerJoinError, JoinSessionRequest, SessionOutbound},
};
use crate::signaling::{
    auth::{self, RegisteredJwtClaims, WebSocketConnectClaims},
    client_batch::{MAX_CLIENT_FRAME_BYTES, decode_client_batch},
};

#[derive(Deserialize)]
struct LegacyChannelScopedConnectClaims {
    #[serde(flatten)]
    registered: RegisteredJwtClaims,
    #[serde(rename = "session_id")]
    session_id: SessionId,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    permissions: Option<SessionPermissions>,
}

// TODO: needs documentation:
pub(super) async fn establish_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
) -> Option<ConnectedSession> {
    let auth_payload = receive_auth_or_reject(state, writer, reader).await?;
    let (channel, claims) = authenticate_session(state, writer, &auth_payload).await?;
    let (session_id, outbound_rx, channel, connection_id) =
        join_authenticated_session(state, writer, channel, claims).await?;
    state.metrics.record_ws_session_joined();
    record_session_span(&channel, &session_id);
    let mut session_protocol = SessionProtocol::new(
        session_id.clone(),
        connection_id,
        Arc::clone(&channel),
        state.transport_adapter.clone(),
        Arc::clone(&state.metrics),
    );
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
        Err(_) => {
            debug!("timed out waiting for initial websocket auth payload");
            Err(Some(WebSocketCloseCode::AuthTimeout))
        }
        Ok(None) => Ok(None),
        Ok(Some(Err(_error))) => {
            debug!("websocket reader returned an error before authentication completed");
            Err(Some(WebSocketCloseCode::Error))
        }
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
    let payload = auth_payload_text(message)?;
    let batch = decode_auth_batch(&payload)?;
    extract_auth_envelope(batch)
}

fn auth_payload_text(message: Message) -> Result<String, WebSocketCloseCode> {
    match message {
        Message::Text(payload) => Ok(payload.to_string()),
        Message::Binary(payload) => {
            if payload.len() > MAX_CLIENT_FRAME_BYTES {
                return Err(WebSocketCloseCode::ProtocolError);
            }
            String::from_utf8(payload.to_vec()).map_err(|_error| WebSocketCloseCode::ProtocolError)
        }
        Message::Close(_) => Err(WebSocketCloseCode::Clean),
        Message::Ping(_) | Message::Pong(_) => Err(WebSocketCloseCode::ProtocolError),
    }
}

fn decode_auth_batch(payload: &str) -> Result<Vec<ClientEnvelope>, WebSocketCloseCode> {
    let batch = decode_client_batch(payload).map_err(|_error| WebSocketCloseCode::ProtocolError)?;
    if batch.len() != 1 {
        warn!(
            batch_len = batch.len(),
            "authentication batch must contain exactly one envelope"
        );
        return Err(WebSocketCloseCode::ProtocolError);
    }
    Ok(batch)
}

fn extract_auth_envelope(batch: Vec<ClientEnvelope>) -> Result<AuthPayload, WebSocketCloseCode> {
    let Some(envelope) = batch.into_iter().next() else {
        return Err(WebSocketCloseCode::ProtocolError);
    };
    match envelope {
        ClientEnvelope::Message(ClientMessage::Auth(auth_payload)) => Ok(auth_payload),
        ClientEnvelope::Message(_)
        | ClientEnvelope::Request { .. }
        | ClientEnvelope::Response { .. } => {
            debug!("first websocket envelope was not an auth message");
            Err(WebSocketCloseCode::ProtocolError)
        }
    }
}

async fn authenticate(
    state: &RuntimeState,
    auth_payload: &AuthPayload,
) -> Result<(Arc<Channel>, WebSocketConnectClaims), WebSocketCloseCode> {
    if let Some(channel_uuid) = auth_payload.channel.as_deref() {
        let Some(channel) = state.channels.get_by_uuid(channel_uuid).await else {
            debug!(
                channel_uuid,
                "authentication referenced an unknown explicit channel"
            );
            return Err(WebSocketCloseCode::AuthFailed);
        };
        let key = channel.key().unwrap_or(&state.config.auth_key);
        let claims = authenticate_channel_scoped_claims(&auth_payload.jwt, key, channel_uuid)?;
        return Ok((channel, claims));
    }

    let claims = auth::verify::<WebSocketConnectClaims>(&auth_payload.jwt, &state.config.auth_key)
        .map_err(|_error| {
            warn!("failed to verify websocket auth token against the global key");
            WebSocketCloseCode::AuthFailed
        })?;
    let Some(channel) = state.channels.get_by_uuid(&claims.sfu_channel_uuid).await else {
        debug!(
            channel_uuid = claims.sfu_channel_uuid,
            "verified websocket token referenced a missing channel"
        );
        return Err(WebSocketCloseCode::AuthFailed);
    };
    if channel.key().is_some() {
        debug!(
            channel_uuid = claims.sfu_channel_uuid,
            "global-key websocket token targeted a channel that requires a scoped key"
        );
        return Err(WebSocketCloseCode::AuthFailed);
    }
    Ok((channel, claims))
}

fn authenticate_channel_scoped_claims(
    token: &str,
    key: &str,
    channel_uuid: &str,
) -> Result<WebSocketConnectClaims, WebSocketCloseCode> {
    if let Ok(claims) = auth::verify::<WebSocketConnectClaims>(token, key) {
        if claims.sfu_channel_uuid != channel_uuid {
            debug!(
                expected_channel_uuid = channel_uuid,
                claimed_channel_uuid = claims.sfu_channel_uuid,
                "channel-scoped websocket token targeted the wrong channel"
            );
            return Err(WebSocketCloseCode::AuthFailed);
        }
        return Ok(claims);
    }

    let claims =
        auth::verify::<LegacyChannelScopedConnectClaims>(token, key).map_err(|_error| {
            warn!("failed to verify websocket auth token against the channel-scoped key");
            WebSocketCloseCode::AuthFailed
        })?;
    Ok(WebSocketConnectClaims {
        registered: claims.registered,
        sfu_channel_uuid: channel_uuid.to_owned(),
        session_id: claims.session_id,
        label: claims.label,
        permissions: claims.permissions,
    })
}

async fn authenticate_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    auth_payload: &AuthPayload,
) -> Option<(Arc<Channel>, WebSocketConnectClaims)> {
    match authenticate(state, auth_payload).await {
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
            RuntimeState::session_cleanup_policy(),
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
            debug!(
                ?session_id,
                ?error,
                close_code = u16::from(close_code),
                "rejecting websocket because the authenticated session could not join the channel"
            );
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
        debug!(?session_id, connection_id, "failed to send welcome payload");
        state.metrics.record_ws_startup_send_failure();
        cleanup_failed_session(state, channel, session_id, connection_id).await;
        return None;
    }
    if session_protocol.initialize(writer).await.is_err() {
        warn!(
            ?session_id,
            connection_id, "failed to initialize websocket session protocol"
        );
        state.metrics.record_ws_session_initialize_failure();
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
        .close_session(
            channel.uuid(),
            session_id,
            connection_id,
            &state.transport_adapter,
            RuntimeState::session_cleanup_policy(),
        )
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

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;
    use serde_json::json;

    use super::parse_auth_payload;
    use o_sfu_protocol::signaling::WebSocketCloseCode;

    #[test]
    fn parse_auth_payload_accepts_single_auth_message() {
        let frame = Message::Text(
            serde_json::to_string(&vec![json!({
                "t": "auth",
                "p": {
                    "jwt": "token",
                    "channel": "channel-1",
                },
            })])
            .unwrap_or_default()
            .into(),
        );

        let payload = parse_auth_payload(frame);
        assert!(payload.is_ok());
        let Some(payload) = payload.ok() else {
            return;
        };
        assert_eq!(payload.jwt, "token");
        assert_eq!(payload.channel.as_deref(), Some("channel-1"));
    }

    #[test]
    fn parse_auth_payload_rejects_generated_non_auth_first_frames() {
        let cases = [
            Message::Text("not-json".into()),
            Message::Text(
                serde_json::to_string(&vec![json!({
                    "t": "info",
                    "p": {},
                })])
                .unwrap_or_default()
                .into(),
            ),
            Message::Text(
                serde_json::to_string(&vec![
                    json!({
                        "t": "auth",
                        "p": { "jwt": "token-a" },
                    }),
                    json!({
                        "t": "auth",
                        "p": { "jwt": "token-b" },
                    }),
                ])
                .unwrap_or_default()
                .into(),
            ),
            Message::Binary(vec![0xff].into()),
            Message::Ping(Vec::new().into()),
        ];

        for frame in cases {
            assert_eq!(
                parse_auth_payload(frame),
                Err(WebSocketCloseCode::ProtocolError)
            );
        }
    }

    #[test]
    fn parse_auth_payload_treats_close_frame_as_clean_shutdown() {
        assert_eq!(
            parse_auth_payload(Message::Close(None)),
            Err(WebSocketCloseCode::Clean)
        );
    }
}
