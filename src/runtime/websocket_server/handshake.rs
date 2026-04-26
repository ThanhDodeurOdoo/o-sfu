//! WebSocket Handshake and User Establishment
//!
//! This module handles the transition from a raw, newly-upgraded WebSocket
//! into an authenticated and fully established RTC user.
//! (very similar flow that the odoo/sfu way)
//!
//! 1. **Receive Authentication**: Waits for the very first frame from the client, which
//!    must be a valid `auth` message.
//!
//! 2. **Authentication**: Validates the JWT against either
//!    the global key or a room-specific key (like the old SFU),
//!    to validate the  client's identity and permissions for the target room.
//!    (the JWT is signed by the Odoo server that owns the room)
//!
//! 3. **Room Admission**: Requests the `RoomManager` to admit the client into
//!    the `Room`. This allocates a unique connection ID and sets up the
//!    outbound message routing queues.
//!
//! 4. **User Initialization**: the server send a complete state snapshot
//!    (the `Welcome` message) back to the client, including the current peers and
//!    room features. It also initializes the `SessionProtocol`, wich coordinates
//!    with the `TransportAdapter` to prepare the backend WebRTC transport.

use std::{sync::Arc, time::Duration};

use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use o_sfu_protocol::{
    shared::{UserId, UserPermissions},
    signaling::{
        AuthPayload, ClientEnvelope, ClientMessage, ServerMessage, WebSocketCloseCode,
        WelcomePayload,
    },
};
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tracing::{Instrument, Span, debug, field, info, instrument, warn};

use super::{
    WsWriter, close_writer,
    controller::{ConnectedSession, WsReader},
    session_protocol::SessionProtocol,
};
use crate::runtime::{
    ConnectionId, RuntimeState,
    auth::{self, RegisteredJwtClaims, WebSocketConnectClaims},
    room::{JoinUserRequest, Room, RoomManagerJoinError, UserOutbound},
    telemetry,
    websocket_server::{MAX_CLIENT_FRAME_BYTES, decode_client_batch},
};

#[derive(Deserialize)]
struct RoomScopedConnectClaims {
    #[serde(flatten)]
    registered: RegisteredJwtClaims,
    #[serde(rename = "user_id", alias = "session_id")]
    user_id: UserId,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    permissions: Option<UserPermissions>,
}

enum HandshakeRoomResolution {
    ExplicitRoom(Arc<Room>),
    GlobalClaims {
        room: Arc<Room>,
        claims: Box<WebSocketConnectClaims>,
    },
}

/// Admit one upgraded socket into an authenticated room user.
///
/// The first client frame must be exactly one `auth` envelope. On success this function
/// authenticates the JWT, joins the target room, sends the initial welcome snapshot,
/// and initializes the post-auth protocol state that will drive the first offer/answer
/// exchange
///
/// Returning `None` means the caller should stop processing the socket imediately. In
/// rejection cases this function is also responsible for sending the appropriate close
/// frame so callrs do not duplicate handshake failure handling.
#[o_sfu_telemetry::measure_duration(
    metrics = "state.metrics",
    record = "record_ws_handshake_duration"
)]
pub(super) async fn establish_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
    remote_address: Arc<str>,
) -> Option<ConnectedSession> {
    let (room, claims) =
        authenticate_handshake_session(state, writer, reader, remote_address.as_ref()).await?;
    let (user_id, outbound_rx, room, connection_id) =
        join_user(state, writer, room, claims, remote_address.as_ref()).await?;
    state.metrics.record_ws_user_joined();
    record_session_span(&room, &user_id, connection_id, remote_address.as_ref());
    let mut session_protocol = SessionProtocol::new(
        user_id.clone(),
        connection_id,
        Arc::clone(&remote_address),
        Arc::clone(&room),
        state.application.media_core(),
        Arc::clone(&state.metrics),
    );
    initialize_session(
        state,
        writer,
        &room,
        &user_id,
        connection_id,
        &mut session_protocol,
        remote_address.as_ref(),
    )
    .instrument(telemetry::activated_span(tracing::info_span!(
        "user.initialize",
        room_id = %room.uuid(),
        user_id = ?user_id,
        connection_id = ?connection_id,
        remote_address = %remote_address
    )))
    .await?;
    Some(ConnectedSession {
        room,
        user_id,
        connection_id,
        remote_address,
        outbound_rx,
        session_protocol,
    })
}

async fn receive_auth(
    state: &RuntimeState,
    reader: &mut WsReader,
) -> Result<Option<AuthPayload>, Option<WebSocketCloseCode>> {
    match timeout(
        Duration::from_millis(state.websocket_options.auth.authentication_timeout_ms),
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
    remote_address: &str,
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
                remote_address,
                "rejecting websocket during auth receive",
            )
            .await
        }
    }
}

#[o_sfu_telemetry::measure_duration(metrics = "state.metrics", record = "record_ws_auth_duration")]
async fn authenticate_handshake_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    reader: &mut WsReader,
    remote_address: &str,
) -> Option<(Arc<Room>, WebSocketConnectClaims)> {
    let auth_payload = receive_auth_or_reject(state, writer, reader, remote_address).await?;
    authenticate_session(state, writer, &auth_payload, remote_address).await
}

fn parse_auth_payload(message: Message) -> Result<AuthPayload, WebSocketCloseCode> {
    let payload = auth_payload_text(message)?;
    decode_auth_payload_text(&payload)
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

pub(crate) fn decode_auth_payload_text(payload: &str) -> Result<AuthPayload, WebSocketCloseCode> {
    let batch = decode_auth_batch(payload)?;
    extract_auth_envelope(batch)
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
    remote_address: &str,
) -> Result<(Arc<Room>, WebSocketConnectClaims), WebSocketCloseCode> {
    match resolve_handshake_room(state, auth_payload, remote_address).await? {
        HandshakeRoomResolution::ExplicitRoom(room) => {
            let room_id = room.uuid();
            let claims = authenticate_room_scoped_claims(
                &auth_payload.jwt,
                room.key().unwrap_or(&state.websocket_options.auth.key),
                room_id,
                remote_address,
            )?;
            Ok((room, claims))
        }
        HandshakeRoomResolution::GlobalClaims { room, claims } => Ok((room, *claims)),
    }
}

async fn resolve_handshake_room(
    state: &RuntimeState,
    auth_payload: &AuthPayload,
    remote_address: &str,
) -> Result<HandshakeRoomResolution, WebSocketCloseCode> {
    let Some(explicit_room_id) = auth_payload.channel.as_deref() else {
        let claims = auth::verify::<WebSocketConnectClaims>(
            &auth_payload.jwt,
            &state.websocket_options.auth.key,
        )
        .map_err(|_error| {
            warn!(
                remote_address,
                "failed to verify websocket auth token against the global key"
            );
            WebSocketCloseCode::AuthFailed
        })?;
        let room = resolve_global_claims_room(state, &claims, remote_address).await?;
        return Ok(HandshakeRoomResolution::GlobalClaims {
            room,
            claims: Box::new(claims),
        });
    };
    let room = resolve_explicit_room(state, explicit_room_id).await?;
    Ok(HandshakeRoomResolution::ExplicitRoom(room))
}

async fn resolve_explicit_room(
    state: &RuntimeState,
    room_id: &str,
) -> Result<Arc<Room>, WebSocketCloseCode> {
    state
        .room_manager
        .get_by_uuid(room_id)
        .await
        .ok_or_else(|| {
            debug!(
                room_id,
                "authentication referenced an unknown explicit room"
            );
            WebSocketCloseCode::AuthFailed
        })
}

async fn resolve_global_claims_room(
    state: &RuntimeState,
    claims: &WebSocketConnectClaims,
    _remote_address: &str,
) -> Result<Arc<Room>, WebSocketCloseCode> {
    let Some(room) = state.room_manager.get_by_uuid(&claims.room_id).await else {
        debug!(
            room_id = claims.room_id,
            "verified websocket token referenced a missing room"
        );
        return Err(WebSocketCloseCode::AuthFailed);
    };
    if room.key().is_some() {
        debug!(
            room_id = claims.room_id,
            "global-key websocket token targeted a room that requires a scoped key"
        );
        return Err(WebSocketCloseCode::AuthFailed);
    }
    Ok(room)
}

fn authenticate_room_scoped_claims(
    token: &str,
    key: &str,
    room_id: &str,
    remote_address: &str,
) -> Result<WebSocketConnectClaims, WebSocketCloseCode> {
    if let Ok(claims) = auth::verify::<WebSocketConnectClaims>(token, key) {
        if claims.room_id != room_id {
            debug!(
                expected_room_id = room_id,
                claimed_room_id = claims.room_id,
                "room-scoped websocket token targeted the wrong room"
            );
            return Err(WebSocketCloseCode::AuthFailed);
        }
        return Ok(claims);
    }

    let claims = auth::verify::<RoomScopedConnectClaims>(token, key).map_err(|_error| {
        warn!(
            remote_address,
            "failed to verify websocket auth token against the room-scoped key"
        );
        WebSocketCloseCode::AuthFailed
    })?;
    Ok(WebSocketConnectClaims {
        registered: claims.registered,
        room_id: room_id.to_owned(),
        user_id: claims.user_id,
        label: claims.label,
        permissions: claims.permissions,
    })
}

async fn authenticate_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    auth_payload: &AuthPayload,
    remote_address: &str,
) -> Option<(Arc<Room>, WebSocketConnectClaims)> {
    match authenticate(state, auth_payload, remote_address).await {
        Ok(result) => Some(result),
        Err(code) => {
            info!(
                event = telemetry::schema::event::WS_AUTH_REJECTED,
                close_code = u16::from(code),
                remote_address,
                "rejected websocket authentication"
            );
            reject_handshake(
                state,
                Some(writer),
                Some(code),
                remote_address,
                "rejecting websocket during authentication",
            )
            .await
        }
    }
}

#[instrument(
    name = "room.join",
    skip_all,
    fields(room_id = %room.uuid(), user_id = ?claims.user_id)
)]
async fn join_user(
    state: &RuntimeState,
    writer: &mut WsWriter,
    room: Arc<Room>,
    claims: WebSocketConnectClaims,
    remote_address: &str,
) -> Option<(
    UserId,
    mpsc::UnboundedReceiver<UserOutbound>,
    Arc<Room>,
    ConnectionId,
)> {
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
    let user_id = claims.user_id.clone();
    let join_result = state
        .room_manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: user_id.clone(),
                label: claims.label,
                permissions: claims.permissions.unwrap_or_default(),
                sender: outbound_tx,
            },
            &state.transport_adapter,
        )
        .await;
    match join_result {
        Ok((room, connection_id)) => {
            info!(
                event = telemetry::schema::event::WS_JOIN_SUCCEEDED,
                connection_id = ?connection_id,
                "joined websocket user"
            );
            Some((user_id, outbound_rx, room, connection_id))
        }
        Err(error) => {
            let close_code = match error {
                RoomManagerJoinError::RoomFull => WebSocketCloseCode::RoomFull,
                RoomManagerJoinError::MissingRoom | RoomManagerJoinError::RouterState => {
                    WebSocketCloseCode::AuthFailed
                }
            };
            warn!(
                event = telemetry::schema::event::WS_JOIN_FAILED,
                ?user_id,
                remote_address,
                ?error,
                close_code = u16::from(close_code),
                "rejecting websocket because the authenticated user could not join the room"
            );
            reject_handshake(
                state,
                Some(writer),
                Some(close_code),
                remote_address,
                "rejecting websocket during user join",
            )
            .await
        }
    }
}

fn record_session_span(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    remote_address: &str,
) {
    let current_span = Span::current();
    current_span.record("room_id", field::display(room.uuid()));
    current_span.record("user_id", field::debug(user_id));
    current_span.record("connection_id", field::debug(connection_id));
    current_span.record(
        telemetry::schema::field::REMOTE_ADDRESS,
        field::display(remote_address),
    );
    info!(
        event = telemetry::schema::event::WS_USER_ESTABLISHED,
        connection_id = ?connection_id,
        remote_address,
        "websocket user established"
    );
}

#[o_sfu_telemetry::measure_duration(
    metrics = "state.metrics",
    record = "record_ws_user_initialize_duration"
)]
async fn initialize_session(
    state: &RuntimeState,
    writer: &mut WsWriter,
    room: &Arc<Room>,
    user_id: &UserId,
    connection_id: ConnectionId,
    session_protocol: &mut SessionProtocol,
    remote_address: &str,
) -> Option<()> {
    if send_welcome(room, user_id, writer).await.is_err() {
        debug!(
            ?user_id,
            connection_id = ?connection_id,
            "failed to send welcome payload"
        );
        state.metrics.record_ws_startup_send_failure();
        warn!(
            event = telemetry::schema::event::WS_JOIN_FAILED,
            user_id = ?user_id,
            connection_id = ?connection_id,
            remote_address,
            outcome = "welcome_send_failed",
            "failed to send websocket welcome payload"
        );
        session_protocol.finish().await;
        cleanup_failed_session(state, room, user_id, connection_id).await;
        return None;
    }
    if session_protocol.initialize(writer).await.is_err() {
        warn!(
            event = telemetry::schema::event::WS_JOIN_FAILED,
            ?user_id,
            connection_id = ?connection_id,
            remote_address,
            outcome = "user_initialize_failed",
            "failed to initialize websocket user protocol"
        );
        state.metrics.record_ws_user_initialize_failure();
        session_protocol.finish().await;
        cleanup_failed_session(state, room, user_id, connection_id).await;
        return None;
    }
    Some(())
}

async fn cleanup_failed_session(
    state: &RuntimeState,
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
) {
    let _ = state
        .room_manager
        .close_session(
            room.uuid(),
            user_id,
            connection_id,
            &state.transport_adapter,
        )
        .await;
}

async fn reject_handshake<T>(
    state: &RuntimeState,
    writer: Option<&mut WsWriter>,
    close_code: Option<WebSocketCloseCode>,
    remote_address: &str,
    message: &str,
) -> Option<T> {
    state.metrics.record_ws_handshake_rejection(close_code);
    if let Some(code) = close_code {
        info!(
            event = telemetry::schema::event::WS_HANDSHAKE_REJECTED,
            close_code = u16::from(code),
            remote_address,
            "{message}"
        );
        if let Some(writer) = writer {
            close_writer(writer, code).await;
        }
    }
    None
}

async fn send_welcome(room: &Room, user_id: &UserId, writer: &mut WsWriter) -> Result<(), ()> {
    let welcome_payload = ServerMessage::Welcome(WelcomePayload {
        features: room.available_features(),
        recording: room.recording_state().await,
        peers: room.peer_snapshots_except(user_id).await,
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
    use o_sfu_protocol::signaling::WebSocketCloseCode;
    use serde_json::json;

    use super::parse_auth_payload;

    #[test]
    fn parse_auth_payload_accepts_single_auth_message() {
        let frame = Message::Text(
            serde_json::to_string(&vec![json!({
                "t": "auth",
                "p": {
                    "jwt": "token",
                    "channel": "room-1",
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
        assert_eq!(payload.channel.as_deref(), Some("room-1"));
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
