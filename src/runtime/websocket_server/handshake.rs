//! websocket handshake admission boundary
//!
//! this module handles the cold path that turns an upgraded socket into a
//! [`ConnectedUser`]
//! the controller handles HTTP upgrade admission
//! the steady-state session loop handles authenticated signaling
//! this file covers the narrow interval where the socket is open but not yet
//! a room user
//!
//! admission has four ordered steps:
//!
//! - receive exactly one first-frame `auth` envelope before the configured auth timeout
//! - select the candidate room from the explicit auth payload channel or from a JWT room id
//! - verify the same JWT with the selected room key before trusting claims
//! - join the room and send the startup output returned by [`User::start`]
//!
//! the order is security-critical because decoded JWT contents can only select
//! a candidate room
//! identity, permissions and optional labels become trusted only after
//! room-key verification succeeds
//! current Odoo uses the explicit payload channel path and signs a legacy
//! room-scoped token without a room id claim
//!
//! rejection is terminal for the socket
//! helpers that return `None` have already recorded metrics and sent any close
//! frame that should reach the peer
//!
//! pre-auth capacity is released as soon as authentication succeeds and before
//! room admission allocates post-auth user resources

use std::{sync::Arc, time::Duration};

use axum::extract::ws::Message;
use futures_util::StreamExt;
use o_sfu_protocol::wire::{
    AuthPayload, ClientEnvelope, ClientMessage, UserId, UserPermissions, WebSocketCloseCode,
};
use serde::Deserialize;
use tokio::time::timeout;
use tracing::{Instrument, Span, debug, field, info, instrument, warn};

use super::{
    WsWriter,
    admission::PreAuthWebSocketPermit,
    close_writer,
    controller::{ConnectedUser, WebSocketServices, WsReader},
    io::send_user_output_bounded,
};
use crate::{
    application::user_session::User,
    core::server::room::{
        JoinUserRequest, Room, RoomManagerJoinError, UserOutboundQueueLimits, UserOutboundReceiver,
        UserOutboundSender,
    },
    runtime::{
        ConnectionId,
        auth::{self, RegisteredJwtClaims, WebSocketConnectClaims},
        telemetry::{
            self,
            schema::{event as telemetry_event, field as telemetry_field},
        },
        websocket_server::{MAX_CLIENT_FRAME_BYTES, decode_client_batch},
    },
};

/// legacy Odoo WebSocket claims scoped by the selected room key
///
/// current Odoo sends the room id as [`AuthPayload::channel`], not as a signed JWT claim
/// the JWT still authenticates the user because it is signed with the
/// room key selected during channel creation
/// this shape keeps that compatibility path explicit so the modern
/// [`WebSocketConnectClaims`] verifier can stay room-id-bound
#[derive(Deserialize)]
struct RoomScopedConnectClaims {
    /// registered JWT lifetime claims validated by the shared verifier
    #[serde(flatten)]
    registered: RegisteredJwtClaims,
    /// odoo RTC session id before runtime normalization
    #[serde(rename = "user_id", alias = "session_id")]
    user_id: UserId,
    /// optional display label forwarded into room membership
    #[serde(default)]
    label: Option<String>,
    /// optional room permissions forwarded into room membership
    #[serde(default)]
    permissions: Option<UserPermissions>,
}

struct HandshakeAuthentication {
    room: Arc<Room>,
    claims: WebSocketConnectClaims,
}

/// post-auth handoff from the handshake to the steady-state session loop
///
/// the room manager has already accepted this user
/// failures after this point must clean up this connection or hand it to the
/// caller
struct JoinedUser {
    room: Arc<Room>,
    user_id: UserId,
    connection_id: ConnectionId,
    outbound_rx: UserOutboundReceiver,
    user: User,
}

impl JoinedUser {
    fn into_connected(self, remote_address: Arc<str>) -> ConnectedUser {
        ConnectedUser {
            room: self.room,
            user_id: self.user_id,
            connection_id: self.connection_id,
            remote_address,
            outbound_rx: self.outbound_rx,
            user: self.user,
        }
    }
}

/// admit one upgraded socket into an authenticated room user
///
/// auth and join failures send terminal close frames
/// startup failures clean up the accepted room session before returning `None`
/// successful handshakes return the typed state consumed by the user loop
#[o_sfu_telemetry::measure_duration(
    metrics = "state.metrics",
    record = "record_ws_handshake_duration"
)]
pub(super) async fn establish_user(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    reader: &mut WsReader,
    remote_address: Arc<str>,
    pre_auth_permit: PreAuthWebSocketPermit,
) -> Option<ConnectedUser> {
    let authentication =
        receive_and_authenticate(state, writer, reader, remote_address.as_ref()).await?;
    drop(pre_auth_permit);
    let mut joined_user =
        join_user(state, writer, authentication, Arc::clone(&remote_address)).await?;
    state.metrics.record_ws_user_joined();
    record_session_span(&joined_user, remote_address.as_ref());
    let initialization_span = telemetry::activated_span(tracing::info_span!(
        "user.initialize",
        room_id = %joined_user.room.uuid(),
        user_id = ?joined_user.user_id,
        connection_id = ?joined_user.connection_id,
        remote_address = %remote_address
    ));
    initialize_user(state, writer, &mut joined_user, remote_address.as_ref())
        .instrument(initialization_span)
        .await?;
    Some(joined_user.into_connected(remote_address))
}

/// read the first client frame under the authentication timeout
///
/// `Ok(None)` means the peer closed before a frame was available
/// error variants carry the close code that should be reported by
/// `reject_handshake`
async fn receive_auth(
    state: &WebSocketServices,
    reader: &mut WsReader,
) -> Result<Option<AuthPayload>, Option<WebSocketCloseCode>> {
    match timeout(
        Duration::from_millis(state.auth.authentication_timeout_ms),
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

/// receive first-frame auth and convert parse failures into socket rejection
///
/// this helper is the last point where malformed unauthenticated input can fail
/// without room context
/// once it returns an `AuthPayload`, later failures are authentication or
/// room-admission failures
async fn receive_auth_or_reject(
    state: &WebSocketServices,
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

/// receive the auth frame and authenticate it against a room
///
/// the duration metric includes first-frame wait time plus JWT verification
/// because both contribute to unauthenticated socket pressure
#[o_sfu_telemetry::measure_duration(metrics = "state.metrics", record = "record_ws_auth_duration")]
async fn receive_and_authenticate(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    reader: &mut WsReader,
    remote_address: &str,
) -> Option<HandshakeAuthentication> {
    let auth_payload = receive_auth_or_reject(state, writer, reader, remote_address).await?;
    verify_auth_or_reject(state, writer, &auth_payload, remote_address).await
}

/// extract the protocol auth payload from the first WebSocket message
fn parse_auth_payload(message: Message) -> Result<AuthPayload, WebSocketCloseCode> {
    let payload = auth_payload_text(message)?;
    decode_auth_payload_text(&payload)
}

/// normalize the accepted first-frame wire shapes into UTF-8 JSON text
///
/// text and binary frames are accepted for compatibility with WebSocket clients
/// control frames before authentication are protocol errors except close, which
/// is treated as clean shutdown
fn auth_payload_text(message: Message) -> Result<String, WebSocketCloseCode> {
    match message {
        Message::Text(payload) => {
            if payload.len() > MAX_CLIENT_FRAME_BYTES {
                return Err(WebSocketCloseCode::ProtocolError);
            }
            Ok(payload.to_string())
        }
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

/// decode the first-frame batch and enforce the one-envelope auth contract
///
/// steady-state signaling can batch envelopes
/// auth cannot because no room user exists yet and any extra envelope would be
/// unauthenticated work
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

/// decodes the first WebSocket authentication frame
///
/// the frame must contain exactly one signaling envelope and that envelope must
/// be an auth message
/// steady-state client batches are decoded by `decode_client_batch`
///
/// # Errors
///
/// returns the close code that the WebSocket edge uses when the frame is not a
/// valid authentication batch
pub fn decode_auth_payload_text(payload: &str) -> Result<AuthPayload, WebSocketCloseCode> {
    let batch = decode_auth_batch(payload)?;
    extract_auth_envelope(batch)
}

/// convert the single-envelope batch into the only message allowed pre-auth
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

/// resolve a room and verify the JWT with that room's key
///
/// room selection happens before verification so current Odoo can continue to
/// send the room id in the auth payload
/// untrusted token fields are used only for candidate room lookup when the
/// payload has no channel
/// the returned claims have passed room-key verification
async fn verify_auth_payload(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
    remote_address: &str,
) -> Result<HandshakeAuthentication, WebSocketCloseCode> {
    let room = resolve_handshake_room(state, auth_payload).await?;
    let claims = authenticate_room_scoped_claims(
        &auth_payload.jwt,
        room.key(),
        room.uuid(),
        remote_address,
    )?;
    Ok(HandshakeAuthentication { room, claims })
}

/// select the candidate room without trusting user claims
///
/// explicit `AuthPayload.channel` is the current Odoo path
/// an absent channel falls back to the token room-id claim shape by decoding
/// the JWT payload only for room lookup
/// the caller must still run room-key verification before using any claim data
async fn resolve_handshake_room(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
) -> Result<Arc<Room>, WebSocketCloseCode> {
    let Some(explicit_room_id) = auth_payload.channel.as_deref() else {
        // this decode is only a room-directory lookup hint
        // trust starts after the token verifies with the selected room key
        let unverified_claims = auth::decode_unverified_claims::<WebSocketConnectClaims>(
            &auth_payload.jwt,
        )
        .map_err(|_error| {
            debug!("authentication payload did not select a room");
            WebSocketCloseCode::AuthFailed
        })?;
        return resolve_room_by_id(state, &unverified_claims.room_id).await;
    };
    let room = resolve_room_by_id(state, explicit_room_id).await?;
    Ok(room)
}

/// look up the selected room id in the live room directory
///
/// missing rooms are authentication failures because the client is trying to
/// join an admission boundary that no longer exists or never existed in this
/// process
async fn resolve_room_by_id(
    state: &WebSocketServices,
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

/// verify user claims with the selected room key
///
/// modern claims must contain the same room id that selected the room
/// legacy Odoo claims omit a room id and are accepted only after the token
/// verifies with the selected room key
/// the legacy path constructs canonical `WebSocketConnectClaims` so the rest of
/// the runtime sees one claim shape
fn authenticate_room_scoped_claims(
    token: &str,
    key: &str,
    room_id: &str,
    remote_address: &str,
) -> Result<WebSocketConnectClaims, WebSocketCloseCode> {
    if let Ok(mut claims) = auth::verify::<WebSocketConnectClaims>(token, key) {
        if claims.room_id != room_id {
            debug!(
                expected_room_id = room_id,
                claimed_room_id = claims.room_id,
                "room-scoped websocket token targeted the wrong room"
            );
            return Err(WebSocketCloseCode::AuthFailed);
        }
        claims.normalize_runtime_user_id();
        return Ok(claims);
    }

    // current Odoo signs user tokens with the room key but keeps the room id
    // outside the token in `AuthPayload.channel`
    let claims = auth::verify::<RoomScopedConnectClaims>(token, key).map_err(|_error| {
        warn!(
            remote_address,
            "failed to verify websocket auth token against the room-scoped key"
        );
        WebSocketCloseCode::AuthFailed
    })?;
    let mut claims = WebSocketConnectClaims {
        registered: claims.registered,
        room_id: room_id.to_owned(),
        user_id: claims.user_id,
        label: claims.label,
        permissions: claims.permissions,
    };
    claims.normalize_runtime_user_id();
    Ok(claims)
}

/// authenticate a parsed payload and turn auth failures into terminal rejection
async fn verify_auth_or_reject(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    auth_payload: &AuthPayload,
    remote_address: &str,
) -> Option<HandshakeAuthentication> {
    match verify_auth_payload(state, auth_payload, remote_address).await {
        Ok(result) => Some(result),
        Err(code) => {
            info!(
                event = telemetry_event::WS_AUTH_REJECTED,
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
    fields(room_id = %authentication.room.uuid(), user_id = ?authentication.claims.user_id)
)]
/// allocate room membership and the bounded outbound queue
///
/// after this succeeds the room owns a live user entry
/// later startup failure must call `cleanup_failed_session` with the returned
/// connection id before the socket is abandoned
async fn join_user(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    authentication: HandshakeAuthentication,
    remote_address: Arc<str>,
) -> Option<JoinedUser> {
    let HandshakeAuthentication { room, claims } = authentication;
    // the room must never accept a user without a bounded outbound sink
    // create the queue before join so admission is atomic from the room view
    let (outbound_tx, outbound_rx) = UserOutboundSender::channel_with_limits(
        UserOutboundQueueLimits::new(
            state.user.outbound_queue_capacity,
            state.user.outbound_queue_byte_capacity,
        ),
        Arc::clone(&state.metrics),
    );
    let user_id = claims.user_id.clone();
    let permissions = claims.permissions.unwrap_or_default();
    let join_result = state
        .room_manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: user_id.clone(),
                label: claims.label,
                permissions,
                sender: outbound_tx,
            },
            &state.media_transport,
        )
        .await;
    match join_result {
        Ok((room, connection_id)) => {
            let user = User::new(
                user_id.clone(),
                connection_id,
                Arc::clone(&remote_address),
                Arc::clone(&room),
                state.sfu_core.clone(),
            );
            let joined_user = JoinedUser {
                room,
                user_id,
                connection_id,
                outbound_rx,
                user,
            };
            info!(
                event = telemetry_event::WS_JOIN_SUCCEEDED,
                connection_id = ?joined_user.connection_id,
                "joined websocket user"
            );
            Some(joined_user)
        }
        Err(error) => {
            let close_code = match error {
                RoomManagerJoinError::RoomFull => WebSocketCloseCode::RoomFull,
                RoomManagerJoinError::MissingRoom | RoomManagerJoinError::RouterState => {
                    WebSocketCloseCode::AuthFailed
                }
            };
            warn!(
                event = telemetry_event::WS_JOIN_FAILED,
                ?user_id,
                remote_address = remote_address.as_ref(),
                ?error,
                close_code = u16::from(close_code),
                "rejecting websocket because the authenticated user could not join the room"
            );
            reject_handshake(
                state,
                Some(writer),
                Some(close_code),
                remote_address.as_ref(),
                "rejecting websocket during user join",
            )
            .await
        }
    }
}

/// attach authenticated room identity to the active tracing span
///
/// the upgrade span starts before authentication knows the room or user
/// this is the point where later logs can be correlated with the accepted room
/// user
fn record_session_span(joined_user: &JoinedUser, remote_address: &str) {
    let current_span = Span::current();
    current_span.record("room_id", field::display(joined_user.room.uuid()));
    current_span.record("user_id", field::debug(&joined_user.user_id));
    current_span.record("connection_id", field::debug(joined_user.connection_id));
    current_span.record(
        telemetry_field::REMOTE_ADDRESS,
        field::display(remote_address),
    );
    info!(
        event = telemetry_event::WS_USER_ESTABLISHED,
        connection_id = ?joined_user.connection_id,
        remote_address,
        "websocket user established"
    );
}

/// sends the startup payload that makes the accepted room user usable
///
/// `User::start` returns the welcome state and any initial offer
/// if startup fails or the socket cannot receive output, this helper closes
/// application state and removes accepted room membership before returning
/// `None`
#[o_sfu_telemetry::measure_duration(
    metrics = "state.metrics",
    record = "record_ws_user_initialize_duration"
)]
async fn initialize_user(
    state: &WebSocketServices,
    writer: &mut WsWriter,
    joined_user: &mut JoinedUser,
    remote_address: &str,
) -> Option<()> {
    let output = match joined_user.user.start().await {
        Ok(output) => output,
        Err(_error) => {
            warn!(
                event = telemetry_event::WS_JOIN_FAILED,
                user_id = ?joined_user.user_id,
                connection_id = ?joined_user.connection_id,
                remote_address,
                outcome = "user_initialize_failed",
                "failed to initialize websocket user"
            );
            state.metrics.record_ws_user_initialize_failure();
            joined_user.user.close().await;
            cleanup_failed_session(state, joined_user).await;
            return None;
        }
    };
    if send_user_output_bounded(writer, output).await.is_err() {
        debug!(
            user_id = ?joined_user.user_id,
            connection_id = ?joined_user.connection_id,
            "failed to send user startup payload"
        );
        state.metrics.record_ws_startup_send_failure();
        warn!(
            event = telemetry_event::WS_JOIN_FAILED,
            user_id = ?joined_user.user_id,
            connection_id = ?joined_user.connection_id,
            remote_address,
            outcome = "startup_send_failed",
            "failed to send websocket user startup payload"
        );
        joined_user.user.close().await;
        cleanup_failed_session(state, joined_user).await;
        return None;
    }
    Some(())
}

/// remove a user that joined the room but never reached steady state
///
/// cleanup is best effort here because the socket is already failing
/// later room reconciliation still owns any transport retry work exposed by the
/// close path
async fn cleanup_failed_session(state: &WebSocketServices, joined_user: &JoinedUser) {
    let _ = state
        .room_manager
        .close_session(
            joined_user.room.uuid(),
            &joined_user.user_id,
            joined_user.connection_id,
            &state.media_transport,
        )
        .await;
}

/// record terminal rejection and close the socket when there is a close code
///
/// returning `None` lets call sites use the helper directly in `Option` chains
async fn reject_handshake<T>(
    state: &WebSocketServices,
    writer: Option<&mut WsWriter>,
    close_code: Option<WebSocketCloseCode>,
    remote_address: &str,
    message: &str,
) -> Option<T> {
    state.metrics.record_ws_handshake_rejection(close_code);
    if let Some(code) = close_code {
        info!(
            event = telemetry_event::WS_HANDSHAKE_REJECTED,
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

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;
    use o_sfu_protocol::wire::WebSocketCloseCode;
    use serde_json::json;

    use super::{MAX_CLIENT_FRAME_BYTES, parse_auth_payload};

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
            Message::Text("x".repeat(MAX_CLIENT_FRAME_BYTES + 1).into()),
            Message::Binary(vec![b'x'; MAX_CLIENT_FRAME_BYTES + 1].into()),
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
