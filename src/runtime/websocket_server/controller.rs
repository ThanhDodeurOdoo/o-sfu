//! WebSocket Connection Lifecycle
//!
//! This module manages the lifecycle of a client's WebSocket connection and its
//! relationship to the underlaying RTC sialing user:
//!
//! 1. **Creation**: A connection begins when the Axum router accepts an HTTP upgrade
//!    request in the `upgrade` handler. It is then split into a read and write stream
//!    as a raw, unauthenticated socket.
//!
//! 2. **Upgrade to RTC User**: The raw socket is passed to `handshake::establish_session`,
//!    where it waits for an `auth` envelope from the client. After JWT validation,
//!    the connection is admitted into a `Room`. At this point, the connection is upgraded
//!    into a full RTC user: a `SessionProtocol` is created to handle WebRTC state, and
//!    the `TransportAdapter` initializes the backend WebRTC transport resources.
//!
//! 3. **Steady State**: The connection enters the steady-state `session_loop::run`, continuously
//!    polling for incoming WebSocket frames to feed the `SessionProtocol` and outbound
//!    room events to send back to the client.
//!
//! 4. **Removal**: When the user loop terminates (due to client disconnect, timeout, or
//!    protocol error), the connection is cleaned up. The `close_session` method is invoked
//!    on the `RoomManager`, which removes the user from the room and signasl
//!    the `TransportAdapter` to tear down the associated WebRTC media resources.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{
        ConnectInfo, Extension, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::Response,
};
use futures_util::{SinkExt, StreamExt, stream::SplitStream};
use o_sfu_protocol::{shared::UserId, signaling::WebSocketCloseCode};
use tokio::sync::mpsc;
use tracing::{Instrument, Span, field, info};

use super::{WsWriter, session_protocol::SessionProtocol};
use crate::runtime::{
    ConnectionId, RuntimeState,
    request_origin::resolve_remote_address,
    room::{Room, UserOutbound},
    telemetry,
};

pub(super) type WsReader = SplitStream<WebSocket>;

pub(super) struct ConnectedSession {
    pub(super) room: Arc<Room>,
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) remote_address: Arc<str>,
    pub(super) outbound_rx: mpsc::UnboundedReceiver<UserOutbound>,
    pub(super) session_protocol: SessionProtocol,
}

pub(crate) async fn upgrade(
    State(state): State<RuntimeState>,
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let remote_address = Arc::<str>::from(resolve_remote_address(
        &headers,
        state.websocket_options.trust_proxy_headers,
        connect_info.map(|Extension(ConnectInfo(addr))| addr),
    ));
    websocket.on_upgrade(move |socket| handle_socket(socket, state, remote_address))
}

/// Owns one upgraded WebSocket from first split through final room cleanup.
///
/// After the HTTP upgrade complete, this function records connection metrics, delegates
/// authenticated admission to [`super::handshake::establish_session`], runs the steady-state
/// user loop, and then closes the logical room user exactly once. Keeping that
/// sequencing here prevent handshake code and steady-state protocol code from racing to
/// clean up the same chanel user.
async fn handle_socket(socket: WebSocket, state: RuntimeState, remote_address: Arc<str>) {
    async move {
        let current_span = Span::current();
        current_span.record(
            telemetry::schema::field::REMOTE_ADDRESS,
            field::display(remote_address.as_ref()),
        );
        let (mut ws_writer, mut ws_reader) = socket.split();
        state.metrics.record_ws_connection_accepted();
        info!(
            event = telemetry::schema::event::WS_CONNECTION_ACCEPTED,
            remote_address = remote_address.as_ref(),
            "accepted websocket connection"
        );
        let handshake_span = telemetry::ws_handshake_span();
        handshake_span.record(
            telemetry::schema::field::REMOTE_ADDRESS,
            field::display(remote_address.as_ref()),
        );
        let Some(mut user) = super::handshake::establish_session(
            &state,
            &mut ws_writer,
            &mut ws_reader,
            Arc::clone(&remote_address),
        )
        .instrument(handshake_span)
        .await
        else {
            return;
        };
        current_span.record("room_id", field::display(user.room.uuid()));
        current_span.record("user_id", field::debug(&user.user_id));
        current_span.record("connection_id", field::debug(user.connection_id));
        state.metrics.record_ws_user_loop_started();
        let exit_reason = super::session_loop::run(
            &mut ws_writer,
            &mut ws_reader,
            &mut user.outbound_rx,
            &mut user.session_protocol,
            state.websocket_options.user.timeout_ms,
            state.websocket_options.user.ping_interval_ms,
            &state.metrics,
        )
        .await;
        state.metrics.record_ws_user_loop_exit(exit_reason);
        info!(
            event = telemetry::schema::event::WS_CONNECTION_CLOSED,
            connection_id = ?user.connection_id,
            remote_address = user.remote_address.as_ref(),
            ?exit_reason,
            "closing websocket user"
        );
        user.session_protocol.finish().await;
        let _ = state
            .room_manager
            .close_session(
                user.room.uuid(),
                &user.user_id,
                user.connection_id,
                &state.transport_adapter,
            )
            .await;
    }
    .instrument(telemetry::ws_upgrade_span())
    .await;
}

pub(crate) async fn close_writer(writer: &mut WsWriter, close_code: WebSocketCloseCode) {
    let _result = writer
        .send(Message::Close(Some(CloseFrame {
            code: u16::from(close_code),
            reason: "".into(),
        })))
        .await;
}
