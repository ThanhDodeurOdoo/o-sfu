//! WebSocket Connection Lifecycle
//!
//! This module manages the lifecycle of a client's WebSocket connection and its
//! relationship to the underlaying RTC sialing session:
//!
//! 1. **Creation**: A connection begins when the Axum router accepts an HTTP upgrade
//!    request in the `upgrade` handler. It is then split into a read and write stream
//!    as a raw, unauthenticated socket.
//!
//! 2. **Upgrade to RTC Session**: The raw socket is passed to `handshake::establish_session`,
//!    where it waits for an `auth` envelope from the client. After JWT validation,
//!    the connection is admitted into a `Channel`. At this point, the connection is upgraded
//!    into a full RTC session: a `SessionProtocol` is created to handle WebRTC state, and
//!    the `TransportAdapter` initializes the backend WebRTC transport resources.
//!
//! 3. **Steady State**: The connection enters the steady-state `session_loop::run`, continuously
//!    polling for incoming WebSocket frames to feed the `SessionProtocol` and outbound
//!    channel events to send back to the client.
//!
//! 4. **Removal**: When the session loop terminates (due to client disconnect, timeout, or
//!    protocol error), the connection is cleaned up. The `close_session` method is invoked
//!    on the `ChannelManager`, which removes the user from the channel and signasl
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
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use o_sfu_protocol::{shared::SessionId, signaling::WebSocketCloseCode};
use tokio::sync::mpsc;
use tracing::{Instrument, Span, field, info};

use crate::runtime::{
    ConnectionId, RuntimeState,
    channel::{Channel, SessionOutbound},
    request_origin::resolve_remote_address,
    telemetry,
};

use super::{WsWriter, session_protocol::SessionProtocol};

pub(super) type WsReader = SplitStream<WebSocket>;

pub(super) struct ConnectedSession {
    pub(super) channel: Arc<Channel>,
    pub(super) session_id: SessionId,
    pub(super) connection_id: ConnectionId,
    pub(super) remote_address: Arc<str>,
    pub(super) outbound_rx: mpsc::UnboundedReceiver<SessionOutbound>,
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
        &state.config,
        connect_info.map(|Extension(ConnectInfo(addr))| addr),
    ));
    websocket.on_upgrade(move |socket| handle_socket(socket, state, remote_address))
}

/// Owns one upgraded WebSocket from first split through final channel cleanup.
///
/// After the HTTP upgrade complete, this function records connection metrics, delegates
/// authenticated admission to [`super::handshake::establish_session`], runs the steady-state
/// session loop, and then closes the logical channel session exactly once. Keeping that
/// sequencing here prevent handshake code and steady-state protocol code from racing to
/// clean up the same chanel session.
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
        let Some(mut session) = super::handshake::establish_session(
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
        current_span.record("channel_uuid", field::display(session.channel.uuid()));
        current_span.record("session_id", field::debug(&session.session_id));
        current_span.record("connection_id", field::debug(session.connection_id));
        state.metrics.record_ws_session_loop_started();
        let exit_reason = super::session_loop::run(
            &mut ws_writer,
            &mut ws_reader,
            &mut session.outbound_rx,
            &mut session.session_protocol,
            state.config.session_timeout_ms,
            state.config.ping_interval_ms,
            &state.metrics,
        )
        .await;
        state.metrics.record_ws_session_loop_exit(exit_reason);
        info!(
            event = telemetry::schema::event::WS_CONNECTION_CLOSED,
            connection_id = ?session.connection_id,
            remote_address = session.remote_address.as_ref(),
            ?exit_reason,
            "closing websocket session"
        );
        let _ = state
            .channel_manager
            .close_session(
                session.channel.uuid(),
                &session.session_id,
                session.connection_id,
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
