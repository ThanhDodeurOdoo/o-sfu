use std::sync::Arc;

use axum::{
    extract::{
        State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{Instrument, field, info, info_span};

use super::{
    RuntimeState,
    channel::{Channel, SessionOutbound},
    stub_bus::{StubBusSession, WsWriter},
};
use crate::signaling::{current_protocol::CurrentWebSocketCloseCode, shared::SessionId};

mod handshake;
mod session_loop;
#[cfg(test)]
mod tests;

type WsReader = SplitStream<WebSocket>;

struct ConnectedSession {
    channel: Arc<Channel>,
    session_id: SessionId,
    connection_id: u64,
    outbound_rx: mpsc::UnboundedReceiver<SessionOutbound>,
    stub_bus: StubBusSession,
}

pub(super) async fn upgrade(
    State(state): State<RuntimeState>,
    websocket: WebSocketUpgrade,
) -> Response {
    websocket.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: RuntimeState) {
    async move {
        let (mut ws_writer, mut ws_reader) = socket.split();
        state.metrics.record_ws_connection_accepted();
        info!("accepted websocket connection");
        let Some(mut session) =
            handshake::establish_session(&state, &mut ws_writer, &mut ws_reader).await
        else {
            return;
        };
        state.metrics.record_ws_session_loop_started();
        info!("entering websocket session loop");
        let exit_reason = session_loop::run(
            &mut ws_writer,
            &mut ws_reader,
            &mut session.outbound_rx,
            &mut session.stub_bus,
            &state.metrics,
        )
        .await;
        state.metrics.record_ws_session_loop_exit(exit_reason);
        info!("closing websocket session");
        state
            .channels
            .leave_session(
                session.channel.uuid(),
                &session.session_id,
                session.connection_id,
            )
            .await;
        if state
            .transport_adapter
            .close_session(&session.session_id)
            .await
            .is_err()
        {
            info!("failed to cleanup transport-adapter session state");
        }
    }
    .instrument(info_span!(
        "ws.connection",
        channel_uuid = field::Empty,
        session_id = field::Empty
    ))
    .await;
}

async fn close_writer(writer: &mut WsWriter, close_code: CurrentWebSocketCloseCode) {
    let _result = writer
        .send(Message::Close(Some(CloseFrame {
            code: u16::from(close_code),
            reason: "".into(),
        })))
        .await;
}
