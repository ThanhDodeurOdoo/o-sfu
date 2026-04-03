use axum::{Error as AxumError, extract::ws::Message};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, info};

use super::{WsReader, close_writer};
use crate::runtime::{
    channel::SessionOutbound,
    stub_bus::{StubBusOutcome, StubBusSession, WsWriter, send_server_message_batch},
};
use crate::signaling::{
    current_protocol::CurrentServerMessage, current_protocol::CurrentWebSocketCloseCode,
};

pub(super) async fn run(
    writer: &mut WsWriter,
    reader: &mut WsReader,
    outbound_rx: &mut mpsc::UnboundedReceiver<SessionOutbound>,
    stub_bus: &mut StubBusSession,
) {
    loop {
        tokio::select! {
            message = reader.next() => {
                if handle_incoming_socket_event(writer, stub_bus, message).await.is_break() {
                    break;
                }
            }
            outbound = outbound_rx.recv() => {
                if handle_outbound_event(writer, outbound).await.is_break() {
                    break;
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum LoopControl {
    Continue,
    Break,
}

impl LoopControl {
    const fn is_break(self) -> bool {
        matches!(self, Self::Break)
    }
}

async fn handle_incoming_socket_event(
    writer: &mut WsWriter,
    stub_bus: &mut StubBusSession,
    message: Option<Result<Message, AxumError>>,
) -> LoopControl {
    match message {
        Some(Ok(message)) => handle_incoming_frame(writer, stub_bus, message).await,
        Some(Err(_error)) => {
            info!("websocket reader returned an error");
            LoopControl::Break
        }
        None => {
            info!("websocket peer closed the socket");
            LoopControl::Break
        }
    }
}

async fn handle_incoming_frame(
    writer: &mut WsWriter,
    stub_bus: &mut StubBusSession,
    message: Message,
) -> LoopControl {
    debug!("received websocket frame");
    match stub_bus.handle_frame(writer, message).await {
        StubBusOutcome::Continue => LoopControl::Continue,
        StubBusOutcome::Break => LoopControl::Break,
        StubBusOutcome::Close(code) => {
            info!(
                close_code = u16::from(code),
                "closing websocket from session loop"
            );
            close_writer(writer, code).await;
            LoopControl::Break
        }
    }
}

async fn handle_outbound_event(
    writer: &mut WsWriter,
    outbound: Option<SessionOutbound>,
) -> LoopControl {
    match outbound {
        Some(SessionOutbound::Message(message)) => handle_outbound_message(writer, message).await,
        Some(SessionOutbound::Close(code)) => handle_outbound_close(writer, code).await,
        None => {
            info!("session outbound channel closed");
            LoopControl::Break
        }
    }
}

async fn handle_outbound_message(
    writer: &mut WsWriter,
    message: CurrentServerMessage,
) -> LoopControl {
    debug!(server_message = ?message, "sending outbound server message");
    if send_server_message_batch(writer, &message).await.is_err() {
        info!("failed to send outbound server message");
        return LoopControl::Break;
    }
    LoopControl::Continue
}

async fn handle_outbound_close(
    writer: &mut WsWriter,
    code: CurrentWebSocketCloseCode,
) -> LoopControl {
    info!(
        close_code = u16::from(code),
        "closing websocket from outbound signal"
    );
    close_writer(writer, code).await;
    LoopControl::Break
}
