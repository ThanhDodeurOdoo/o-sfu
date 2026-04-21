use axum::{Error as AxumError, extract::ws::Message};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tracing::debug;

use super::{close_writer, controller::WsReader};
use crate::runtime::{
    channel::SessionOutbound,
    metrics::{RuntimeMetrics, WsSessionLoopExitReason},
};
use o_sfu_protocol::signaling::WebSocketCloseCode;

use super::{
    WsWriter,
    session_protocol::{SessionProtocol, SessionProtocolOutcome},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LivenessState {
    Idle,
    WaitingForPong { deadline: Instant },
}

impl LivenessState {
    const fn pong_deadline(self) -> Option<Instant> {
        match self {
            Self::Idle => None,
            Self::WaitingForPong { deadline } => Some(deadline),
        }
    }
}

/// Drives a live authenticated WebSocket session until it terminates.
///
/// This loop owns transport-shaped concerns only: ping timeouts, RTC transport health
/// checks, inbound frame dispatch, and outbound channel fanout. The detailed signaling
/// state machine lives behind [`SessionProtocol`]; this function only decides which event
/// source fired next and converts that outcome into a [`WsSessionLoopExitReason`].
pub(super) async fn run(
    writer: &mut WsWriter,
    reader: &mut WsReader,
    outbound_rx: &mut mpsc::UnboundedReceiver<SessionOutbound>,
    session_protocol: &mut SessionProtocol,
    session_timeout_ms: u64,
    ping_interval_ms: u64,
    metrics: &RuntimeMetrics,
) -> WsSessionLoopExitReason {
    let ping_interval = Duration::from_millis(ping_interval_ms);
    let ping_timeout = Duration::from_millis(session_timeout_ms);
    let mut next_ping_at = Instant::now() + ping_interval;
    let mut next_transport_state_check_at = next_ping_at;
    let mut liveness_state = LivenessState::Idle;
    loop {
        let transport_state_tick = sleep_until(next_transport_state_check_at);
        tokio::pin!(transport_state_tick);
        let ping_tick = sleep_until(next_ping_at);
        tokio::pin!(ping_tick);
        let pong_deadline = liveness_state.pong_deadline();
        tokio::select! {
            () = &mut transport_state_tick => {
                next_transport_state_check_at = Instant::now() + ping_interval;
                if let Some(reason) = handle_transport_state_tick(writer, session_protocol).await {
                    return reason;
                }
            }
            () = &mut ping_tick, if matches!(liveness_state, LivenessState::Idle) => {
                if let Some(reason) = handle_transport_state_tick(writer, session_protocol).await {
                    return reason;
                }
                if let Some(reason) = handle_ping_tick(
                    writer,
                    ping_interval,
                    ping_timeout,
                    &mut next_ping_at,
                    &mut liveness_state,
                ).await {
                    return reason;
                }
            }
            () = async {
                if let Some(deadline) = pong_deadline {
                    sleep_until(deadline).await;
                }
            }, if pong_deadline.is_some() => {
                debug!("timed out waiting for websocket pong");
                close_writer(writer, WebSocketCloseCode::Error).await;
                return WsSessionLoopExitReason::PingTimeout;
            }
            message = reader.next() => {
                if let Some(reason) = handle_incoming_socket_event(writer, session_protocol, message, &mut liveness_state).await {
                    return reason;
                }
            }
            outbound = outbound_rx.recv() => {
                if let Some(reason) = handle_outbound_event(writer, outbound, session_protocol, metrics).await {
                    return reason;
                }
            }
        }
    }
}

async fn handle_ping_tick(
    writer: &mut WsWriter,
    ping_interval: Duration,
    ping_timeout: Duration,
    next_ping_at: &mut Instant,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    if writer.send(Message::Ping(Vec::new().into())).await.is_err() {
        debug!("failed to send websocket ping frame");
        return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
    }
    let now = Instant::now();
    *next_ping_at = now + ping_interval;
    *liveness_state = LivenessState::WaitingForPong {
        deadline: now + ping_timeout,
    };
    None
}

async fn handle_transport_state_tick(
    writer: &mut WsWriter,
    session_protocol: &SessionProtocol,
) -> Option<WsSessionLoopExitReason> {
    let close_code = session_protocol.transport_close_code()?;
    debug!(
        close_code = u16::from(close_code),
        "closing websocket because the underlying RTC transport disconnected"
    );
    close_writer(writer, close_code).await;
    Some(WsSessionLoopExitReason::TransportDisconnected)
}

async fn handle_incoming_socket_event(
    writer: &mut WsWriter,
    session_protocol: &mut SessionProtocol,
    message: Option<Result<Message, AxumError>>,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Some(Ok(message)) => {
            handle_incoming_frame(writer, session_protocol, message, liveness_state).await
        }
        Some(Err(_error)) => {
            debug!("websocket reader returned an error");
            Some(WsSessionLoopExitReason::ReaderError)
        }
        None => {
            debug!("websocket peer closed the socket");
            Some(WsSessionLoopExitReason::PeerClosed)
        }
    }
}

async fn handle_incoming_frame(
    writer: &mut WsWriter,
    session_protocol: &mut SessionProtocol,
    message: Message,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Message::Ping(payload) => {
            if writer.send(Message::Pong(payload)).await.is_err() {
                debug!("failed to send websocket pong frame");
                return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
            }
            return None;
        }
        Message::Pong(_) => {
            *liveness_state = LivenessState::Idle;
            return None;
        }
        Message::Text(_) | Message::Binary(_) | Message::Close(_) => {}
    }
    match session_protocol.handle_frame(writer, message).await {
        SessionProtocolOutcome::Continue => None,
        SessionProtocolOutcome::Break => Some(WsSessionLoopExitReason::BusBreak),
        SessionProtocolOutcome::Close(code) => {
            debug!(
                close_code = u16::from(code),
                "closing websocket from session loop"
            );
            close_writer(writer, code).await;
            Some(WsSessionLoopExitReason::BusBreak)
        }
    }
}

async fn handle_outbound_event(
    writer: &mut WsWriter,
    outbound: Option<SessionOutbound>,
    session_protocol: &mut SessionProtocol,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    if let Some(outbound) = outbound {
        handle_outbound_payload(writer, outbound, session_protocol, metrics).await
    } else {
        debug!("session outbound channel closed");
        Some(WsSessionLoopExitReason::OutboundChannelClosed)
    }
}

#[allow(
    clippy::cognitive_complexity,
    reason = "outbound handling keeps protocol send, close-signal handling, and metrics in one explicit session-loop branch"
)]
async fn handle_outbound_payload(
    writer: &mut WsWriter,
    outbound: SessionOutbound,
    session_protocol: &mut SessionProtocol,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    match session_protocol.send_outbound(writer, outbound).await {
        Ok(batch_len) => {
            metrics.record_ws_bus_batch_sent(batch_len);
            None
        }
        Err(WebSocketCloseCode::Kicked) => {
            debug!(close_code = 4003, "closing websocket from outbound signal");
            close_writer(writer, WebSocketCloseCode::Kicked).await;
            Some(WsSessionLoopExitReason::OutboundCloseSignal)
        }
        Err(code) => {
            metrics.record_ws_bus_send_failure();
            debug!(
                close_code = u16::from(code),
                "failed to send outbound session event"
            );
            if matches!(
                code,
                WebSocketCloseCode::Clean
                    | WebSocketCloseCode::Leaving
                    | WebSocketCloseCode::Kicked
                    | WebSocketCloseCode::ChannelFull
                    | WebSocketCloseCode::AuthFailed
                    | WebSocketCloseCode::AuthTimeout
            ) {
                close_writer(writer, code).await;
            }
            Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
        }
    }
}
