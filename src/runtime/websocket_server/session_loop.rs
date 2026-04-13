use axum::{Error as AxumError, extract::ws::Message};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, info};

use super::{close_writer, controller::WsReader};
use crate::runtime::{
    channel::SessionOutbound,
    metrics::{RuntimeMetrics, WsSessionLoopExitReason},
    stub_bus::WsWriter,
};
use crate::signaling::protocol::WebSocketCloseCode;

use super::session_protocol::{SessionProtocol, SessionProtocolOutcome};

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
    let mut ping_response_deadline: Option<Instant> = None;
    loop {
        let ping_tick = sleep_until(next_ping_at);
        tokio::pin!(ping_tick);
        tokio::select! {
            () = &mut ping_tick, if ping_response_deadline.is_none() => {
                if let Some(reason) = handle_transport_state_tick(writer, session_protocol).await {
                    return reason;
                }
                if let Some(reason) = handle_ping_tick(
                    writer,
                    session_protocol,
                    ping_interval,
                    ping_timeout,
                    &mut next_ping_at,
                    &mut ping_response_deadline,
                ).await {
                    return reason;
                }
            }
            () = async {
                if let Some(deadline) = ping_response_deadline {
                    sleep_until(deadline).await;
                }
            }, if ping_response_deadline.is_some() => {
                info!("timed out waiting for websocket bus ping response");
                close_writer(writer, WebSocketCloseCode::Error).await;
                return WsSessionLoopExitReason::PingTimeout;
            }
            message = reader.next() => {
                if let Some(reason) = handle_incoming_socket_event(writer, session_protocol, message).await {
                    return reason;
                }
                if !session_protocol.awaiting_ping_response() {
                    ping_response_deadline = None;
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
    session_protocol: &mut SessionProtocol,
    ping_interval: Duration,
    ping_timeout: Duration,
    next_ping_at: &mut Instant,
    ping_response_deadline: &mut Option<Instant>,
) -> Option<WsSessionLoopExitReason> {
    if session_protocol.send_ping(writer).await.is_err() {
        info!("failed to send websocket bus ping request");
        return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
    }
    let now = Instant::now();
    *next_ping_at = now + ping_interval;
    *ping_response_deadline = Some(now + ping_timeout);
    None
}

async fn handle_transport_state_tick(
    writer: &mut WsWriter,
    session_protocol: &SessionProtocol,
) -> Option<WsSessionLoopExitReason> {
    let close_code = session_protocol.transport_close_code()?;
    info!(
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
) -> Option<WsSessionLoopExitReason> {
    match message {
        Some(Ok(message)) => handle_incoming_frame(writer, session_protocol, message).await,
        Some(Err(_error)) => {
            info!("websocket reader returned an error");
            Some(WsSessionLoopExitReason::ReaderError)
        }
        None => {
            info!("websocket peer closed the socket");
            Some(WsSessionLoopExitReason::PeerClosed)
        }
    }
}

async fn handle_incoming_frame(
    writer: &mut WsWriter,
    session_protocol: &mut SessionProtocol,
    message: Message,
) -> Option<WsSessionLoopExitReason> {
    debug!("received websocket frame");
    match session_protocol.handle_frame(writer, message).await {
        SessionProtocolOutcome::Continue => None,
        SessionProtocolOutcome::Break => Some(WsSessionLoopExitReason::BusBreak),
        SessionProtocolOutcome::Close(code) => {
            info!(
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
        info!("session outbound channel closed");
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
    debug!(?outbound, "sending outbound session event");
    match session_protocol.send_outbound(writer, outbound).await {
        Ok(batch_len) => {
            metrics.record_ws_bus_batch_sent(batch_len);
            None
        }
        Err(WebSocketCloseCode::Kicked) => {
            info!(close_code = 4003, "closing websocket from outbound signal");
            close_writer(writer, WebSocketCloseCode::Kicked).await;
            Some(WsSessionLoopExitReason::OutboundCloseSignal)
        }
        Err(code) => {
            metrics.record_ws_bus_send_failure();
            info!(
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
