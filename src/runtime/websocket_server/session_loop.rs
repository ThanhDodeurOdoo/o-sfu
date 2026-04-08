use axum::{Error as AxumError, extract::ws::Message};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, info};

use super::{WsReader, close_writer};
use crate::runtime::{
    channel::SessionOutbound,
    metrics::{RuntimeMetrics, WsSessionLoopExitReason},
    stub_bus::{
        StubBusOutcome, StubBusSession, WsWriter, send_server_message_batch,
        send_server_request_batch,
    },
};
use crate::signaling::current_protocol::{
    CurrentServerMessage, CurrentServerRequest, CurrentWebSocketCloseCode,
};

pub(super) async fn run(
    writer: &mut WsWriter,
    reader: &mut WsReader,
    outbound_rx: &mut mpsc::UnboundedReceiver<SessionOutbound>,
    stub_bus: &mut StubBusSession,
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
                if let Some(reason) = handle_ping_tick(
                    writer,
                    stub_bus,
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
                close_writer(writer, CurrentWebSocketCloseCode::Error).await;
                return WsSessionLoopExitReason::PingTimeout;
            }
            message = reader.next() => {
                if let Some(reason) = handle_incoming_socket_event(writer, stub_bus, message).await {
                    return reason;
                }
                if !stub_bus.awaiting_ping_response() {
                    ping_response_deadline = None;
                }
            }
            outbound = outbound_rx.recv() => {
                if let Some(reason) = handle_outbound_event(writer, outbound, metrics).await {
                    return reason;
                }
            }
        }
    }
}

async fn handle_ping_tick(
    writer: &mut WsWriter,
    stub_bus: &mut StubBusSession,
    ping_interval: Duration,
    ping_timeout: Duration,
    next_ping_at: &mut Instant,
    ping_response_deadline: &mut Option<Instant>,
) -> Option<WsSessionLoopExitReason> {
    if stub_bus.send_ping(writer).await.is_err() {
        info!("failed to send websocket bus ping request");
        return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
    }
    let now = Instant::now();
    *next_ping_at = now + ping_interval;
    *ping_response_deadline = Some(now + ping_timeout);
    None
}

async fn handle_incoming_socket_event(
    writer: &mut WsWriter,
    stub_bus: &mut StubBusSession,
    message: Option<Result<Message, AxumError>>,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Some(Ok(message)) => handle_incoming_frame(writer, stub_bus, message).await,
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
    stub_bus: &mut StubBusSession,
    message: Message,
) -> Option<WsSessionLoopExitReason> {
    debug!("received websocket frame");
    match stub_bus.handle_frame(writer, message).await {
        StubBusOutcome::Continue => None,
        StubBusOutcome::Break => Some(WsSessionLoopExitReason::BusBreak),
        StubBusOutcome::Close(code) => {
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
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    match outbound {
        Some(SessionOutbound::Message(message)) => {
            handle_outbound_message(writer, message, metrics).await
        }
        Some(SessionOutbound::Request(request)) => {
            handle_outbound_request(writer, *request, metrics).await
        }
        Some(SessionOutbound::Close(code)) => handle_outbound_close(writer, code).await,
        None => {
            info!("session outbound channel closed");
            Some(WsSessionLoopExitReason::OutboundChannelClosed)
        }
    }
}

async fn handle_outbound_message(
    writer: &mut WsWriter,
    message: CurrentServerMessage,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    debug!(server_message = ?message, "sending outbound server message");
    if send_server_message_batch(writer, &message).await.is_err() {
        metrics.record_ws_bus_send_failure();
        info!("failed to send outbound server message");
        return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
    }
    metrics.record_ws_bus_batch_sent(1);
    None
}

async fn handle_outbound_request(
    writer: &mut WsWriter,
    request: CurrentServerRequest,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    debug!(server_request = ?request, "sending outbound server request");
    if send_server_request_batch(writer, &request).await.is_err() {
        metrics.record_ws_bus_send_failure();
        info!("failed to send outbound server request");
        return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
    }
    metrics.record_ws_bus_batch_sent(1);
    None
}

async fn handle_outbound_close(
    writer: &mut WsWriter,
    code: CurrentWebSocketCloseCode,
) -> Option<WsSessionLoopExitReason> {
    info!(
        close_code = u16::from(code),
        "closing websocket from outbound signal"
    );
    close_writer(writer, code).await;
    Some(WsSessionLoopExitReason::OutboundCloseSignal)
}
