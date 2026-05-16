use std::time::Duration;

use axum::{Error as AxumError, extract::ws::Message};
use futures_util::{SinkExt, StreamExt};
use o_sfu_protocol::{
    shared::UserId,
    signaling::{
        ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
        RecordingActionResult, RecordingOptions, RequestId, ServerResponse, WebSocketCloseCode,
    },
};
use tokio::time::{Instant, sleep_until, timeout};
use tracing::{debug, info, warn};

use super::{WsWriter, close_writer, controller::WsReader, io::send_user_output};
use crate::{
    application::user_session::{User, UserError, UserOutput, UserSignal},
    core::server::room::{
        Room, RoomEventRequest, RoomManager, UserCloseReason, UserOutbound, UserOutboundEvent,
        UserOutboundOverflow, UserOutboundReceiver,
    },
    runtime::{
        ConnectionId,
        media_transport::MediaTransport,
        metrics::{RuntimeMetrics, WsSessionLoopExitReason},
        telemetry::schema::event as telemetry_event,
        websocket_server::{
            ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES, decode_client_batch,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LivenessState {
    Idle,
    WaitingForPong { deadline: Instant },
}

const OUTBOUND_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

impl LivenessState {
    const fn pong_deadline(self) -> Option<Instant> {
        match self {
            Self::Idle => None,
            Self::WaitingForPong { deadline } => Some(deadline),
        }
    }
}

pub(super) struct UserLoop<'a> {
    pub(super) writer: &'a mut WsWriter,
    pub(super) reader: &'a mut WsReader,
    pub(super) room_manager: &'a RoomManager,
    pub(super) room: &'a Room,
    pub(super) user_id: &'a UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) outbound_rx: &'a mut UserOutboundReceiver,
    pub(super) user: &'a mut User,
    pub(super) media_transport: &'a MediaTransport,
    pub(super) user_timeout_ms: u64,
    pub(super) ping_interval_ms: u64,
    pub(super) metrics: &'a RuntimeMetrics,
}

pub(super) async fn run(mut session: UserLoop<'_>) -> WsSessionLoopExitReason {
    let exit_reason = run_until_exit(&mut session).await;
    teardown(&mut session).await;
    exit_reason
}

async fn run_until_exit(session: &mut UserLoop<'_>) -> WsSessionLoopExitReason {
    let ping_interval = Duration::from_millis(session.ping_interval_ms);
    let ping_timeout = Duration::from_millis(session.user_timeout_ms);
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
                if let Some(reason) = handle_transport_state_tick(session).await {
                    return reason;
                }
            }
            () = &mut ping_tick, if matches!(liveness_state, LivenessState::Idle) => {
                if let Some(reason) = handle_transport_state_tick(session).await {
                    return reason;
                }
                if let Some(reason) = handle_ping_tick(
                    session.writer,
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
                close_writer_bounded(session.writer, WebSocketCloseCode::Error).await;
                return WsSessionLoopExitReason::PingTimeout;
            }
            message = session.reader.next() => {
                if let Some(reason) = handle_incoming_socket_event(
                    session,
                    message,
                    &mut liveness_state,
                ).await {
                    return reason;
                }
            }
            outbound = session.outbound_rx.recv_event() => {
                if let Some(reason) = handle_outbound_event(session, outbound).await {
                    return reason;
                }
            }
        }
    }
}

async fn teardown(session: &mut UserLoop<'_>) {
    session.user.close().await;
    let _removed = session
        .room_manager
        .close_session(
            session.room.uuid(),
            session.user_id,
            session.connection_id,
            session.media_transport,
        )
        .await;
}

async fn handle_ping_tick(
    writer: &mut WsWriter,
    ping_interval: Duration,
    ping_timeout: Duration,
    next_ping_at: &mut Instant,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    if send_ping_bounded(writer).await.is_err() {
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
    session: &mut UserLoop<'_>,
) -> Option<WsSessionLoopExitReason> {
    session.user.disconnect_reason()?;
    debug!("closing websocket because the underlying RTC transport disconnected");
    close_writer_bounded(session.writer, WebSocketCloseCode::Error).await;
    Some(WsSessionLoopExitReason::TransportDisconnected)
}

async fn handle_incoming_socket_event(
    session: &mut UserLoop<'_>,
    message: Option<Result<Message, AxumError>>,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Some(Ok(message)) => handle_incoming_frame(session, message, liveness_state).await,
        Some(Err(_error)) => {
            debug!("websocket reader returned an error");
            Some(WsSessionLoopExitReason::ReaderError)
        }
        None => {
            debug!("websocket user closed the socket");
            Some(WsSessionLoopExitReason::UserClosed)
        }
    }
}

async fn handle_incoming_frame(
    session: &mut UserLoop<'_>,
    message: Message,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Message::Ping(payload) => {
            if session.writer.send(Message::Pong(payload)).await.is_err() {
                debug!("failed to send websocket pong frame");
                return Some(WsSessionLoopExitReason::OutboundMessageSendFailure);
            }
            None
        }
        Message::Pong(_) => {
            *liveness_state = LivenessState::Idle;
            None
        }
        Message::Close(frame) => {
            info!(?frame, "websocket user sent close frame");
            Some(WsSessionLoopExitReason::BusBreak)
        }
        Message::Text(payload) => handle_text_payload(session, &payload).await,
        Message::Binary(payload) => handle_binary_payload(session, &payload).await,
    }
}

async fn handle_binary_payload(
    session: &mut UserLoop<'_>,
    payload: &[u8],
) -> Option<WsSessionLoopExitReason> {
    if payload.len() > MAX_CLIENT_FRAME_BYTES {
        session.metrics.record_ws_bus_invalid_input_failure();
        warn!(
            payload_len = payload.len(),
            max_len = MAX_CLIENT_FRAME_BYTES,
            "received oversized websocket binary frame"
        );
        close_writer_bounded(session.writer, WebSocketCloseCode::ProtocolError).await;
        return Some(WsSessionLoopExitReason::BusBreak);
    }
    match String::from_utf8(payload.to_vec()) {
        Ok(payload) => handle_text_payload(session, &payload).await,
        Err(_error) => {
            session.metrics.record_ws_bus_invalid_input_failure();
            warn!("received websocket binary frame with invalid UTF-8");
            close_writer_bounded(session.writer, WebSocketCloseCode::ProtocolError).await;
            Some(WsSessionLoopExitReason::BusBreak)
        }
    }
}

async fn handle_text_payload(
    session: &mut UserLoop<'_>,
    payload: &str,
) -> Option<WsSessionLoopExitReason> {
    let batch = match decode_client_batch(payload) {
        Ok(batch) => batch,
        Err(error) => {
            match error.kind() {
                ClientBatchDecodeFailureKind::InvalidInput => {
                    session.metrics.record_ws_bus_invalid_input_failure();
                    warn!(
                        "failed to decode client websocket batch because the payload was invalid"
                    );
                }
                ClientBatchDecodeFailureKind::UnsupportedFeature => {
                    session.metrics.record_ws_bus_unsupported_feature_failure();
                    warn!(
                        "failed to decode client websocket batch because it used an unsupported feature"
                    );
                }
            }
            close_writer_bounded(session.writer, WebSocketCloseCode::ProtocolError).await;
            return Some(WsSessionLoopExitReason::BusBreak);
        }
    };
    session.metrics.record_ws_bus_batch_received(batch.len());
    let mut output = UserOutput::new();
    for envelope in batch {
        record_client_envelope_metrics(session.metrics, &envelope);
        match dispatch_client_envelope(session, envelope).await {
            Ok(user_output) => output.extend(user_output),
            Err(error) => {
                let close_code = map_user_error(error);
                close_writer_bounded(session.writer, close_code).await;
                return Some(WsSessionLoopExitReason::BusBreak);
            }
        }
    }
    match send_user_output_bounded(session.writer, output).await {
        Ok(_sent) => None,
        Err(code) => {
            close_writer_bounded(session.writer, code).await;
            Some(WsSessionLoopExitReason::BusBreak)
        }
    }
}

fn record_client_envelope_metrics(metrics: &RuntimeMetrics, envelope: &ClientEnvelope) {
    match envelope {
        ClientEnvelope::Request { .. } => metrics.record_ws_bus_client_request(),
        ClientEnvelope::Message(_) => metrics.record_ws_bus_client_message(),
        ClientEnvelope::Response { .. } => {}
    }
}

async fn dispatch_client_envelope(
    session: &mut UserLoop<'_>,
    envelope: ClientEnvelope,
) -> Result<UserOutput, UserError> {
    match envelope {
        ClientEnvelope::Message(ClientMessage::Info(info)) => session.user.update_info(info).await,
        ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload { message })) => {
            session.user.broadcast(message).await
        }
        ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
            let target_user_id = payload.user_id.normalized_for_runtime();
            session
                .user
                .subscribe_to(&target_user_id, &payload.states)
                .await
        }
        ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
            session.user.publish(payload.stream_type).await
        }
        ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
            session.user.unpublish(payload.stream_type).await
        }
        ClientEnvelope::Response {
            response_to,
            response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
        } => session.user.complete_negotiation(response_to, answer).await,
        ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StartRecording(payload),
        } => Ok(start_recording(session, request_id, payload).await),
        ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StopRecording,
        } => Ok(stop_recording(session, request_id).await),
        ClientEnvelope::Message(ClientMessage::Auth(_)) => Err(UserError::ProtocolViolation),
    }
}

async fn start_recording(
    session: &UserLoop<'_>,
    request_id: RequestId,
    payload: RecordingOptions,
) -> UserOutput {
    let ok = session
        .room
        .start_recording_runtime(session.user_id, session.connection_id, payload)
        .await;
    info!(
        event = telemetry_event::RECORDING_STARTED,
        operation = "recording_start",
        outcome = if ok { "accepted" } else { "rejected" },
        "processed recording start request"
    );
    UserOutput::new().with_signal(UserSignal::response(
        request_id,
        ServerResponse::StartRecording(RecordingActionResult { ok }),
    ))
}

async fn stop_recording(session: &UserLoop<'_>, request_id: RequestId) -> UserOutput {
    let ok = session
        .room
        .stop_recording_runtime(session.user_id, session.connection_id)
        .await;
    info!(
        event = telemetry_event::RECORDING_STOPPED,
        operation = "recording_stop",
        outcome = if ok { "accepted" } else { "rejected" },
        "processed recording stop request"
    );
    UserOutput::new().with_signal(UserSignal::response(
        request_id,
        ServerResponse::StopRecording(RecordingActionResult { ok }),
    ))
}

async fn handle_outbound_event(
    session: &mut UserLoop<'_>,
    outbound: UserOutboundEvent,
) -> Option<WsSessionLoopExitReason> {
    match outbound {
        UserOutboundEvent::Message(outbound) => handle_outbound_payload(session, outbound).await,
        UserOutboundEvent::Overflow(overflow) => {
            handle_outbound_overflow(session.writer, overflow).await;
            Some(WsSessionLoopExitReason::OutboundQueueOverflow)
        }
        UserOutboundEvent::Closed => {
            debug!("user outbound room closed");
            Some(WsSessionLoopExitReason::OutboundChannelClosed)
        }
    }
}

async fn handle_outbound_overflow(writer: &mut WsWriter, overflow: UserOutboundOverflow) {
    warn!(
        capacity = overflow.capacity(),
        byte_capacity = overflow.byte_capacity(),
        queued_bytes = overflow.queued_bytes(),
        message_bytes = overflow.message_bytes(),
        overflow_kind = ?overflow.kind(),
        "closing websocket because the outbound queue overflowed"
    );
    close_writer_bounded(writer, WebSocketCloseCode::Kicked).await;
}

#[allow(
    clippy::cognitive_complexity,
    reason = "outbound handling keeps protocol send, close-signal handling, and metrics in one explicit user-loop branch"
)]
async fn handle_outbound_payload(
    session: &mut UserLoop<'_>,
    outbound: UserOutbound,
) -> Option<WsSessionLoopExitReason> {
    match dispatch_room_outbound(session.user, outbound).await {
        Ok(output) => match send_user_output_bounded(session.writer, output).await {
            Ok(batch_len) => {
                session.metrics.record_ws_bus_batch_sent(batch_len);
                None
            }
            Err(WebSocketCloseCode::Kicked) => {
                debug!(close_code = 4108, "closing websocket from outbound signal");
                close_writer_bounded(session.writer, WebSocketCloseCode::Kicked).await;
                Some(WsSessionLoopExitReason::OutboundCloseSignal)
            }
            Err(code) => {
                session.metrics.record_ws_bus_send_failure();
                debug!(
                    close_code = u16::from(code),
                    "failed to send outbound user event"
                );
                close_writer_if_terminal(session.writer, code).await;
                Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
            }
        },
        Err(code) => {
            if code == WebSocketCloseCode::Kicked {
                debug!(close_code = 4108, "closing websocket from outbound signal");
                close_writer_bounded(session.writer, WebSocketCloseCode::Kicked).await;
                Some(WsSessionLoopExitReason::OutboundCloseSignal)
            } else {
                session.metrics.record_ws_bus_send_failure();
                close_writer_if_terminal(session.writer, code).await;
                Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
            }
        }
    }
}

async fn send_ping_bounded(writer: &mut WsWriter) -> Result<(), WebSocketCloseCode> {
    match timeout(
        OUTBOUND_WRITE_TIMEOUT,
        writer.send(Message::Ping(Vec::new().into())),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(WebSocketCloseCode::Error),
    }
}

async fn send_user_output_bounded(
    writer: &mut WsWriter,
    output: UserOutput,
) -> Result<usize, WebSocketCloseCode> {
    match timeout(OUTBOUND_WRITE_TIMEOUT, send_user_output(writer, output)).await {
        Ok(result) => result,
        Err(_elapsed) => Err(WebSocketCloseCode::Error),
    }
}

async fn close_writer_bounded(writer: &mut WsWriter, code: WebSocketCloseCode) {
    let _closed = timeout(OUTBOUND_WRITE_TIMEOUT, close_writer(writer, code)).await;
}

async fn dispatch_room_outbound(
    user: &mut User,
    outbound: UserOutbound,
) -> Result<UserOutput, WebSocketCloseCode> {
    match outbound {
        UserOutbound::Close(reason) => Err(map_room_close_reason(reason)),
        UserOutbound::Message(message) => user
            .apply_room_message(message)
            .await
            .map_err(map_user_error),
        UserOutbound::Request(request) => match *request {
            RoomEventRequest::BootstrapRemoteTrack(payload) => {
                user.add_remote_track(payload).await.map_err(map_user_error)
            }
        },
        UserOutbound::TrackBindingUpdate(update) => user
            .update_remote_track(update)
            .await
            .map_err(map_user_error),
    }
}

async fn close_writer_if_terminal(writer: &mut WsWriter, code: WebSocketCloseCode) {
    if matches!(
        code,
        WebSocketCloseCode::Clean
            | WebSocketCloseCode::Leaving
            | WebSocketCloseCode::Kicked
            | WebSocketCloseCode::RoomFull
            | WebSocketCloseCode::AuthFailed
            | WebSocketCloseCode::AuthTimeout
    ) {
        close_writer_bounded(writer, code).await;
    }
}

fn map_room_close_reason(reason: UserCloseReason) -> WebSocketCloseCode {
    match reason {
        UserCloseReason::Replaced | UserCloseReason::RemovedByRuntime => WebSocketCloseCode::Kicked,
    }
}

fn map_user_error(error: UserError) -> WebSocketCloseCode {
    match error {
        UserError::ProtocolViolation => WebSocketCloseCode::ProtocolError,
        UserError::Kicked => WebSocketCloseCode::Kicked,
        UserError::InternalError => WebSocketCloseCode::Error,
    }
}
