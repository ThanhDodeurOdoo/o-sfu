use std::time::Duration;

use axum::{Error as AxumError, extract::ws::Message};
use futures_util::{SinkExt, StreamExt};
use o_sfu_protocol::wire::{
    ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
    RecordingActionResult, RecordingOptions, RequestId, ServerResponse, UserId, WebSocketCloseCode,
};
use tokio::time::{Instant, sleep_until};
use tracing::{debug, info, warn};

use super::{
    WsWriter,
    controller::WsReader,
    io::{close_writer_bounded, send_message_bounded, send_user_output_bounded},
};
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

impl LivenessState {
    const fn pong_deadline(self) -> Option<Instant> {
        match self {
            Self::Idle => None,
            Self::WaitingForPong { deadline } => Some(deadline),
        }
    }
}

pub(super) struct UserSocket<'a> {
    pub(super) writer: &'a mut WsWriter,
    pub(super) reader: &'a mut WsReader,
    pub(super) outbound_rx: &'a mut UserOutboundReceiver,
}

pub(super) struct VerifiedUserSession<'a> {
    pub(super) room_manager: &'a RoomManager,
    pub(super) room: &'a Room,
    pub(super) user_id: &'a UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) user: &'a mut User,
    pub(super) media_transport: &'a MediaTransport,
}

impl VerifiedUserSession<'_> {
    async fn close(&mut self) {
        self.user.close().await;
        let _removed = self
            .room_manager
            .close_session(
                self.room.uuid(),
                self.user_id,
                self.connection_id,
                self.media_transport,
            )
            .await;
    }

    fn transport_disconnected(&self) -> Option<()> {
        self.user.disconnect_reason().map(|_reason| ())
    }

    async fn start_recording(&self, payload: RecordingOptions) -> bool {
        self.room
            .start_recording_runtime(self.user_id, self.connection_id, payload)
            .await
    }

    async fn stop_recording(&self) -> bool {
        self.room
            .stop_recording_runtime(self.user_id, self.connection_id)
            .await
    }
}

pub(super) struct UserLoopConfig {
    pub(super) user_timeout_ms: u64,
    pub(super) ping_interval_ms: u64,
}

pub(super) struct UserLoop<'a> {
    pub(super) socket: UserSocket<'a>,
    pub(super) session: VerifiedUserSession<'a>,
    pub(super) config: UserLoopConfig,
    pub(super) metrics: &'a RuntimeMetrics,
}

pub(super) async fn run(mut user_loop: UserLoop<'_>) -> WsSessionLoopExitReason {
    let exit_reason = run_until_exit(&mut user_loop).await;
    user_loop.session.close().await;
    exit_reason
}

async fn run_until_exit(user_loop: &mut UserLoop<'_>) -> WsSessionLoopExitReason {
    let ping_interval = Duration::from_millis(user_loop.config.ping_interval_ms);
    let ping_timeout = Duration::from_millis(user_loop.config.user_timeout_ms);
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
                if let Some(reason) = handle_transport_state_tick(user_loop).await {
                    return reason;
                }
            }
            () = &mut ping_tick, if matches!(liveness_state, LivenessState::Idle) => {
                if let Some(reason) = handle_transport_state_tick(user_loop).await {
                    return reason;
                }
                if let Some(reason) = handle_ping_tick(
                    user_loop.socket.writer,
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
                close_writer_bounded(user_loop.socket.writer, WebSocketCloseCode::Error).await;
                return WsSessionLoopExitReason::PingTimeout;
            }
            message = user_loop.socket.reader.next() => {
                if let Some(reason) = handle_incoming_socket_event(
                    user_loop,
                    message,
                    &mut liveness_state,
                ).await {
                    return reason;
                }
            }
            outbound = user_loop.socket.outbound_rx.recv_event() => {
                if let Some(reason) = handle_outbound_event(user_loop, outbound).await {
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
    user_loop: &mut UserLoop<'_>,
) -> Option<WsSessionLoopExitReason> {
    user_loop.session.transport_disconnected()?;
    debug!("closing websocket because the underlying RTC transport disconnected");
    close_writer_bounded(user_loop.socket.writer, WebSocketCloseCode::Error).await;
    Some(WsSessionLoopExitReason::TransportDisconnected)
}

async fn handle_incoming_socket_event(
    user_loop: &mut UserLoop<'_>,
    message: Option<Result<Message, AxumError>>,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Some(Ok(message)) => handle_incoming_frame(user_loop, message, liveness_state).await,
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
    user_loop: &mut UserLoop<'_>,
    message: Message,
    liveness_state: &mut LivenessState,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Message::Ping(payload) => {
            if user_loop
                .socket
                .writer
                .send(Message::Pong(payload))
                .await
                .is_err()
            {
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
        Message::Text(payload) => handle_text_payload(user_loop, &payload).await,
        Message::Binary(payload) => handle_binary_payload(user_loop, &payload).await,
    }
}

async fn handle_binary_payload(
    user_loop: &mut UserLoop<'_>,
    payload: &[u8],
) -> Option<WsSessionLoopExitReason> {
    if payload.len() > MAX_CLIENT_FRAME_BYTES {
        user_loop.metrics.record_ws_bus_invalid_input_failure();
        warn!(
            payload_len = payload.len(),
            max_len = MAX_CLIENT_FRAME_BYTES,
            "received oversized websocket binary frame"
        );
        close_writer_bounded(user_loop.socket.writer, WebSocketCloseCode::ProtocolError).await;
        return Some(WsSessionLoopExitReason::BusBreak);
    }
    match String::from_utf8(payload.to_vec()) {
        Ok(payload) => handle_text_payload(user_loop, &payload).await,
        Err(_error) => {
            user_loop.metrics.record_ws_bus_invalid_input_failure();
            warn!("received websocket binary frame with invalid UTF-8");
            close_writer_bounded(user_loop.socket.writer, WebSocketCloseCode::ProtocolError).await;
            Some(WsSessionLoopExitReason::BusBreak)
        }
    }
}

async fn handle_text_payload(
    user_loop: &mut UserLoop<'_>,
    payload: &str,
) -> Option<WsSessionLoopExitReason> {
    let batch = match decode_client_batch(payload) {
        Ok(batch) => batch,
        Err(error) => {
            match error.kind() {
                ClientBatchDecodeFailureKind::InvalidInput => {
                    user_loop.metrics.record_ws_bus_invalid_input_failure();
                    warn!(
                        "failed to decode client websocket batch because the payload was invalid"
                    );
                }
                ClientBatchDecodeFailureKind::UnsupportedFeature => {
                    user_loop
                        .metrics
                        .record_ws_bus_unsupported_feature_failure();
                    warn!(
                        "failed to decode client websocket batch because it used an unsupported feature"
                    );
                }
            }
            close_writer_bounded(user_loop.socket.writer, WebSocketCloseCode::ProtocolError).await;
            return Some(WsSessionLoopExitReason::BusBreak);
        }
    };
    user_loop.metrics.record_ws_bus_batch_received(batch.len());
    let mut output = UserOutput::new();
    for envelope in batch {
        record_client_envelope_metrics(user_loop.metrics, &envelope);
        match dispatch_client_envelope(user_loop, envelope).await {
            Ok(user_output) => output.extend(user_output),
            Err(error) => {
                let close_code = map_user_error(error);
                close_writer_bounded(user_loop.socket.writer, close_code).await;
                return Some(WsSessionLoopExitReason::BusBreak);
            }
        }
    }
    match send_user_output_bounded(user_loop.socket.writer, output).await {
        Ok(_sent) => None,
        Err(code) => {
            close_writer_bounded(user_loop.socket.writer, code).await;
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
    user_loop: &mut UserLoop<'_>,
    envelope: ClientEnvelope,
) -> Result<UserOutput, UserError> {
    match envelope {
        ClientEnvelope::Message(ClientMessage::Info(info)) => {
            user_loop.session.user.update_info(info).await
        }
        ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload { message })) => {
            user_loop.session.user.broadcast(message).await
        }
        ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
            let target_user_id = payload.user_id.normalized_for_runtime();
            user_loop
                .session
                .user
                .subscribe_to(&target_user_id, &payload.states)
                .await
        }
        ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
            user_loop.session.user.publish(payload.stream_type).await
        }
        ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
            user_loop.session.user.unpublish(payload.stream_type).await
        }
        ClientEnvelope::Response {
            response_to,
            response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
        } => {
            user_loop
                .session
                .user
                .complete_negotiation(response_to, answer)
                .await
        }
        ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StartRecording(payload),
        } => Ok(start_recording(&user_loop.session, request_id, payload).await),
        ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StopRecording,
        } => Ok(stop_recording(&user_loop.session, request_id).await),
        ClientEnvelope::Message(ClientMessage::Auth(_)) => Err(UserError::ProtocolViolation),
    }
}

async fn start_recording(
    session: &VerifiedUserSession<'_>,
    request_id: RequestId,
    payload: RecordingOptions,
) -> UserOutput {
    let ok = session.start_recording(payload).await;
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

async fn stop_recording(session: &VerifiedUserSession<'_>, request_id: RequestId) -> UserOutput {
    let ok = session.stop_recording().await;
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
    user_loop: &mut UserLoop<'_>,
    outbound: UserOutboundEvent,
) -> Option<WsSessionLoopExitReason> {
    match outbound {
        UserOutboundEvent::Message(outbound) => handle_outbound_payload(user_loop, outbound).await,
        UserOutboundEvent::Overflow(overflow) => {
            handle_outbound_overflow(user_loop.socket.writer, overflow).await;
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

async fn handle_outbound_payload(
    user_loop: &mut UserLoop<'_>,
    outbound: UserOutbound,
) -> Option<WsSessionLoopExitReason> {
    match dispatch_room_outbound(user_loop.session.user, outbound).await {
        Ok(output) => match send_user_output_bounded(user_loop.socket.writer, output).await {
            Ok(batch_len) => {
                user_loop.metrics.record_ws_bus_batch_sent(batch_len);
                None
            }
            Err(code) => handle_outbound_close_code(user_loop, code, true).await,
        },
        Err(code) => handle_outbound_close_code(user_loop, code, false).await,
    }
}

async fn handle_outbound_close_code(
    user_loop: &mut UserLoop<'_>,
    code: WebSocketCloseCode,
    log_send_failure: bool,
) -> Option<WsSessionLoopExitReason> {
    if code == WebSocketCloseCode::Kicked {
        debug!(close_code = 4108, "closing websocket from outbound signal");
        close_writer_bounded(user_loop.socket.writer, WebSocketCloseCode::Kicked).await;
        return Some(WsSessionLoopExitReason::OutboundCloseSignal);
    }
    user_loop.metrics.record_ws_bus_send_failure();
    if log_send_failure {
        debug!(
            close_code = u16::from(code),
            "failed to send outbound user event"
        );
    }
    close_writer_if_terminal(user_loop.socket.writer, code).await;
    Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
}

async fn send_ping_bounded(writer: &mut WsWriter) -> Result<(), WebSocketCloseCode> {
    send_message_bounded(writer, Message::Ping(Vec::new().into())).await
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
