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
use tokio::{
    sync::mpsc,
    time::{Instant, sleep_until},
};
use tracing::{debug, info, warn};

use super::{WsWriter, close_writer, controller::WsReader, io::send_user_output};
use crate::{
    application::user_session::{User, UserError, UserOutput, UserSignal},
    core::runtime::room::{
        Room, RoomEventMessage, RoomEventRequest, UserCloseReason, UserOutbound,
    },
    runtime::{
        ConnectionId,
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

pub(super) struct UserLoop<'a> {
    pub(super) writer: &'a mut WsWriter,
    pub(super) reader: &'a mut WsReader,
    pub(super) room: &'a Room,
    pub(super) user_id: &'a UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) outbound_rx: &'a mut mpsc::UnboundedReceiver<UserOutbound>,
    pub(super) user: &'a mut User,
    pub(super) user_timeout_ms: u64,
    pub(super) ping_interval_ms: u64,
    pub(super) metrics: &'a RuntimeMetrics,
}

pub(super) async fn run(session: UserLoop<'_>) -> WsSessionLoopExitReason {
    let UserLoop {
        writer,
        reader,
        room,
        user_id,
        connection_id,
        outbound_rx,
        user,
        user_timeout_ms,
        ping_interval_ms,
        metrics,
    } = session;
    let ping_interval = Duration::from_millis(ping_interval_ms);
    let ping_timeout = Duration::from_millis(user_timeout_ms);
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
                if let Some(reason) = handle_transport_state_tick(writer, user).await {
                    return reason;
                }
            }
            () = &mut ping_tick, if matches!(liveness_state, LivenessState::Idle) => {
                if let Some(reason) = handle_transport_state_tick(writer, user).await {
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
                if let Some(reason) = handle_incoming_socket_event(
                    writer,
                    room,
                    user_id,
                    connection_id,
                    user,
                    message,
                    &mut liveness_state,
                    metrics,
                ).await {
                    return reason;
                }
            }
            outbound = outbound_rx.recv() => {
                if let Some(reason) = handle_outbound_event(writer, outbound, user, metrics).await {
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
    user: &User,
) -> Option<WsSessionLoopExitReason> {
    user.disconnect_reason()?;
    debug!("closing websocket because the underlying RTC transport disconnected");
    close_writer(writer, WebSocketCloseCode::Error).await;
    Some(WsSessionLoopExitReason::TransportDisconnected)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the websocket select loop passes the active user context into one socket-event branch"
)]
async fn handle_incoming_socket_event(
    writer: &mut WsWriter,
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    user: &mut User,
    message: Option<Result<Message, AxumError>>,
    liveness_state: &mut LivenessState,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Some(Ok(message)) => {
            handle_incoming_frame(
                writer,
                room,
                user_id,
                connection_id,
                user,
                message,
                liveness_state,
                metrics,
            )
            .await
        }
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

#[allow(
    clippy::too_many_arguments,
    reason = "frame handling needs the socket writer plus room-scoped user context"
)]
async fn handle_incoming_frame(
    writer: &mut WsWriter,
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    user: &mut User,
    message: Message,
    liveness_state: &mut LivenessState,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    match message {
        Message::Ping(payload) => {
            if writer.send(Message::Pong(payload)).await.is_err() {
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
        Message::Text(payload) => {
            handle_text_payload(
                writer,
                room,
                user_id,
                connection_id,
                user,
                &payload,
                metrics,
            )
            .await
        }
        Message::Binary(payload) => {
            handle_binary_payload(
                writer,
                room,
                user_id,
                connection_id,
                user,
                &payload,
                metrics,
            )
            .await
        }
    }
}

async fn handle_binary_payload(
    writer: &mut WsWriter,
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    user: &mut User,
    payload: &[u8],
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    if payload.len() > MAX_CLIENT_FRAME_BYTES {
        metrics.record_ws_bus_invalid_input_failure();
        warn!(
            payload_len = payload.len(),
            max_len = MAX_CLIENT_FRAME_BYTES,
            "received oversized websocket binary frame"
        );
        close_writer(writer, WebSocketCloseCode::ProtocolError).await;
        return Some(WsSessionLoopExitReason::BusBreak);
    }
    match String::from_utf8(payload.to_vec()) {
        Ok(payload) => {
            handle_text_payload(
                writer,
                room,
                user_id,
                connection_id,
                user,
                &payload,
                metrics,
            )
            .await
        }
        Err(_error) => {
            metrics.record_ws_bus_invalid_input_failure();
            warn!("received websocket binary frame with invalid UTF-8");
            close_writer(writer, WebSocketCloseCode::ProtocolError).await;
            Some(WsSessionLoopExitReason::BusBreak)
        }
    }
}

async fn handle_text_payload(
    writer: &mut WsWriter,
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    user: &mut User,
    payload: &str,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    let batch = match decode_client_batch(payload) {
        Ok(batch) => batch,
        Err(error) => {
            match error.kind() {
                ClientBatchDecodeFailureKind::InvalidInput => {
                    metrics.record_ws_bus_invalid_input_failure();
                    warn!(
                        "failed to decode client websocket batch because the payload was invalid"
                    );
                }
                ClientBatchDecodeFailureKind::UnsupportedFeature => {
                    metrics.record_ws_bus_unsupported_feature_failure();
                    warn!(
                        "failed to decode client websocket batch because it used an unsupported feature"
                    );
                }
            }
            close_writer(writer, WebSocketCloseCode::ProtocolError).await;
            return Some(WsSessionLoopExitReason::BusBreak);
        }
    };
    metrics.record_ws_bus_batch_received(batch.len());
    let mut output = UserOutput::new();
    for envelope in batch {
        record_client_envelope_metrics(metrics, &envelope);
        match dispatch_client_envelope(room, user_id, connection_id, user, envelope).await {
            Ok(user_output) => output.extend(user_output),
            Err(error) => {
                let close_code = map_user_error(error);
                close_writer(writer, close_code).await;
                return Some(WsSessionLoopExitReason::BusBreak);
            }
        }
    }
    match send_user_output(writer, output).await {
        Ok(_sent) => None,
        Err(code) => {
            close_writer(writer, code).await;
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
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    user: &mut User,
    envelope: ClientEnvelope,
) -> Result<UserOutput, UserError> {
    match envelope {
        ClientEnvelope::Message(ClientMessage::Info(info)) => user.update_info(info).await,
        ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload { message })) => {
            user.broadcast(message).await
        }
        ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
            user.subscribe_to(&payload.user_id, &payload.states).await
        }
        ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
            user.publish(payload.stream_type).await
        }
        ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
            user.unpublish(payload.stream_type).await
        }
        ClientEnvelope::Response {
            response_to,
            response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
        } => user.complete_negotiation(response_to, answer).await,
        ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StartRecording(payload),
        } => Ok(start_recording(room, user_id, connection_id, request_id, payload).await),
        ClientEnvelope::Request {
            request_id,
            request: ClientRequest::StopRecording,
        } => Ok(stop_recording(room, user_id, connection_id, request_id).await),
        ClientEnvelope::Message(ClientMessage::Auth(_)) => Err(UserError::ProtocolViolation),
    }
}

async fn start_recording(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    request_id: RequestId,
    payload: RecordingOptions,
) -> UserOutput {
    let ok = room
        .start_recording_runtime(user_id, connection_id, payload)
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

async fn stop_recording(
    room: &Room,
    user_id: &UserId,
    connection_id: ConnectionId,
    request_id: RequestId,
) -> UserOutput {
    let ok = room.stop_recording_runtime(user_id, connection_id).await;
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
    writer: &mut WsWriter,
    outbound: Option<UserOutbound>,
    user: &mut User,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    if let Some(outbound) = outbound {
        handle_outbound_payload(writer, outbound, user, metrics).await
    } else {
        debug!("user outbound room closed");
        Some(WsSessionLoopExitReason::OutboundChannelClosed)
    }
}

#[allow(
    clippy::cognitive_complexity,
    reason = "outbound handling keeps protocol send, close-signal handling, and metrics in one explicit user-loop branch"
)]
async fn handle_outbound_payload(
    writer: &mut WsWriter,
    outbound: UserOutbound,
    user: &mut User,
    metrics: &RuntimeMetrics,
) -> Option<WsSessionLoopExitReason> {
    match dispatch_room_outbound(user, outbound).await {
        Ok(output) => match send_user_output(writer, output).await {
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
                    "failed to send outbound user event"
                );
                close_writer_if_terminal(writer, code).await;
                Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
            }
        },
        Err(code) => {
            if code == WebSocketCloseCode::Kicked {
                debug!(close_code = 4003, "closing websocket from outbound signal");
                close_writer(writer, WebSocketCloseCode::Kicked).await;
                Some(WsSessionLoopExitReason::OutboundCloseSignal)
            } else {
                metrics.record_ws_bus_send_failure();
                close_writer_if_terminal(writer, code).await;
                Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
            }
        }
    }
}

async fn dispatch_room_outbound(
    user: &mut User,
    outbound: UserOutbound,
) -> Result<UserOutput, WebSocketCloseCode> {
    match outbound {
        UserOutbound::Close(reason) => Err(map_room_close_reason(reason)),
        UserOutbound::Message(message) => dispatch_room_message(user, message)
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

async fn dispatch_room_message(
    user: &mut User,
    message: RoomEventMessage,
) -> Result<UserOutput, UserError> {
    match message {
        RoomEventMessage::Broadcast { sender_id, message } => {
            user.notify_broadcast(sender_id, message).await
        }
        RoomEventMessage::UserJoined { user_id, info } => user.add_remote_user(user_id, info).await,
        RoomEventMessage::UserDeparted { user_id } => user.remove_remote_user(user_id).await,
        RoomEventMessage::UserInfoChanged(snapshot) => user.update_remote_users(snapshot).await,
        RoomEventMessage::RecordingStateChanged(state) => user.update_recording_state(state).await,
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
        close_writer(writer, code).await;
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
