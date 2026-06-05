use std::{str, sync::Arc, time::Duration};

use axum::{
    Error as AxumError,
    extract::ws::{Message, WebSocket},
};
use futures_util::StreamExt;
use o_sfu_protocol::wire::{
    ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
    RecordingActionResult, RecordingOptions, RequestId, ServerResponse, UserId, WebSocketCloseCode,
};
use tokio::time::{Instant, sleep_until};
use tracing::{Instrument, Span, debug, field, info, instrument, warn};

use super::{
    WsReader, WsWriter,
    admission::PreAuthWebSocketPermit,
    controller::WebSocketServices,
    handshake::{self, AuthenticatedJoin},
    io::{close_writer_bounded, send_message_bounded, send_user_output_bounded},
};
use crate::{
    application::user_session::{
        PublishIntent, SubscribeIntent, User, UserError, UserOutput, UserSignal,
    },
    core::server::room::{
        JoinUserRequest, Room, RoomEventRequest, RoomManagerJoinError, UserCloseReason,
        UserOutbound, UserOutboundEvent, UserOutboundOverflow, UserOutboundQueueLimits,
        UserOutboundReceiver, UserOutboundSender,
    },
    runtime::{
        ConnectionId, MediaTransport,
        metrics::{RuntimeMetrics, WsSessionLoopExitReason},
        room::RoomManager,
        telemetry::{
            self,
            schema::{event as telemetry_event, field as telemetry_field},
        },
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

pub(super) struct ActiveWebSocketSession {
    writer: WsWriter,
    reader: WsReader,
    accepted: AcceptedUser,
    services: WebSocketServices,
    remote_address: Arc<str>,
}

impl ActiveWebSocketSession {
    pub(super) async fn accept(
        socket: WebSocket,
        services: WebSocketServices,
        remote_address: Arc<str>,
        pre_auth_permit: PreAuthWebSocketPermit,
    ) -> Option<Self> {
        let metrics = Arc::clone(&services.metrics);
        let started_at = Instant::now();
        let session = async {
            let (mut writer, mut reader) = socket.split();
            let auth =
                handshake::receive(&services, &mut writer, &mut reader, remote_address.as_ref())
                    .await?;
            drop(pre_auth_permit);

            let mut accepted =
                AcceptedUser::join(&services, auth, Arc::clone(&remote_address), &mut writer)
                    .await?;
            services.metrics.record_ws_user_joined();
            accepted.record_span(&Span::current(), remote_address.as_ref());
            accepted.log_established(remote_address.as_ref());

            let initialization_span = telemetry::activated_span(tracing::info_span!(
                "user.initialize",
                room_id = %accepted.room_id(),
                user_id = ?accepted.user_id(),
                connection_id = ?accepted.connection_id(),
                remote_address = %remote_address
            ));
            accepted
                .start(&services, &mut writer, remote_address.as_ref())
                .instrument(initialization_span)
                .await?;

            Some(Self {
                writer,
                reader,
                accepted,
                services,
                remote_address,
            })
        }
        .await;
        metrics.record_ws_handshake_duration(started_at.elapsed());
        session
    }

    pub(super) fn record_upgrade_span(&self) {
        self.accepted
            .record_span(&Span::current(), self.remote_address.as_ref());
    }

    pub(super) async fn serve(mut self) {
        self.services.metrics.record_ws_user_loop_started();
        let reason = self.run_until_exit().await;
        self.services.metrics.record_ws_user_loop_exit(reason);
        info!(
            event = telemetry_event::WS_CONNECTION_CLOSED,
            connection_id = ?self.accepted.connection_id(),
            remote_address = self.remote_address.as_ref(),
            ?reason,
            "closing websocket user"
        );
        self.accepted.close().await;
    }

    async fn run_until_exit(&mut self) -> WsSessionLoopExitReason {
        let ping_interval = Duration::from_millis(self.services.user.ping_interval_ms);
        let ping_timeout = Duration::from_millis(self.services.user.timeout_ms);
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
                biased;
                () = &mut transport_state_tick => {
                    next_transport_state_check_at = Instant::now() + ping_interval;
                    if let Some(reason) = self.handle_transport_state_tick().await {
                        return reason;
                    }
                }
                () = &mut ping_tick, if matches!(liveness_state, LivenessState::Idle) => {
                    if let Some(reason) = self.handle_transport_state_tick().await {
                        return reason;
                    }
                    if let Some(reason) = self.handle_ping_tick(
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
                    close_writer_bounded(&mut self.writer, WebSocketCloseCode::Error).await;
                    return WsSessionLoopExitReason::PingTimeout;
                }
                outbound = self.accepted.outbound_rx.recv_event() => {
                    if let Some(reason) = self.handle_outbound_event(outbound).await {
                        return reason;
                    }
                }
                message = self.reader.next() => {
                    if let Some(reason) = self
                        .handle_incoming_socket_event(message, &mut liveness_state)
                        .await
                    {
                        return reason;
                    }
                }
            }
        }
    }

    async fn handle_ping_tick(
        &mut self,
        ping_interval: Duration,
        ping_timeout: Duration,
        next_ping_at: &mut Instant,
        liveness_state: &mut LivenessState,
    ) -> Option<WsSessionLoopExitReason> {
        if send_message_bounded(&mut self.writer, Message::Ping(Vec::new().into()))
            .await
            .is_err()
        {
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

    async fn handle_transport_state_tick(&mut self) -> Option<WsSessionLoopExitReason> {
        self.accepted.user.disconnect_reason()?;
        debug!("closing websocket because the underlying RTC transport disconnected");
        close_writer_bounded(&mut self.writer, WebSocketCloseCode::Error).await;
        Some(WsSessionLoopExitReason::TransportDisconnected)
    }

    async fn handle_incoming_socket_event(
        &mut self,
        message: Option<Result<Message, AxumError>>,
        liveness_state: &mut LivenessState,
    ) -> Option<WsSessionLoopExitReason> {
        match message {
            Some(Ok(message)) => self.handle_incoming_frame(message, liveness_state).await,
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
        &mut self,
        message: Message,
        liveness_state: &mut LivenessState,
    ) -> Option<WsSessionLoopExitReason> {
        match message {
            Message::Ping(payload) => {
                if send_message_bounded(&mut self.writer, Message::Pong(payload))
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
            Message::Text(payload) => self.handle_text_payload(&payload).await,
            Message::Binary(payload) => self.handle_binary_payload(&payload).await,
        }
    }

    async fn handle_binary_payload(&mut self, payload: &[u8]) -> Option<WsSessionLoopExitReason> {
        if payload.len() > MAX_CLIENT_FRAME_BYTES {
            self.services.metrics.record_ws_bus_invalid_input_failure();
            warn!(
                payload_len = payload.len(),
                max_len = MAX_CLIENT_FRAME_BYTES,
                "received oversized websocket binary frame"
            );
            return Some(
                self.close_client_error(WebSocketCloseCode::ProtocolError)
                    .await,
            );
        }
        match str::from_utf8(payload) {
            Ok(payload) => self.handle_text_payload(payload).await,
            Err(_error) => {
                self.services.metrics.record_ws_bus_invalid_input_failure();
                warn!("received websocket binary frame with invalid UTF-8");
                Some(
                    self.close_client_error(WebSocketCloseCode::ProtocolError)
                        .await,
                )
            }
        }
    }

    async fn handle_text_payload(&mut self, payload: &str) -> Option<WsSessionLoopExitReason> {
        let batch = match decode_client_batch(payload) {
            Ok(batch) => batch,
            Err(error) => {
                match error.kind() {
                    ClientBatchDecodeFailureKind::InvalidInput => {
                        self.services.metrics.record_ws_bus_invalid_input_failure();
                        warn!(
                            "failed to decode client websocket batch because the payload was invalid"
                        );
                    }
                    ClientBatchDecodeFailureKind::UnsupportedFeature => {
                        self.services
                            .metrics
                            .record_ws_bus_unsupported_feature_failure();
                        warn!(
                            "failed to decode client websocket batch because it used an unsupported feature"
                        );
                    }
                }
                return Some(
                    self.close_client_error(WebSocketCloseCode::ProtocolError)
                        .await,
                );
            }
        };
        self.services
            .metrics
            .record_ws_bus_batch_received(batch.len());
        let mut output = UserOutput::new();
        for envelope in batch {
            record_client_envelope_metrics(&self.services.metrics, &envelope);
            match self.dispatch_client_envelope(envelope).await {
                Ok(user_output) => output.extend(user_output),
                Err(error) => {
                    let close_code = map_user_error(error);
                    return Some(self.close_client_error(close_code).await);
                }
            }
        }
        match send_user_output_bounded(&mut self.writer, output).await {
            Ok(_sent) => None,
            Err(code) => {
                close_writer_bounded(&mut self.writer, code).await;
                Some(WsSessionLoopExitReason::BusBreak)
            }
        }
    }

    async fn close_client_error(
        &mut self,
        fallback_code: WebSocketCloseCode,
    ) -> WsSessionLoopExitReason {
        let close_code = if self.accepted.lease.is_stale().await {
            WebSocketCloseCode::Kicked
        } else {
            fallback_code
        };
        close_writer_bounded(&mut self.writer, close_code).await;
        WsSessionLoopExitReason::BusBreak
    }

    async fn dispatch_client_envelope(
        &mut self,
        envelope: ClientEnvelope,
    ) -> Result<UserOutput, UserError> {
        match envelope {
            ClientEnvelope::Message(ClientMessage::Info(info)) => {
                self.accepted.user.update_info(info).await
            }
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message,
            })) => self.accepted.user.broadcast(message).await,
            ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
                self.accepted
                    .user
                    .subscribe(SubscribeIntent::new(payload.user_id, payload.states))
                    .await
            }
            ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
                self.accepted
                    .user
                    .publish(PublishIntent::Start(payload.stream_type))
                    .await
            }
            ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
                self.accepted
                    .user
                    .publish(PublishIntent::Stop(payload.stream_type))
                    .await
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
            } => {
                self.accepted
                    .user
                    .complete_negotiation(response_to, answer)
                    .await
            }
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StartRecording(payload),
            } => Ok(self.start_recording(request_id, payload).await),
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StopRecording,
            } => Ok(self.stop_recording(request_id).await),
            ClientEnvelope::Message(ClientMessage::Auth(_)) => Err(UserError::ProtocolViolation),
        }
    }

    async fn start_recording(
        &self,
        request_id: RequestId,
        payload: RecordingOptions,
    ) -> UserOutput {
        let ok = self.accepted.lease.start_recording(payload).await;
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

    async fn stop_recording(&self, request_id: RequestId) -> UserOutput {
        let ok = self.accepted.lease.stop_recording().await;
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
        &mut self,
        outbound: UserOutboundEvent,
    ) -> Option<WsSessionLoopExitReason> {
        match outbound {
            UserOutboundEvent::Message(outbound) => self.handle_outbound_payload(outbound).await,
            UserOutboundEvent::Overflow(overflow) => {
                handle_outbound_overflow(&mut self.writer, overflow).await;
                Some(WsSessionLoopExitReason::OutboundQueueOverflow)
            }
            UserOutboundEvent::Closed => {
                debug!("user outbound room closed");
                Some(WsSessionLoopExitReason::OutboundChannelClosed)
            }
        }
    }

    async fn handle_outbound_payload(
        &mut self,
        outbound: UserOutbound,
    ) -> Option<WsSessionLoopExitReason> {
        match dispatch_room_outbound(&mut self.accepted.user, outbound).await {
            Ok(output) => match send_user_output_bounded(&mut self.writer, output).await {
                Ok(batch_len) => {
                    self.services.metrics.record_ws_bus_batch_sent(batch_len);
                    None
                }
                Err(code) => self.handle_outbound_close_code(code, true).await,
            },
            Err(code) => self.handle_outbound_close_code(code, false).await,
        }
    }

    async fn handle_outbound_close_code(
        &mut self,
        code: WebSocketCloseCode,
        log_send_failure: bool,
    ) -> Option<WsSessionLoopExitReason> {
        if code == WebSocketCloseCode::Kicked {
            debug!(close_code = 4108, "closing websocket from outbound signal");
            close_writer_bounded(&mut self.writer, WebSocketCloseCode::Kicked).await;
            return Some(WsSessionLoopExitReason::OutboundCloseSignal);
        }
        self.services.metrics.record_ws_bus_send_failure();
        if log_send_failure {
            debug!(
                close_code = u16::from(code),
                "failed to send outbound user event"
            );
        }
        close_writer_if_terminal(&mut self.writer, code).await;
        Some(WsSessionLoopExitReason::OutboundMessageSendFailure)
    }
}

struct AcceptedUser {
    lease: RoomUserLease,
    outbound_rx: UserOutboundReceiver,
    user: User,
}

impl AcceptedUser {
    #[instrument(
        name = "room.join",
        skip_all,
        fields(room_id = %auth.room.uuid(), user_id = ?auth.claims.user_id)
    )]
    async fn join(
        state: &WebSocketServices,
        auth: AuthenticatedJoin,
        remote_address: Arc<str>,
        writer: &mut WsWriter,
    ) -> Option<Self> {
        let AuthenticatedJoin { room, claims } = auth;
        let user_id = claims.user_id;
        let label = claims.label;
        let permissions = claims.permissions.unwrap_or_default();
        let (outbound_tx, outbound_rx) = UserOutboundSender::channel_with_limits(
            UserOutboundQueueLimits::new(
                state.user.outbound_queue_capacity,
                state.user.outbound_queue_byte_capacity,
            ),
            Arc::clone(&state.metrics),
        );
        let join_result = state
            .room_manager
            .join_user(
                room.uuid(),
                JoinUserRequest {
                    user_id: user_id.clone(),
                    label,
                    permissions,
                    sender: outbound_tx,
                },
                &state.media_transport,
            )
            .await;
        match join_result {
            Ok(admission) => {
                let room = Arc::clone(admission.room());
                let connection_id = admission.connection_id();
                let user = User::new(
                    user_id.clone(),
                    connection_id,
                    admission.transport_session_key().clone(),
                    Arc::clone(&remote_address),
                    Arc::clone(&room),
                    state.sfu_core.clone(),
                );
                let lease = RoomUserLease {
                    room,
                    user_id,
                    connection_id,
                    room_manager: Arc::clone(&state.room_manager),
                    media_transport: state.media_transport.clone(),
                };
                info!(
                    event = telemetry_event::WS_JOIN_SUCCEEDED,
                    connection_id = ?lease.connection_id,
                    "joined websocket user"
                );
                Some(Self {
                    lease,
                    outbound_rx,
                    user,
                })
            }
            Err(error) => {
                let close_code = match error {
                    RoomManagerJoinError::RoomFull => WebSocketCloseCode::RoomFull,
                    RoomManagerJoinError::MissingRoom | RoomManagerJoinError::RouterState => {
                        WebSocketCloseCode::AuthFailed
                    }
                };
                warn!(
                    event = telemetry_event::WS_JOIN_FAILED,
                    ?user_id,
                    remote_address = remote_address.as_ref(),
                    ?error,
                    close_code = u16::from(close_code),
                    "rejecting websocket because the authenticated user could not join the room"
                );
                super::handshake::reject_handshake(
                    state,
                    Some(writer),
                    Some(close_code),
                    remote_address.as_ref(),
                    "rejecting websocket during user join",
                )
                .await
            }
        }
    }

    fn record_span(&self, span: &Span, remote_address: &str) {
        span.record("room_id", field::display(self.room_id()));
        span.record("user_id", field::debug(self.user_id()));
        span.record("connection_id", field::debug(self.connection_id()));
        span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(remote_address),
        );
    }

    fn log_established(&self, remote_address: &str) {
        info!(
            event = telemetry_event::WS_USER_ESTABLISHED,
            connection_id = ?self.connection_id(),
            remote_address,
            "websocket user established"
        );
    }

    #[o_sfu_telemetry::measure_duration(
        metrics = "state.metrics",
        record = "record_ws_user_initialize_duration"
    )]
    async fn start(
        &mut self,
        state: &WebSocketServices,
        writer: &mut WsWriter,
        remote_address: &str,
    ) -> Option<()> {
        let output = match self.user.start().await {
            Ok(output) => output,
            Err(_error) => {
                warn!(
                    event = telemetry_event::WS_JOIN_FAILED,
                    user_id = ?self.user_id(),
                    connection_id = ?self.connection_id(),
                    remote_address,
                    outcome = "user_initialize_failed",
                    "failed to initialize websocket user"
                );
                state.metrics.record_ws_user_initialize_failure();
                self.close().await;
                return None;
            }
        };
        if send_user_output_bounded(writer, output).await.is_err() {
            debug!(
                user_id = ?self.user_id(),
                connection_id = ?self.connection_id(),
                "failed to send user startup payload"
            );
            state.metrics.record_ws_startup_send_failure();
            warn!(
                event = telemetry_event::WS_JOIN_FAILED,
                user_id = ?self.user_id(),
                connection_id = ?self.connection_id(),
                remote_address,
                outcome = "startup_send_failed",
                "failed to send websocket user startup payload"
            );
            self.close().await;
            return None;
        }
        Some(())
    }

    async fn close(&mut self) {
        self.user.close().await;
        self.lease.close().await;
    }

    fn room_id(&self) -> &str {
        self.lease.room.uuid()
    }

    fn user_id(&self) -> &UserId {
        &self.lease.user_id
    }

    const fn connection_id(&self) -> ConnectionId {
        self.lease.connection_id
    }
}

struct RoomUserLease {
    room: Arc<Room>,
    user_id: UserId,
    connection_id: ConnectionId,
    room_manager: Arc<RoomManager>,
    media_transport: MediaTransport,
}

impl RoomUserLease {
    async fn close(&self) {
        self.room_manager
            .close_session(
                self.room.uuid(),
                &self.user_id,
                self.connection_id,
                &self.media_transport,
            )
            .await;
    }

    async fn is_stale(&self) -> bool {
        !self
            .room
            .has_connection(&self.user_id, self.connection_id)
            .await
    }

    async fn start_recording(&self, payload: RecordingOptions) -> bool {
        self.room
            .start_recording_runtime(&self.user_id, self.connection_id, payload)
            .await
    }

    async fn stop_recording(&self) -> bool {
        self.room
            .stop_recording_runtime(&self.user_id, self.connection_id)
            .await
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

fn record_client_envelope_metrics(metrics: &RuntimeMetrics, envelope: &ClientEnvelope) {
    match envelope {
        ClientEnvelope::Request { .. } => metrics.record_ws_bus_client_request(),
        ClientEnvelope::Message(_) => metrics.record_ws_bus_client_message(),
        ClientEnvelope::Response { .. } => {}
    }
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
            RoomEventRequest::SetupRemoteTrack(payload) => {
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
