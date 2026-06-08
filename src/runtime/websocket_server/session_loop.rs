use std::{str, sync::Arc, time::Duration};

use axum::{
    Error as AxumError,
    extract::ws::{Message, WebSocket},
};
use futures_util::StreamExt;
use o_sfu_protocol::wire::{ClientEnvelope, WebSocketCloseCode};
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

use super::{
    WsReader, WsWriter,
    accepted_user::AcceptedUser,
    admission::PreAuthWebSocketPermit,
    controller::WebSocketServices,
    handshake,
    io::{close_writer_bounded, send_message_bounded, send_user_output_bounded},
};
use crate::{
    application::user_session::{UserError, UserOutput},
    core::server::room::{UserCloseReason, UserOutbound, UserOutboundEvent, UserOutboundOverflow},
    runtime::{
        metrics::{RuntimeMetrics, WsSessionLoopExitReason},
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

            let accepted =
                AcceptedUser::establish(&services, auth, remote_address, &mut writer).await?;

            Some(Self {
                writer,
                reader,
                accepted,
                services,
            })
        }
        .await;
        metrics.record_ws_handshake_duration(started_at.elapsed());
        session
    }

    pub(super) fn record_upgrade_span(&self) {
        self.accepted.record_current_span();
    }

    pub(super) async fn serve(mut self) {
        self.services.metrics.record_ws_user_loop_started();
        let reason = self.run_until_exit().await;
        self.services.metrics.record_ws_user_loop_exit(reason);
        self.accepted.finish(&self.services, reason).await;
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
        if !self.accepted.user.transport_disconnected() {
            return None;
        }
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
                debug!(?frame, "websocket user sent close frame");
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
            if let Some(exit_reason) = self.handle_client_envelope(envelope, &mut output).await {
                return Some(exit_reason);
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

    async fn handle_client_envelope(
        &mut self,
        envelope: ClientEnvelope,
        output: &mut UserOutput,
    ) -> Option<WsSessionLoopExitReason> {
        record_client_envelope_metrics(&self.services.metrics, &envelope);
        match self.accepted.user.apply_client_envelope(envelope).await {
            Ok(user_output) => {
                output.extend(user_output);
                None
            }
            Err(error) => {
                let close_code = map_user_error(error);
                Some(self.close_client_error(close_code).await)
            }
        }
    }

    async fn close_client_error(
        &mut self,
        fallback_code: WebSocketCloseCode,
    ) -> WsSessionLoopExitReason {
        let close_code = if self.accepted.user.is_current_connection().await {
            fallback_code
        } else {
            WebSocketCloseCode::Kicked
        };
        close_writer_bounded(&mut self.writer, close_code).await;
        WsSessionLoopExitReason::BusBreak
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
        if let UserOutbound::Close(reason) = outbound {
            return self.handle_user_close(reason).await;
        }
        match self.accepted.user.apply_room_outbound(outbound).await {
            Ok(output) => self.send_user_output(output, true).await,
            Err(error) => {
                self.handle_outbound_close_code(map_user_error(error), false)
                    .await
            }
        }
    }

    async fn send_user_output(
        &mut self,
        output: UserOutput,
        log_send_failure: bool,
    ) -> Option<WsSessionLoopExitReason> {
        match send_user_output_bounded(&mut self.writer, output).await {
            Ok(batch_len) => {
                self.services.metrics.record_ws_bus_batch_sent(batch_len);
                None
            }
            Err(code) => {
                self.handle_outbound_close_code(code, log_send_failure)
                    .await
            }
        }
    }

    async fn handle_user_close(
        &mut self,
        reason: UserCloseReason,
    ) -> Option<WsSessionLoopExitReason> {
        self.handle_outbound_close_code(map_room_close_reason(reason), false)
            .await
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
