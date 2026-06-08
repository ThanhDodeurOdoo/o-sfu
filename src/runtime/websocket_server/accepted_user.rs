use std::sync::Arc;

use o_sfu_protocol::wire::WebSocketCloseCode;
use tracing::{Instrument, Span, field, info, info_span, instrument, warn};

use super::{
    WsWriter,
    controller::WebSocketServices,
    handshake::{self, AuthenticatedJoin},
    io::send_user_output_bounded,
};
use crate::{
    application::user_session::User,
    core::server::room::{
        JoinUserRequest, RoomManagerJoinError, UserOutboundQueueLimits, UserOutboundReceiver,
        UserOutboundSender,
    },
    runtime::{
        metrics::WsSessionLoopExitReason,
        telemetry::{
            self,
            schema::{event as telemetry_event, field as telemetry_field},
        },
    },
};

pub(super) struct AcceptedUser {
    pub(super) outbound_rx: UserOutboundReceiver,
    pub(super) user: User,
}

impl AcceptedUser {
    pub(super) async fn establish(
        state: &WebSocketServices,
        auth: AuthenticatedJoin,
        remote_address: Arc<str>,
        writer: &mut WsWriter,
    ) -> Option<Self> {
        let mut accepted = Self::join(state, auth, remote_address, writer).await?;
        state.metrics.record_ws_user_joined();
        accepted.record_current_span();
        accepted.start(state, writer).await?;
        Some(accepted)
    }

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
                let connection_id = admission.connection_id;
                let user = User::new(
                    user_id.clone(),
                    connection_id,
                    admission.transport_session_key,
                    Arc::clone(&remote_address),
                    &admission.room,
                    &state.sfu_core,
                );
                Some(Self { outbound_rx, user })
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
                handshake::reject_handshake(
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

    pub(super) fn record_current_span(&self) {
        let span = Span::current();
        span.record("room_id", field::display(self.user.room_id()));
        span.record("user_id", field::debug(self.user.id()));
        span.record("connection_id", field::debug(self.user.connection_id()));
        span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(self.user.remote_address()),
        );
    }

    async fn start(&mut self, state: &WebSocketServices, writer: &mut WsWriter) -> Option<()> {
        let span = telemetry::activated_span(info_span!(
            "user.initialize",
            room_id = %self.user.room_id(),
            user_id = ?self.user.id(),
            connection_id = ?self.user.connection_id(),
            remote_address = %self.user.remote_address()
        ));
        self.start_inner(state, writer).instrument(span).await
    }

    #[o_sfu_telemetry::measure_duration(
        metrics = "state.metrics",
        record = "record_ws_user_initialize_duration"
    )]
    async fn start_inner(
        &mut self,
        state: &WebSocketServices,
        writer: &mut WsWriter,
    ) -> Option<()> {
        let output = match self.user.start().await {
            Ok(output) => output,
            Err(_error) => {
                warn!(
                    event = telemetry_event::WS_JOIN_FAILED,
                    user_id = ?self.user.id(),
                    connection_id = ?self.user.connection_id(),
                    remote_address = self.user.remote_address(),
                    outcome = "user_initialize_failed",
                    "failed to initialize websocket user"
                );
                state.metrics.record_ws_user_initialize_failure();
                self.close(state).await;
                return None;
            }
        };
        if send_user_output_bounded(writer, output).await.is_err() {
            tracing::debug!(
                user_id = ?self.user.id(),
                connection_id = ?self.user.connection_id(),
                "failed to send user startup payload"
            );
            state.metrics.record_ws_startup_send_failure();
            warn!(
                event = telemetry_event::WS_JOIN_FAILED,
                user_id = ?self.user.id(),
                connection_id = ?self.user.connection_id(),
                remote_address = self.user.remote_address(),
                outcome = "startup_send_failed",
                "failed to send websocket user startup payload"
            );
            self.close(state).await;
            return None;
        }
        Some(())
    }

    pub(super) async fn finish(
        &mut self,
        state: &WebSocketServices,
        reason: WsSessionLoopExitReason,
    ) {
        info!(
            event = telemetry_event::WS_CONNECTION_CLOSED,
            connection_id = ?self.user.connection_id(),
            remote_address = self.user.remote_address(),
            ?reason,
            "closing websocket user"
        );
        self.close(state).await;
    }

    async fn close(&mut self, state: &WebSocketServices) {
        self.user.close().await;
        state
            .room_manager
            .close_session(
                self.user.room_id(),
                self.user.id(),
                self.user.connection_id(),
                &state.media_transport,
            )
            .await;
    }
}
