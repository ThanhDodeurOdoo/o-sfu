use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::websocket_server::WsWriter;
use o_sfu_protocol::shared::{DownloadStates, SessionId, SessionInfo};
use o_sfu_protocol::signaling::{
    ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
    RecordingActionResult, RecordingOptions, RequestId, ServerResponse, WebSocketCloseCode,
};
use tracing::{debug, info, instrument};

use super::super::{
    controller::SessionProtocolOutcome, flow_state::FlowChange, frame_codec::send_server_response,
};
use super::controller::PostAuthSessionProtocol;

impl PostAuthSessionProtocol {
    async fn reject_stale_connection(&self) -> bool {
        if self
            .channel
            .has_connection(&self.session_id, self.connection_id)
            .await
        {
            return false;
        }
        debug!(
            session_id = ?self.session_id,
            connection_id = ?self.connection_id,
            "rejecting client envelope from a stale websocket connection"
        );
        true
    }

    async fn handle_info_message(&self, info: SessionInfo) {
        self.channel
            .update_session_info_runtime_for_connection(
                &self.session_id,
                self.connection_id,
                info,
                false,
                &self.transport_adapter,
            )
            .await;
    }

    #[instrument(
        name = "subscribe.intent",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id,
            target_session_id = ?target_session_id
        )
    )]
    async fn handle_subscribe_intent(
        &self,
        target_session_id: &SessionId,
        states: &DownloadStates,
    ) {
        info!(
            event = telemetry_event::SUBSCRIBE_PREPARED,
            operation = "consume_prepare",
            outcome = "request_received",
            "received subscribe intent"
        );
        self.channel
            .update_subscription_runtime(
                &self.session_id,
                self.connection_id,
                target_session_id,
                states,
                &self.transport_adapter,
            )
            .await;
        info!(
            event = telemetry_event::SUBSCRIBE_SUCCEEDED,
            operation = "consume_prepare",
            outcome = "applied",
            "applied subscribe intent"
        );
    }

    #[instrument(
        name = "recording.start",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id,
            request_id = ?request_id
        )
    )]
    async fn handle_start_recording_request(
        &self,
        writer: &mut WsWriter,
        request_id: RequestId,
        payload: RecordingOptions,
    ) -> SessionProtocolOutcome {
        let ok = self
            .channel
            .start_recording_runtime(&self.session_id, self.connection_id, payload)
            .await;
        info!(
            event = telemetry_event::RECORDING_STARTED,
            operation = "recording_start",
            outcome = if ok { "accepted" } else { "rejected" },
            "processed recording start request"
        );
        match send_server_response(
            writer,
            request_id,
            ServerResponse::StartRecording(RecordingActionResult { ok }),
        )
        .await
        {
            Ok(()) => SessionProtocolOutcome::Continue,
            Err(code) => SessionProtocolOutcome::Close(code),
        }
    }

    #[instrument(
        name = "recording.stop",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id,
            request_id = ?request_id
        )
    )]
    async fn handle_stop_recording_request(
        &self,
        writer: &mut WsWriter,
        request_id: RequestId,
    ) -> SessionProtocolOutcome {
        let ok = self
            .channel
            .stop_recording_runtime(&self.session_id, self.connection_id)
            .await;
        info!(
            event = telemetry_event::RECORDING_STOPPED,
            operation = "recording_stop",
            outcome = if ok { "accepted" } else { "rejected" },
            "processed recording stop request"
        );
        match send_server_response(
            writer,
            request_id,
            ServerResponse::StopRecording(RecordingActionResult { ok }),
        )
        .await
        {
            Ok(()) => SessionProtocolOutcome::Continue,
            Err(code) => SessionProtocolOutcome::Close(code),
        }
    }

    async fn dispatch_flow_change(
        &mut self,
        writer: &mut WsWriter,
        change: FlowChange,
    ) -> SessionProtocolOutcome {
        match change {
            FlowChange::Publish(stream_type) => {
                self.handle_publish_intent(writer, stream_type).await
            }
            FlowChange::Unpublish(stream_type) => {
                self.handle_unpublish_intent_with_writer(stream_type, Some(writer))
                    .await
            }
            FlowChange::Subscribe {
                target_session_id,
                states,
            } => {
                self.handle_subscribe_intent(&target_session_id, &states)
                    .await;
                SessionProtocolOutcome::Continue
            }
        }
    }

    pub(super) async fn dispatch_client_envelope(
        &mut self,
        writer: &mut WsWriter,
        envelope: ClientEnvelope,
    ) -> SessionProtocolOutcome {
        if self.reject_stale_connection().await {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::Kicked);
        }
        match envelope {
            ClientEnvelope::Message(ClientMessage::Info(info)) => {
                self.handle_info_message(info).await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message,
            })) => {
                self.channel
                    .broadcast_runtime(&self.session_id, self.connection_id, message)
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
                self.dispatch_flow_change(
                    writer,
                    FlowChange::Subscribe {
                        target_session_id: payload.session_id,
                        states: payload.states,
                    },
                )
                .await
            }
            ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
                self.dispatch_flow_change(writer, FlowChange::Publish(payload.stream_type))
                    .await
            }
            ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
                self.dispatch_flow_change(writer, FlowChange::Unpublish(payload.stream_type))
                    .await
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
            } => {
                self.handle_negotiation_response(writer, response_to, answer)
                    .await
            }
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StartRecording(payload),
            } => {
                self.handle_start_recording_request(writer, request_id, payload)
                    .await
            }
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StopRecording,
            } => self.handle_stop_recording_request(writer, request_id).await,
            ClientEnvelope::Message(ClientMessage::Auth(_)) => {
                SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }
}
