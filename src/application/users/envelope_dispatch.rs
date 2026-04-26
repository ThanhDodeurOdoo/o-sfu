use o_sfu_protocol::{
    shared::{DownloadStates, UserId, UserInfo},
    signaling::{
        ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
        RecordingActionResult, RecordingOptions, RequestId, ServerResponse,
    },
};
use tracing::{debug, info, instrument};

use super::{User, flow_state::FlowChange};
use crate::{
    application::outcomes::{CallOutcome, UserError, UserSignal},
    runtime::telemetry::schema::event as telemetry_event,
};

impl User {
    async fn reject_stale_connection(&self) -> bool {
        if self.room.has_connection(&self.id, self.connection_id).await {
            return false;
        }
        debug!(
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            "rejecting client envelope from a stale websocket connection"
        );
        true
    }

    async fn handle_info_message(&self, info: UserInfo) {
        self.media_core
            .update_user_info(
                self.room.as_core_room(),
                &self.id,
                self.connection_id,
                info,
                false,
            )
            .await;
    }

    #[instrument(
        name = "subscribe.intent",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            target_session_id = ?target_session_id
        )
    )]
    async fn handle_subscribe_intent(&self, target_session_id: &UserId, states: &DownloadStates) {
        info!(
            event = telemetry_event::SUBSCRIBE_PREPARED,
            operation = "consume_prepare",
            outcome = "request_received",
            "received subscribe intent"
        );
        self.media_core
            .update_subscription(
                self.room.as_core_room(),
                &self.id,
                self.connection_id,
                target_session_id,
                states,
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
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            request_id = ?request_id
        )
    )]
    async fn handle_start_recording_request(
        &self,
        request_id: RequestId,
        payload: RecordingOptions,
    ) -> CallOutcome {
        let ok = self
            .room
            .start_recording(&self.id, self.connection_id, payload)
            .await;
        info!(
            event = telemetry_event::RECORDING_STARTED,
            operation = "recording_start",
            outcome = if ok { "accepted" } else { "rejected" },
            "processed recording start request"
        );
        CallOutcome::new().with_signal(UserSignal::response(
            request_id,
            ServerResponse::StartRecording(RecordingActionResult { ok }),
        ))
    }

    #[instrument(
        name = "recording.stop",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            request_id = ?request_id
        )
    )]
    async fn handle_stop_recording_request(&self, request_id: RequestId) -> CallOutcome {
        let ok = self.room.stop_recording(&self.id, self.connection_id).await;
        info!(
            event = telemetry_event::RECORDING_STOPPED,
            operation = "recording_stop",
            outcome = if ok { "accepted" } else { "rejected" },
            "processed recording stop request"
        );
        CallOutcome::new().with_signal(UserSignal::response(
            request_id,
            ServerResponse::StopRecording(RecordingActionResult { ok }),
        ))
    }

    async fn dispatch_flow_change(&mut self, change: FlowChange) -> Result<CallOutcome, UserError> {
        match change {
            FlowChange::Publish(stream_type) => self.handle_publish_intent(stream_type).await,
            FlowChange::Unpublish(stream_type) => self.handle_unpublish_intent(stream_type).await,
            FlowChange::Subscribe {
                target_session_id,
                states,
            } => {
                self.handle_subscribe_intent(&target_session_id, &states)
                    .await;
                Ok(CallOutcome::new())
            }
        }
    }

    pub(super) async fn handle_client_envelope(
        &mut self,
        envelope: ClientEnvelope,
    ) -> Result<CallOutcome, UserError> {
        if self.reject_stale_connection().await {
            return Err(UserError::Kicked);
        }
        match envelope {
            ClientEnvelope::Message(ClientMessage::Info(info)) => {
                self.handle_info_message(info).await;
                Ok(CallOutcome::new())
            }
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message,
            })) => {
                self.room
                    .broadcast(&self.id, self.connection_id, message)
                    .await;
                Ok(CallOutcome::new())
            }
            ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
                self.dispatch_flow_change(FlowChange::Subscribe {
                    target_session_id: payload.user_id,
                    states: payload.states,
                })
                .await
            }
            ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
                self.dispatch_flow_change(FlowChange::Publish(payload.stream_type))
                    .await
            }
            ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
                self.dispatch_flow_change(FlowChange::Unpublish(payload.stream_type))
                    .await
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
            } => self.handle_negotiation_response(response_to, answer).await,
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StartRecording(payload),
            } => Ok(self
                .handle_start_recording_request(request_id, payload)
                .await),
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StopRecording,
            } => Ok(self.handle_stop_recording_request(request_id).await),
            ClientEnvelope::Message(ClientMessage::Auth(_)) => Err(UserError::ProtocolViolation),
        }
    }
}
