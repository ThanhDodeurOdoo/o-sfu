use o_sfu_protocol::wire::{
    ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
    JsonPayload, RecordingActionResult, RecordingOptions, RequestId, ServerEnvelope,
    ServerResponse, UserInfo,
};

use super::{User, UserError, UserOutput};

impl User {
    pub async fn apply_client_envelope(
        &mut self,
        envelope: ClientEnvelope,
    ) -> Result<UserOutput, UserError> {
        if let ClientEnvelope::Message(ClientMessage::Auth(_)) = &envelope {
            return Err(UserError::ProtocolViolation);
        }
        self.reject_stale_connection().await?;
        match envelope {
            ClientEnvelope::Message(ClientMessage::Info(info)) => self.update_info(info).await,
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message,
            })) => self.broadcast(message).await,
            ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
                self.subscribe(payload.user_id, payload.states).await
            }
            ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
                self.publish(payload.stream_type, true).await
            }
            ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
                self.publish(payload.stream_type, false).await
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Offer(answer) | ClientResponse::Renegotiate(answer),
            } => self.complete_negotiation(response_to, answer).await,
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

    async fn update_info(&self, info: UserInfo) -> Result<UserOutput, UserError> {
        self.session.update_info(info).await;
        Ok(UserOutput::new())
    }

    async fn broadcast(&self, message: JsonPayload) -> Result<UserOutput, UserError> {
        self.session
            .broadcast(message)
            .await
            .map_err(|_error| UserError::ProtocolViolation)?;
        Ok(UserOutput::new())
    }

    async fn start_recording(
        &self,
        request_id: RequestId,
        options: RecordingOptions,
    ) -> UserOutput {
        let ok = self.session.start_recording(options).await;
        vec![ServerEnvelope::Response {
            response_to: request_id,
            response: ServerResponse::StartRecording(RecordingActionResult { ok }),
        }]
    }

    async fn stop_recording(&self, request_id: RequestId) -> UserOutput {
        let ok = self.session.stop_recording().await;
        vec![ServerEnvelope::Response {
            response_to: request_id,
            response: ServerResponse::StopRecording(RecordingActionResult { ok }),
        }]
    }
}
