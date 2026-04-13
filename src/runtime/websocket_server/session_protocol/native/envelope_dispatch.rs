use crate::runtime::websocket_server::WsWriter;
use crate::signaling::protocol::{
    ClientBroadcastPayload, ClientEnvelope, ClientMessage, ClientRequest, ClientResponse,
    RecordingActionResult, ServerResponse, WebSocketCloseCode,
};

use super::super::{controller::SessionProtocolOutcome, frame_codec::send_server_response};
use super::controller::NativeSessionProtocol;

impl NativeSessionProtocol {
    pub(super) async fn dispatch_client_envelope(
        &mut self,
        writer: &mut WsWriter,
        envelope: ClientEnvelope,
    ) -> SessionProtocolOutcome {
        match envelope {
            ClientEnvelope::Message(ClientMessage::Info(info)) => {
                self.channel
                    .update_session_info(&self.session_id, info, false)
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Broadcast(ClientBroadcastPayload {
                message,
            })) => {
                self.channel.broadcast(&self.session_id, message).await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Subscribe(payload)) => {
                self.channel
                    .update_subscription(
                        &self.session_id,
                        &payload.session_id,
                        &payload.states,
                        &self.transport_adapter,
                    )
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Message(ClientMessage::Publish(payload)) => {
                self.handle_publish_intent(writer, payload.stream_type)
                    .await
            }
            ClientEnvelope::Message(ClientMessage::Unpublish(payload)) => {
                self.handle_unpublish_intent_with_writer(payload.stream_type, Some(writer))
                    .await;
                SessionProtocolOutcome::Continue
            }
            ClientEnvelope::Response {
                response_to,
                response: ClientResponse::Ping,
            } if self.request_state.resolve_ping_response(&response_to) => {
                SessionProtocolOutcome::Continue
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
                let ok = self
                    .channel
                    .start_recording(&self.session_id, payload)
                    .await;
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
            ClientEnvelope::Request {
                request_id,
                request: ClientRequest::StopRecording,
            } => {
                let ok = self.channel.stop_recording(&self.session_id).await;
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
            ClientEnvelope::Response { .. } | ClientEnvelope::Message(ClientMessage::Auth(_)) => {
                SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError)
            }
        }
    }
}
