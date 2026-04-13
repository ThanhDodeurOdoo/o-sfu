use crate::runtime::stub_bus::WsWriter;
use crate::signaling::protocol::{
    RequestId, ServerRequest, SessionDescriptionPayload, WebSocketCloseCode,
};

use super::super::{
    controller::SessionProtocolOutcome,
    frame_codec::send_server_request,
    negotiation::{PendingNegotiationAction, PendingNegotiationRequest, RenegotiationDisposition},
};
use super::controller::NativeSessionProtocol;

impl NativeSessionProtocol {
    pub(in crate::runtime::websocket_server) async fn send_initial_offer(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        let router_capabilities = self.channel.router_rtp_capabilities().await;
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        let bootstrap_payload = self
            .transport_adapter
            .transport_bootstrap_payload(&session_key, &router_capabilities)
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        let offer = self
            .transport_adapter
            .create_initial_session_offer(&session_key)
            .await
            .map_err(|_error| WebSocketCloseCode::Error)?;
        let offer_request = ServerRequest::Offer(SessionDescriptionPayload {
            sdp: offer.into_sdp(),
        });
        self.issue_negotiation_request(
            writer,
            offer_request,
            PendingNegotiationAction::EstablishSession {
                client_rtp_capabilities: bootstrap_payload.router_capabilities,
            },
        )
        .await
    }

    async fn issue_negotiation_request(
        &mut self,
        writer: &mut WsWriter,
        request: ServerRequest,
        action: PendingNegotiationAction,
    ) -> Result<(), WebSocketCloseCode> {
        let request_id = self.request_state.next_request_id();
        send_server_request(writer, request_id.clone(), request.clone()).await?;
        self.negotiation.issue(request_id, request, action);
        Ok(())
    }

    pub(super) async fn request_renegotiation(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<bool, WebSocketCloseCode> {
        match self.negotiation.request_renegotiation() {
            RenegotiationDisposition::Skip | RenegotiationDisposition::QueueOnly => Ok(false),
            RenegotiationDisposition::SendNow => {
                let session_key = self
                    .channel
                    .transport_session_key(&self.session_id, self.connection_id);
                let offer = self
                    .transport_adapter
                    .create_session_renegotiation_offer(&session_key)
                    .await
                    .map_err(|_error| WebSocketCloseCode::Error)?;
                self.issue_negotiation_request(
                    writer,
                    ServerRequest::Renegotiate(SessionDescriptionPayload {
                        sdp: offer.into_sdp(),
                    }),
                    PendingNegotiationAction::RefreshSession,
                )
                .await?;
                Ok(true)
            }
        }
    }

    pub(super) async fn handle_negotiation_response(
        &mut self,
        writer: &mut WsWriter,
        response_to: RequestId,
        answer: SessionDescriptionPayload,
    ) -> SessionProtocolOutcome {
        if answer.sdp.is_empty() {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        let Some(resolved) = self.negotiation.resolve_answer(&response_to) else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        if self
            .transport_adapter
            .apply_session_answer(&session_key, &answer.sdp)
            .await
            .is_err()
        {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::Error);
        }
        self.commit_pending_publishes().await;
        if self
            .apply_negotiation_action(&resolved.pending)
            .await
            .is_err()
        {
            return SessionProtocolOutcome::Continue;
        }
        if matches!(resolved.pending.request, ServerRequest::Ping) {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        let needs_follow_up = if self.stage_queued_publish_streams().await {
            true
        } else {
            resolved.queued_renegotiation
        };
        if needs_follow_up {
            match self.request_renegotiation(writer).await {
                Ok(_sent) => {}
                Err(code) => return SessionProtocolOutcome::Close(code),
            }
        }
        SessionProtocolOutcome::Continue
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingNegotiationRequest,
    ) -> Result<(), ()> {
        match &pending.action {
            PendingNegotiationAction::EstablishSession {
                client_rtp_capabilities,
            } => {
                if !self
                    .channel
                    .apply_session_negotiated(
                        &self.session_id,
                        self.connection_id,
                        client_rtp_capabilities.clone(),
                        &self.transport_adapter,
                    )
                    .await
                {
                    return Err(());
                }
            }
            PendingNegotiationAction::RefreshSession => {
                self.channel
                    .bootstrap_missing_consumers(&self.session_id, &self.transport_adapter)
                    .await;
            }
        }
        Ok(())
    }
}
