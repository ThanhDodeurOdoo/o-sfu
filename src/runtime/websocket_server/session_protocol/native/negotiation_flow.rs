use crate::runtime::transport_adapter::{
    RuntimeTransportAdapter, TransportAdapterError, TransportSessionKey,
};
use crate::runtime::websocket_server::WsWriter;
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
                offered_router_rtp_capabilities: router_capabilities,
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
                let Some(request) =
                    staged_renegotiation_request(&self.transport_adapter, &session_key).await?
                else {
                    return Ok(false);
                };
                self.issue_negotiation_request(
                    writer,
                    request,
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
        if self
            .apply_negotiation_action(&resolved.pending, &answer.sdp)
            .await
            .is_err()
        {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        self.commit_staged_publishes().await;
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
        answer_sdp: &str,
    ) -> Result<(), ()> {
        match &pending.action {
            PendingNegotiationAction::EstablishSession {
                offered_router_rtp_capabilities,
            } => {
                let client_rtp_capabilities = self
                    .transport_adapter
                    .negotiated_client_rtp_capabilities(answer_sdp, offered_router_rtp_capabilities)
                    .map_err(|_error| ())?;
                if !self
                    .channel
                    .apply_session_negotiated(
                        &self.session_id,
                        self.connection_id,
                        client_rtp_capabilities,
                        &self.transport_adapter,
                    )
                    .await
                {
                    return Err(());
                }
            }
            PendingNegotiationAction::RefreshSession => {
                if !self
                    .channel
                    .apply_session_refreshed(
                        &self.session_id,
                        self.connection_id,
                        &self.transport_adapter,
                    )
                    .await
                {
                    return Err(());
                }
            }
        }
        Ok(())
    }
}

async fn staged_renegotiation_request(
    transport_adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
) -> Result<Option<ServerRequest>, WebSocketCloseCode> {
    match transport_adapter
        .create_session_renegotiation_offer(session_key)
        .await
    {
        Ok(offer) => Ok(Some(ServerRequest::Renegotiate(
            SessionDescriptionPayload {
                sdp: offer.into_sdp(),
            },
        ))),
        Err(TransportAdapterError::UnsupportedFeature) => Ok(None),
        Err(TransportAdapterError::TransportUnavailable | TransportAdapterError::InvalidInput) => {
            Err(WebSocketCloseCode::Error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Instant,
    };

    use str0m::{Candidate, Rtc, change::SdpOffer};

    use super::staged_renegotiation_request;
    use crate::{
        config::{MediaCodecFlags, RtcPortRange},
        runtime::{
            recording::MediaTap,
            transport_adapter::{
                RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter, TransportSessionKey,
            },
        },
        signaling::{protocol::WebSocketCloseCode, shared::SessionId},
    };

    fn build_real_rtc_transport_adapter(port_min: u16) -> RuntimeTransportAdapter {
        RuntimeTransportAdapter::builder()
            .rtc(RtcTransportAdapterShardSetConfig::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                RtcPortRange::new(port_min, port_min.saturating_add(99)),
                1,
                MediaCodecFlags::default(),
                Arc::new(MediaTap::default()),
            ))
            .build()
    }

    fn answer_offer(offer_sdp: &str, port: u16) -> Option<String> {
        let mut rtc = Rtc::new(Instant::now());
        rtc.add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp").ok()?,
        )?;
        let answer = rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
            .ok()?;
        Some(answer.to_sdp_string())
    }

    #[tokio::test]
    async fn staged_renegotiation_request_returns_none_without_pending_offer() {
        let transport_adapter = build_real_rtc_transport_adapter(58_100);
        let session_key = TransportSessionKey::new(7, 0, 11, SessionId::Integer(19));
        let initial_offer_result = transport_adapter
            .create_initial_session_offer(&session_key)
            .await;
        assert!(
            initial_offer_result.is_ok(),
            "expected initial rtc offer, got {initial_offer_result:?}"
        );
        let Ok(initial_offer) = initial_offer_result else {
            return;
        };
        let initial_offer_sdp = initial_offer.into_sdp();
        let answer_sdp = answer_offer(&initial_offer_sdp, 58_300);
        assert!(
            answer_sdp.is_some(),
            "expected answerer to accept the initial rtc offer"
        );
        let Some(answer_sdp) = answer_sdp else {
            return;
        };
        assert_eq!(
            transport_adapter
                .apply_session_answer(&session_key, &answer_sdp)
                .await,
            Ok(())
        );

        let renegotiation_request =
            staged_renegotiation_request(&transport_adapter, &session_key).await;

        assert_eq!(renegotiation_request, Ok(None));
    }

    #[tokio::test]
    async fn staged_renegotiation_request_keeps_transport_errors_fatal() {
        let transport_adapter = build_real_rtc_transport_adapter(58_400);
        let missing_session_key = TransportSessionKey::new(8, 0, 12, SessionId::Integer(20));

        let renegotiation_request =
            staged_renegotiation_request(&transport_adapter, &missing_session_key).await;

        assert_eq!(renegotiation_request, Err(WebSocketCloseCode::Error));
    }
}
