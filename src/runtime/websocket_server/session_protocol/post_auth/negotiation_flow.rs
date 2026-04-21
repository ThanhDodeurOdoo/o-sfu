use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::transport_adapter::{
    NegotiationPort, SessionOffer, TransportAdapterError, TransportSessionKey,
};
use crate::runtime::websocket_server::WsWriter;
use o_sfu_protocol::signaling::{
    RequestId, ServerRequest, SessionDescriptionPayload, WebSocketCloseCode,
};
use tracing::{info, instrument, warn};

use super::super::{
    controller::SessionProtocolOutcome,
    flow_state::{
        PendingFlowAction, PendingFlowRequest, RenegotiationDisposition, ResolvedFlowState,
    },
    frame_codec::send_server_request,
};
use super::controller::PostAuthSessionProtocol;

impl PostAuthSessionProtocol {
    #[instrument(
        name = "transport.offer.create",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id
        )
    )]
    pub(in crate::runtime::websocket_server) async fn send_initial_offer(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<(), WebSocketCloseCode> {
        let router_capabilities = self.channel.router_rtp_capabilities().await;
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        let offer = create_initial_offer(&self.transport_adapter, &session_key)
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "initial_offer_create",
                    outcome = "transport_error",
                    session_id = ?self.session_id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    ?error,
                    "failed to create initial transport offer"
                );
                WebSocketCloseCode::Error
            })?;
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = "initial_offer_create",
            outcome = "offer_ready",
            "created initial transport offer"
        );
        let offer_request = ServerRequest::Offer(SessionDescriptionPayload {
            sdp: offer.into_sdp(),
        });
        self.issue_negotiation_request(
            writer,
            offer_request,
            PendingFlowAction::EstablishSession {
                offered_router_rtp_capabilities: router_capabilities,
            },
        )
        .await
    }

    async fn issue_negotiation_request(
        &mut self,
        writer: &mut WsWriter,
        request: ServerRequest,
        action: PendingFlowAction,
    ) -> Result<(), WebSocketCloseCode> {
        let request_id = self.request_ids.next_request_id();
        if let Err(code) = send_server_request(writer, request_id.clone(), request.clone()).await {
            warn!(
                event = telemetry_event::NEGOTIATION_FAILED,
                operation = negotiation_operation_name(&action),
                outcome = "request_send_failed",
                session_id = ?self.session_id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?request_id,
                close_code = u16::from(code),
                "failed to send negotiation request over websocket"
            );
            return Err(code);
        }
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = negotiation_operation_name(&action),
            outcome = "request_sent",
            ?request_id,
            "sent negotiation request"
        );
        self.flow_state.issue(request_id, request, action);
        Ok(())
    }

    #[instrument(
        name = "transport.renegotiate",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id
        )
    )]
    pub(super) async fn request_renegotiation(
        &mut self,
        writer: &mut WsWriter,
    ) -> Result<bool, WebSocketCloseCode> {
        match self.flow_state.request_renegotiation() {
            RenegotiationDisposition::Skip | RenegotiationDisposition::QueueOnly => Ok(false),
            RenegotiationDisposition::SendNow => {
                let session_key = self
                    .channel
                    .transport_session_key(&self.session_id, self.connection_id);
                let Some(request) = staged_renegotiation_request(
                    &self.transport_adapter,
                    &session_key,
                    self.remote_address.as_ref(),
                )
                .await?
                else {
                    return Ok(false);
                };
                self.issue_negotiation_request(writer, request, PendingFlowAction::RefreshSession)
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
        if let Err(outcome) = self.validate_negotiation_answer(&response_to, &answer) {
            return outcome;
        }
        let Some(resolved) = self.resolve_negotiation_answer(&response_to) else {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        };
        if let Err(outcome) = self
            .apply_transport_negotiation_answer(&response_to, &answer.sdp, &resolved)
            .await
        {
            return outcome;
        }
        if self
            .apply_negotiation_action(&resolved.pending, &answer.sdp)
            .await
            .is_err()
        {
            return SessionProtocolOutcome::Close(WebSocketCloseCode::ProtocolError);
        }
        info!(
            event = telemetry_event::NEGOTIATION_SUCCEEDED,
            operation = negotiation_operation_name(&resolved.pending.action),
            outcome = "answer_applied",
            ?response_to,
            "applied negotiation answer"
        );
        self.commit_staged_publishes().await;
        if let Err(outcome) = self
            .send_follow_up_renegotiation_if_needed(writer, &resolved)
            .await
        {
            return outcome;
        }
        SessionProtocolOutcome::Continue
    }

    fn validate_negotiation_answer(
        &self,
        response_to: &RequestId,
        answer: &SessionDescriptionPayload,
    ) -> Result<(), SessionProtocolOutcome> {
        if answer.sdp.is_empty() {
            warn!(
                session_id = ?self.session_id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received empty SDP answer for negotiation request"
            );
            return Err(SessionProtocolOutcome::Close(
                WebSocketCloseCode::ProtocolError,
            ));
        }
        Ok(())
    }

    fn resolve_negotiation_answer(&mut self, response_to: &RequestId) -> Option<ResolvedFlowState> {
        let Some(resolved) = self.flow_state.resolve_answer(response_to) else {
            warn!(
                session_id = ?self.session_id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received negotiation answer for an unknown or stale request"
            );
            return None;
        };
        Some(resolved)
    }

    #[instrument(
        name = "transport.answer.apply",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id
        )
    )]
    async fn apply_transport_negotiation_answer(
        &self,
        response_to: &RequestId,
        answer_sdp: &str,
        resolved: &ResolvedFlowState,
    ) -> Result<(), SessionProtocolOutcome> {
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        if let Err(error) =
            apply_transport_answer(&self.transport_adapter, &session_key, answer_sdp).await
        {
            warn!(
                event = telemetry_event::NEGOTIATION_FAILED,
                operation = "answer_apply",
                outcome = "transport_error",
                session_id = ?self.session_id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                request = ?resolved.pending.request,
                ?error,
                "failed to apply negotiation answer to the transport session"
            );
            return Err(SessionProtocolOutcome::Close(WebSocketCloseCode::Error));
        }
        Ok(())
    }

    async fn send_follow_up_renegotiation_if_needed(
        &mut self,
        writer: &mut WsWriter,
        resolved: &ResolvedFlowState,
    ) -> Result<(), SessionProtocolOutcome> {
        let needs_follow_up =
            self.stage_queued_publish_streams().await || resolved.queued_renegotiation;
        if !needs_follow_up {
            return Ok(());
        }
        self.request_renegotiation(writer)
            .await
            .map(|_sent| ())
            .map_err(SessionProtocolOutcome::Close)
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingFlowRequest,
        answer_sdp: &str,
    ) -> Result<(), ()> {
        match &pending.action {
            PendingFlowAction::EstablishSession {
                offered_router_rtp_capabilities,
            } => {
                let client_rtp_capabilities = self
                    .project_client_rtp_capabilities(answer_sdp, offered_router_rtp_capabilities)
                    .map_err(|error| {
                        warn!(
                            event = telemetry_event::NEGOTIATION_FAILED,
                            operation = "answer_apply",
                            outcome = "capability_projection_failed",
                            session_id = ?self.session_id,
                            connection_id = ?self.connection_id,
                            remote_address = self.remote_address.as_ref(),
                            ?error,
                            "failed to project client RTP capabilities from the answered SDP"
                        );
                    })?;
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
                    warn!(
                        event = telemetry_event::NEGOTIATION_FAILED,
                        operation = "answer_apply",
                        outcome = "channel_commit_failed",
                        session_id = ?self.session_id,
                        connection_id = ?self.connection_id,
                        remote_address = self.remote_address.as_ref(),
                        "failed to commit negotiated session state after initial answer"
                    );
                    return Err(());
                }
            }
            PendingFlowAction::RefreshSession => {
                if !self
                    .channel
                    .apply_session_refreshed(
                        &self.session_id,
                        self.connection_id,
                        &self.transport_adapter,
                    )
                    .await
                {
                    warn!(
                        event = telemetry_event::NEGOTIATION_FAILED,
                        operation = "renegotiation_apply",
                        outcome = "channel_refresh_failed",
                        session_id = ?self.session_id,
                        connection_id = ?self.connection_id,
                        remote_address = self.remote_address.as_ref(),
                        "failed to refresh session state after renegotiation answer"
                    );
                    return Err(());
                }
            }
        }
        Ok(())
    }
}

impl PostAuthSessionProtocol {
    fn project_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_rtp_capabilities: &o_sfu_router::MediaCapabilities,
    ) -> Result<o_sfu_router::MediaCapabilities, TransportAdapterError> {
        project_client_rtp_capabilities(
            &self.transport_adapter,
            answer_sdp,
            offered_router_rtp_capabilities,
        )
    }
}

async fn create_initial_offer(
    negotiation_port: &impl NegotiationPort,
    session_key: &TransportSessionKey,
) -> Result<SessionOffer, TransportAdapterError> {
    negotiation_port
        .create_initial_session_offer(session_key)
        .await
}

async fn apply_transport_answer(
    negotiation_port: &impl NegotiationPort,
    session_key: &TransportSessionKey,
    answer_sdp: &str,
) -> Result<(), TransportAdapterError> {
    negotiation_port
        .apply_session_answer(session_key, answer_sdp)
        .await
}

fn project_client_rtp_capabilities(
    negotiation_port: &impl NegotiationPort,
    answer_sdp: &str,
    offered_router_rtp_capabilities: &o_sfu_router::MediaCapabilities,
) -> Result<o_sfu_router::MediaCapabilities, TransportAdapterError> {
    negotiation_port.negotiated_client_rtp_capabilities(answer_sdp, offered_router_rtp_capabilities)
}

async fn staged_renegotiation_request(
    transport_port: &impl NegotiationPort,
    session_key: &TransportSessionKey,
    remote_address: &str,
) -> Result<Option<ServerRequest>, WebSocketCloseCode> {
    match transport_port
        .create_session_renegotiation_offer(session_key)
        .await
    {
        Ok(offer) => Ok(Some(ServerRequest::Renegotiate(
            SessionDescriptionPayload {
                sdp: offer.into_sdp(),
            },
        ))),
        Err(TransportAdapterError::UnsupportedFeature) => Ok(None),
        Err(
            error @ (TransportAdapterError::TransportUnavailable
            | TransportAdapterError::InvalidInput),
        ) => {
            warn!(
                ?session_key,
                event = telemetry_event::NEGOTIATION_FAILED,
                operation = "renegotiation_offer_create",
                outcome = "transport_error",
                remote_address,
                ?error,
                "failed to build a staged renegotiation offer"
            );
            Err(WebSocketCloseCode::Error)
        }
    }
}

fn negotiation_operation_name(action: &PendingFlowAction) -> &'static str {
    match action {
        PendingFlowAction::EstablishSession { .. } => "initial_offer_create",
        PendingFlowAction::RefreshSession => "renegotiation_offer_create",
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
            diagnostics::DiagnosticsStore,
            metrics::RuntimeMetrics,
            recording::MediaTap,
            rtc_adapter::test_support::test_transport_session_key,
            transport_adapter::{
                RtcTransportAdapterShardSetConfig, RuntimeTransportAdapter, SessionBitrateLimits,
            },
        },
    };
    use o_sfu_protocol::{shared::SessionId, signaling::WebSocketCloseCode};

    fn build_real_rtc_transport_adapter(port_min: u16) -> RuntimeTransportAdapter {
        RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            SessionBitrateLimits::new(8_000_000, 10_000_000),
            RtcPortRange::new(port_min, port_min.saturating_add(99)),
            1,
            MediaCodecFlags::default(),
            Arc::new(DiagnosticsStore::default()),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
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
        let session_key = test_transport_session_key(7, 0, 11, SessionId::Integer(19));
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
            staged_renegotiation_request(&transport_adapter, &session_key, "127.0.0.1").await;

        assert_eq!(renegotiation_request, Ok(None));
    }

    #[tokio::test]
    async fn staged_renegotiation_request_keeps_transport_errors_fatal() {
        let transport_adapter = build_real_rtc_transport_adapter(58_400);
        let missing_session_key = test_transport_session_key(8, 0, 12, SessionId::Integer(20));

        let renegotiation_request =
            staged_renegotiation_request(&transport_adapter, &missing_session_key, "127.0.0.1")
                .await;

        assert_eq!(renegotiation_request, Err(WebSocketCloseCode::Error));
    }
}
