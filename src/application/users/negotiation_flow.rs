use o_sfu_protocol::signaling::{
    NegotiationUploadEncoding, NegotiationUploadSlot, RequestId, ServerRequest,
    SessionDescriptionPayload, WebSocketCloseCode,
};
use tracing::{info, instrument, warn};

use super::{
    User,
    flow_state::{
        PendingFlowAction, PendingFlowRequest, RenegotiationDisposition, ResolvedFlowState,
    },
};
use crate::{
    application::outcomes::{CallOutcome, UserSignal},
    core::{MediaNegotiationOffer, MediaUploadEncoding, MediaUploadSlot, SfuCoreError},
    runtime::telemetry::schema::event as telemetry_event,
};

impl User {
    #[instrument(
        name = "transport.offer.create",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id
        )
    )]
    pub(super) async fn send_initial_offer(&mut self) -> Result<CallOutcome, WebSocketCloseCode> {
        let (offer, offered_capabilities) = self
            .media_core
            .create_initial_offer(&self.id, self.connection_id)
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "initial_offer_create",
                    outcome = "transport_error",
                    user_id = ?self.id,
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
        let offer_request = ServerRequest::Offer(session_description_payload(offer));
        Ok(
            CallOutcome::new().with_signal(self.issue_negotiation_request(
                offer_request,
                PendingFlowAction::EstablishSession {
                    offered_capabilities,
                },
            )),
        )
    }

    fn issue_negotiation_request(
        &mut self,
        request: ServerRequest,
        action: PendingFlowAction,
    ) -> UserSignal {
        let request_id = self.request_ids.next_request_id();
        let signal = UserSignal::request(request_id.clone(), request.clone());
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = negotiation_operation_name(&action),
            outcome = "request_prepared",
            ?request_id,
            "prepared negotiation request"
        );
        self.flow_state.issue(request_id, request, action);
        signal
    }

    #[instrument(
        name = "transport.renegotiate",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id
        )
    )]
    pub(super) async fn request_renegotiation(
        &mut self,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        match self.flow_state.request_renegotiation() {
            RenegotiationDisposition::Skip | RenegotiationDisposition::QueueOnly => {
                Ok(CallOutcome::new())
            }
            RenegotiationDisposition::SendNow => {
                let Some(offer) = self.create_renegotiation_offer().await? else {
                    return Ok(CallOutcome::new());
                };
                let request = ServerRequest::Renegotiate(session_description_payload(offer));
                Ok(CallOutcome::new().with_signal(
                    self.issue_negotiation_request(request, PendingFlowAction::RefreshSession),
                ))
            }
        }
    }

    pub(super) async fn handle_negotiation_response(
        &mut self,
        response_to: RequestId,
        answer: SessionDescriptionPayload,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        self.validate_negotiation_answer(&response_to, &answer)?;
        let Some(resolved) = self.resolve_negotiation_answer(&response_to) else {
            return Err(WebSocketCloseCode::ProtocolError);
        };
        self.apply_negotiation_action(&resolved.pending, &answer.sdp, &response_to)
            .await?;
        info!(
            event = telemetry_event::NEGOTIATION_SUCCEEDED,
            operation = negotiation_operation_name(&resolved.pending.action),
            outcome = "answer_applied",
            ?response_to,
            "applied negotiation answer"
        );
        Self::record_staged_publishes_committed();
        self.send_follow_up_renegotiation_if_needed(&resolved).await
    }

    fn validate_negotiation_answer(
        &self,
        response_to: &RequestId,
        answer: &SessionDescriptionPayload,
    ) -> Result<(), WebSocketCloseCode> {
        if answer.sdp.is_empty() {
            warn!(
                user_id = ?self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received empty SDP answer for negotiation request"
            );
            return Err(WebSocketCloseCode::ProtocolError);
        }
        Ok(())
    }

    fn resolve_negotiation_answer(&mut self, response_to: &RequestId) -> Option<ResolvedFlowState> {
        let Some(resolved) = self.flow_state.resolve_answer(response_to) else {
            warn!(
                user_id = ?self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received negotiation answer for an unknown or stale request"
            );
            return None;
        };
        Some(resolved)
    }

    async fn send_follow_up_renegotiation_if_needed(
        &mut self,
        resolved: &ResolvedFlowState,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        let needs_follow_up =
            self.stage_queued_publish_streams().await || resolved.queued_renegotiation;
        if !needs_follow_up {
            return Ok(CallOutcome::new());
        }
        self.request_renegotiation().await
    }

    async fn apply_negotiation_action(
        &self,
        pending: &PendingFlowRequest,
        answer_sdp: &str,
        response_to: &RequestId,
    ) -> Result<(), WebSocketCloseCode> {
        let result = match &pending.action {
            PendingFlowAction::EstablishSession {
                offered_capabilities,
            } => {
                self.media_core
                    .apply_initial_answer(
                        &self.id,
                        self.connection_id,
                        answer_sdp,
                        offered_capabilities,
                    )
                    .await
            }
            PendingFlowAction::RefreshSession => {
                self.media_core
                    .apply_renegotiation_answer(&self.id, self.connection_id, answer_sdp)
                    .await
            }
        };
        if let Err(error) = result {
            self.log_negotiation_apply_error(response_to, pending, error);
            return Err(map_core_negotiation_error(error));
        }
        Ok(())
    }

    async fn create_renegotiation_offer(
        &self,
    ) -> Result<Option<MediaNegotiationOffer>, WebSocketCloseCode> {
        self.media_core
            .create_renegotiation_offer(&self.id, self.connection_id)
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "renegotiation_offer_create",
                    outcome = "transport_error",
                    user_id = ?self.id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    ?error,
                    "failed to build a staged renegotiation offer"
                );
                WebSocketCloseCode::Error
            })
    }

    fn log_negotiation_apply_error(
        &self,
        response_to: &RequestId,
        pending: &PendingFlowRequest,
        error: SfuCoreError,
    ) {
        let (operation, outcome, message) = match error {
            SfuCoreError::Transport(_) => (
                "answer_apply",
                "transport_error",
                "failed to apply negotiation answer to the transport user",
            ),
            SfuCoreError::CapabilityProjection(_) => (
                "answer_apply",
                "capability_projection_failed",
                "failed to project client RTP capabilities from the answered SDP",
            ),
            SfuCoreError::UserStateCommitRejected => (
                "answer_apply",
                "room_commit_failed",
                "failed to commit negotiated user state after initial answer",
            ),
            SfuCoreError::UserStateRefreshRejected => (
                "renegotiation_apply",
                "room_refresh_failed",
                "failed to refresh user state after renegotiation answer",
            ),
        };
        warn!(
            event = telemetry_event::NEGOTIATION_FAILED,
            operation,
            outcome,
            user_id = ?self.id,
            connection_id = ?self.connection_id,
            remote_address = self.remote_address.as_ref(),
            ?response_to,
            request = ?pending.request,
            ?error,
            "{message}"
        );
    }
}

fn session_description_payload(offer: MediaNegotiationOffer) -> SessionDescriptionPayload {
    SessionDescriptionPayload {
        sdp: offer.sdp,
        upload_slots: offer
            .upload_slots
            .into_iter()
            .map(protocol_upload_slot)
            .collect(),
    }
}

fn protocol_upload_slot(slot: MediaUploadSlot) -> NegotiationUploadSlot {
    NegotiationUploadSlot {
        mid: slot.mid,
        kind: slot.kind,
        codecs: slot.codecs,
        simulcast_encodings: slot
            .simulcast_encodings
            .into_iter()
            .map(protocol_upload_encoding)
            .collect(),
    }
}

fn protocol_upload_encoding(encoding: MediaUploadEncoding) -> NegotiationUploadEncoding {
    NegotiationUploadEncoding {
        rid: encoding.rid,
        max_bitrate: encoding.max_bitrate,
    }
}

fn negotiation_operation_name(action: &PendingFlowAction) -> &'static str {
    match action {
        PendingFlowAction::EstablishSession { .. } => "initial_offer_create",
        PendingFlowAction::RefreshSession => "renegotiation_offer_create",
    }
}

fn map_core_negotiation_error(error: SfuCoreError) -> WebSocketCloseCode {
    match error {
        SfuCoreError::Transport(_) => WebSocketCloseCode::Error,
        SfuCoreError::CapabilityProjection(_)
        | SfuCoreError::UserStateCommitRejected
        | SfuCoreError::UserStateRefreshRejected => WebSocketCloseCode::ProtocolError,
    }
}
