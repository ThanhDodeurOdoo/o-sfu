use o_sfu_protocol::wire::{RequestId, ServerRequest, SessionDescriptionPayload};
use tracing::{info, instrument, warn};

use super::{
    User, UserError, UserOutput, UserSignal,
    compat::map_core_negotiation_error,
    projection::session_description_payload,
    state::{PendingUserAction, RenegotiationDisposition, ResolvedUserNegotiation},
};
use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    core::prelude::{
        InitialOffer, NegotiationOffer, SessionNegotiationOutcome, SfuCoreError, UserStreamId,
    },
    runtime::telemetry::schema::event as telemetry_event,
};

impl User {
    /// Apply a browser answer for the pending offer or renegotiation request.
    ///
    /// The response id must match the current pending server request. A valid
    /// answer is applied through core, then any streams committed by the answer
    /// update presence state. Queued publishes or queued renegotiation requests
    /// may produce a follow-up offer in the returned output.
    ///
    /// # Errors
    ///
    /// Returns [`UserError::ProtocolViolation`] for empty SDP, unknown request
    /// ids and core rejections caused by unusable answers or stale callbacks.
    /// Returns [`UserError::InternalError`] for transport failures.
    pub async fn complete_negotiation(
        &mut self,
        response_to: RequestId,
        answer: SessionDescriptionPayload,
    ) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        self.validate_negotiation_answer(&response_to, &answer)?;
        let Some(ResolvedUserNegotiation {
            action,
            queued_renegotiation,
        }) = self.state.negotiation_state.resolve_answer(&response_to)
        else {
            warn!(
                user_id = ?&self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received negotiation answer for an unknown or stale request"
            );
            return Err(UserError::ProtocolViolation);
        };
        let request_operation = action.operation_name();
        let committed_stream_ids = self
            .apply_negotiation_action(action, &answer.sdp, &response_to, request_operation)
            .await?;
        for stream_id in committed_stream_ids {
            if let Some(stream_type) = stream_type_for_stream_id(&stream_id) {
                self.update_publication_info(stream_type, true).await?;
            }
        }
        info!(
            event = telemetry_event::NEGOTIATION_SUCCEEDED,
            operation = request_operation,
            outcome = "answer_applied",
            ?response_to,
            "applied negotiation answer"
        );
        let needs_follow_up = self.stage_queued_publish_slots().await? || queued_renegotiation;
        if needs_follow_up {
            return self.renegotiate().await;
        }
        Ok(UserOutput::new())
    }

    #[instrument(
        name = "transport.offer.create",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.id,
            connection_id = ?self.connection_id
        )
    )]
    pub(super) async fn create_initial_offer(&mut self) -> Result<UserOutput, UserError> {
        let negotiation = self.media().negotiation();
        let initial_offer = negotiation.create_initial_offer().await.map_err(|error| {
            warn!(
                event = telemetry_event::NEGOTIATION_FAILED,
                operation = "initial_offer_create",
                outcome = "transport_error",
                user_id = ?&self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?error,
                "failed to create initial transport offer"
            );
            UserError::InternalError
        })?;
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = "initial_offer_create",
            outcome = "offer_ready",
            "created initial transport offer"
        );
        Ok(self.issue_negotiation_request(NegotiationRequestDraft::initial(initial_offer)))
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
    pub(super) async fn renegotiate(&mut self) -> Result<UserOutput, UserError> {
        match self.state.negotiation_state.schedule_renegotiation() {
            RenegotiationDisposition::Skip | RenegotiationDisposition::QueueOnly => {
                Ok(UserOutput::new())
            }
            RenegotiationDisposition::SendNow => {
                let Some(offer) = self.create_renegotiation_offer().await? else {
                    return Ok(UserOutput::new());
                };
                Ok(self.issue_negotiation_request(NegotiationRequestDraft::refresh(offer)))
            }
        }
    }

    fn issue_negotiation_request(&mut self, draft: NegotiationRequestDraft) -> UserOutput {
        let NegotiationRequestDraft { request, action } = draft;
        let request_id = self.state.next_request_id();
        info!(
            event = telemetry_event::NEGOTIATION_STARTED,
            operation = action.operation_name(),
            outcome = "request_prepared",
            ?request_id,
            "prepared negotiation request"
        );
        let signal = UserSignal::request(request_id.clone(), request);
        self.state.negotiation_state.issue(request_id, action);
        UserOutput::new().with_signal(signal)
    }

    fn validate_negotiation_answer(
        &self,
        response_to: &RequestId,
        answer: &SessionDescriptionPayload,
    ) -> Result<(), UserError> {
        if answer.sdp.is_empty() {
            warn!(
                user_id = ?&self.id,
                connection_id = ?self.connection_id,
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received empty SDP answer for negotiation request"
            );
            return Err(UserError::ProtocolViolation);
        }
        Ok(())
    }

    async fn apply_negotiation_action(
        &self,
        action: PendingUserAction,
        answer_sdp: &str,
        response_to: &RequestId,
        request_operation: &'static str,
    ) -> Result<Vec<UserStreamId>, UserError> {
        let negotiation = self.media().negotiation();
        let (apply_operation, result) = match action {
            PendingUserAction::EstablishSession(initial_offer) => (
                "answer_apply",
                negotiation
                    .apply_initial_answer(answer_sdp, initial_offer)
                    .await,
            ),
            PendingUserAction::RefreshSession => (
                "renegotiation_apply",
                negotiation.apply_renegotiation_answer(answer_sdp).await,
            ),
        };
        match result {
            Ok(committed_stream_ids) => Ok(committed_stream_ids),
            Err(error) => {
                self.log_negotiation_apply_error(
                    response_to,
                    apply_operation,
                    request_operation,
                    error,
                );
                Err(map_core_negotiation_error(error))
            }
        }
    }

    async fn create_renegotiation_offer(&self) -> Result<Option<NegotiationOffer>, UserError> {
        self.media()
            .negotiation()
            .create_renegotiation_offer()
            .await
            .map_err(|error| {
                warn!(
                    event = telemetry_event::NEGOTIATION_FAILED,
                    operation = "renegotiation_offer_create",
                    outcome = "transport_error",
                    user_id = ?&self.id,
                    connection_id = ?self.connection_id,
                    remote_address = self.remote_address.as_ref(),
                    ?error,
                    "failed to build a staged renegotiation offer"
                );
                UserError::InternalError
            })
    }

    fn log_negotiation_apply_error(
        &self,
        response_to: &RequestId,
        apply_operation: &'static str,
        request_operation: &'static str,
        error: SfuCoreError,
    ) {
        let (outcome, message) = match error {
            SfuCoreError::Transport(_) => (
                "transport_error",
                "failed to apply negotiation answer to the transport endpoint",
            ),
            SfuCoreError::CapabilityProjection(_) => (
                "capability_projection_failed",
                "failed to project client RTP capabilities from the answered SDP",
            ),
            SfuCoreError::SessionNegotiationRejected(outcome) => (
                match outcome {
                    SessionNegotiationOutcome::Applied => "room_commit_unexpected",
                    SessionNegotiationOutcome::StaleConnection => "stale_connection",
                },
                "failed to commit negotiated user state after initial answer",
            ),
            SfuCoreError::SessionRefreshRejected(outcome) => (
                match outcome {
                    SessionNegotiationOutcome::Applied => "room_refresh_unexpected",
                    SessionNegotiationOutcome::StaleConnection => "stale_connection",
                },
                "failed to refresh user state after renegotiation answer",
            ),
        };
        warn!(
            event = telemetry_event::NEGOTIATION_FAILED,
            operation = apply_operation,
            request_operation,
            outcome,
            user_id = ?&self.id,
            connection_id = ?self.connection_id,
            remote_address = self.remote_address.as_ref(),
            ?response_to,
            ?error,
            "{message}"
        );
    }
}

struct NegotiationRequestDraft {
    request: ServerRequest,
    action: PendingUserAction,
}

impl NegotiationRequestDraft {
    fn initial(initial_offer: InitialOffer) -> Self {
        let request =
            ServerRequest::Offer(session_description_payload(initial_offer.offer().clone()));
        Self {
            request,
            action: PendingUserAction::EstablishSession(initial_offer),
        }
    }

    fn refresh(offer: NegotiationOffer) -> Self {
        Self {
            request: ServerRequest::Renegotiate(session_description_payload(offer)),
            action: PendingUserAction::RefreshSession,
        }
    }
}
