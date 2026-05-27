use std::{
    collections::BTreeSet,
    mem::{replace, take},
};

use o_sfu_protocol::wire::{RequestId, ServerRequest, SessionDescriptionPayload, StreamType};
use tracing::{info, instrument, warn};

use super::{User, UserError, UserOutput, UserSignal, projection::session_description_payload};
use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    core::prelude::{
        InitialOffer, NegotiationOffer, SessionNegotiationOutcome, SfuCoreError, UserStreamId,
    },
    runtime::telemetry::schema::event as telemetry_event,
};

/// generator for monotonic server-authored request ids
#[derive(Debug, Default)]
pub(in crate::application::user_session) struct UserRequestIdSequencer {
    next_request_counter: u64,
}

impl UserRequestIdSequencer {
    pub(in crate::application::user_session) fn next(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }
}

/// reason a server-authored negotiation request exists
///
/// the pending action tells `User` which media-core answer path is legal when
/// the browser resolves the request
#[derive(Debug)]
pub(in crate::application::user_session) enum PendingUserAction {
    /// the request expects to establish the first transport session
    EstablishSession(InitialOffer),
    /// the request only refreshes an existing transport session
    RefreshSession,
}

impl PendingUserAction {
    pub(in crate::application::user_session) const fn operation_name(&self) -> &'static str {
        match self {
            Self::EstablishSession(_) => "initial_offer_create",
            Self::RefreshSession => "renegotiation_offer_create",
        }
    }
}

/// command returned to the orchestrator after a renegotiation request
///
/// this keeps the state decision pure so `User` performs media work only when
/// the state machine says a request can be issued immediately
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application::user_session) enum RenegotiationDisposition {
    /// no negotiation work is needed
    Skip,
    /// a request is already pending, the intent was queued for a follow-up
    QueueOnly,
    /// the session is stable and ready to issue a new offer immediately
    SendNow,
}

/// resolved answer metadata returned after a request id match
///
/// the queued renegotiation flag is carried beside the pending request so
/// `User` can apply the answer first, then decide whether follow-up media work
/// needs another offer
#[derive(Debug)]
pub(in crate::application::user_session) struct ResolvedUserNegotiation {
    pub action: PendingUserAction,
    pub queued_renegotiation: bool,
}

/// negotiation state machine for one browser session
///
/// the machine ensures that only one server offer is pending at a time, it
/// handles queuing for publish intents and room events that arrive while an
/// answer is outstanding
#[derive(Debug)]
pub(in crate::application::user_session) struct UserNegotiationState {
    phase: NegotiationPhase,
    queued_publish_slots: BTreeSet<StreamType>,
}

#[derive(Debug)]
enum NegotiationPhase {
    BeforeInitialOffer,
    Stable,
    Negotiating {
        request_id: RequestId,
        action: PendingUserAction,
        queued_renegotiation: bool,
    },
}

impl Default for UserNegotiationState {
    fn default() -> Self {
        Self {
            phase: NegotiationPhase::BeforeInitialOffer,
            queued_publish_slots: BTreeSet::default(),
        }
    }
}

impl UserNegotiationState {
    pub const fn awaiting_answer(&self) -> bool {
        matches!(self.phase, NegotiationPhase::Negotiating { .. })
    }

    pub fn has_queued_publish(&self, stream_type: StreamType) -> bool {
        self.queued_publish_slots.contains(&stream_type)
    }

    pub fn queue_publish_slot(&mut self, stream_type: StreamType) {
        self.queued_publish_slots.insert(stream_type);
    }

    pub fn clear_queued_publish(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_slots.remove(&stream_type)
    }

    pub fn take_queued_publish_slots(&mut self) -> Vec<StreamType> {
        take(&mut self.queued_publish_slots).into_iter().collect()
    }

    /// record a newly issued server request in the state machine
    ///
    /// this moves the session to the negotiating phase and preserves any existing
    /// publish queue
    pub fn issue(&mut self, request_id: RequestId, action: PendingUserAction) {
        self.phase = NegotiationPhase::Negotiating {
            request_id,
            action,
            queued_renegotiation: false,
        };
    }

    /// assess whether a new renegotiation offer can be sent right now
    ///
    /// returns [`RenegotiationDisposition::SendNow`] if the state is stable,
    /// otherwise it flags the machine to trigger a follow-up after the current
    /// answer arrives
    pub fn schedule_renegotiation(&mut self) -> RenegotiationDisposition {
        match &mut self.phase {
            NegotiationPhase::BeforeInitialOffer => RenegotiationDisposition::Skip,
            NegotiationPhase::Stable => RenegotiationDisposition::SendNow,
            NegotiationPhase::Negotiating {
                queued_renegotiation,
                ..
            } => {
                *queued_renegotiation = true;
                RenegotiationDisposition::QueueOnly
            }
        }
    }

    /// resolve a browser answer and return to stable state
    ///
    /// returns the pending request metadata if the id matches, otherwise it
    /// returns `None` and preserves the current state
    pub fn resolve_answer(&mut self, response_to: &RequestId) -> Option<ResolvedUserNegotiation> {
        let NegotiationPhase::Negotiating { request_id, .. } = &self.phase else {
            return None;
        };
        if *request_id != *response_to {
            return None;
        }
        match replace(&mut self.phase, NegotiationPhase::Stable) {
            NegotiationPhase::Negotiating {
                action,
                queued_renegotiation,
                ..
            } => Some(ResolvedUserNegotiation {
                action,
                queued_renegotiation,
            }),
            other => {
                self.phase = other;
                None
            }
        }
    }
}

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
            .apply_negotiation_answer(action, &answer.sdp, &response_to, request_operation)
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

    async fn apply_negotiation_answer(
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
                let user_error = if error.is_client_negotiation_error() {
                    UserError::ProtocolViolation
                } else {
                    UserError::InternalError
                };
                self.log_negotiation_apply_error(
                    response_to,
                    apply_operation,
                    request_operation,
                    error,
                );
                Err(user_error)
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

#[cfg(test)]
mod tests {
    use o_sfu_protocol::wire::{RequestId, StreamType};

    use super::*;

    #[test]
    fn queued_publish_slots_are_unique() {
        let mut state = UserNegotiationState::default();

        state.queue_publish_slot(StreamType::Camera);
        state.queue_publish_slot(StreamType::Camera);

        assert_eq!(state.take_queued_publish_slots(), vec![StreamType::Camera]);
    }

    #[test]
    fn resolving_answer_keeps_queued_publish_slots_for_follow_up_staging() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = UserNegotiationState::default();
        state.queue_publish_slot(StreamType::Camera);
        state.issue(request_id.clone(), PendingUserAction::RefreshSession);

        let resolved = state.resolve_answer(&request_id);

        assert!(resolved.is_some());
        assert!(matches!(
            state.schedule_renegotiation(),
            RenegotiationDisposition::SendNow
        ));
        assert_eq!(state.take_queued_publish_slots(), vec![StreamType::Camera]);
    }

    #[test]
    fn stale_answers_keep_the_current_pending_request() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = UserNegotiationState::default();
        state.issue(request_id, PendingUserAction::RefreshSession);

        assert!(
            state
                .resolve_answer(&RequestId::new(String::from("server-2")))
                .is_none()
        );
        assert!(state.awaiting_answer());
        assert!(matches!(
            state.schedule_renegotiation(),
            RenegotiationDisposition::QueueOnly
        ));
    }
}
