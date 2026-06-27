use std::collections::BTreeMap;

use o_sfu_protocol::{
    host::NegotiationKind,
    wire::{
        ClientResponse, DownloadStates, NegotiationUploadEncoding, NegotiationUploadSlot,
        RequestId, ServerEnvelope, ServerRequest, SessionDescriptionPayload, StreamType, UserId,
    },
};
use tracing::{Span, field, instrument, warn};

use super::{User, UserError, UserOutput};
use crate::{
    application::stream_catalog::{DiscussStream, source_publish_intent_for_stream_type},
    core::prelude::{
        Bitrate, NegotiationOffer, SessionError, SfuCoreError, UploadEncoding, UploadSlot,
    },
    runtime::telemetry::schema::event as telemetry_event,
};

impl User {
    pub(super) async fn complete_negotiation(
        &mut self,
        response_to: RequestId,
        response: ClientResponse,
    ) -> Result<UserOutput, UserError> {
        let (response_kind, answer) = match response {
            ClientResponse::Offer(answer) => (NegotiationKind::Offer, answer),
            ClientResponse::Renegotiate(answer) => (NegotiationKind::Renegotiate, answer),
        };
        self.validate_negotiation_answer(&response_to, &answer)?;
        if !self.negotiation.expects_answer(&response_to, response_kind) {
            self.log_unknown_answer(&response_to);
            return Err(UserError::ProtocolViolation);
        }
        let offer = self
            .session
            .answer(&answer.sdp)
            .await
            .map_err(|e| self.negotiation_error(response_kind, Some(&response_to), e))?;
        self.negotiation.clear_pending();
        Ok(self.issue_offer(NegotiationKind::Renegotiate, offer))
    }

    #[instrument(
        name = "transport.renegotiate",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id()
        )
    )]
    pub(super) async fn renegotiate(&mut self) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        let offer = self
            .session
            .renegotiate()
            .await
            .map_err(|e| self.negotiation_error(NegotiationKind::Renegotiate, None, e))?;
        Ok(self.issue_offer(NegotiationKind::Renegotiate, offer))
    }

    #[instrument(
        name = "publish.intent",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            ?stream_type,
            active
        )
    )]
    pub(super) async fn publish(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> Result<UserOutput, UserError> {
        let result = if active {
            let intent = source_publish_intent_for_stream_type(stream_type);
            self.session.publish(intent).await
        } else {
            let intent = DiscussStream::for_type(stream_type).unpublish_intent();
            self.session.unpublish(intent).await
        };
        let offer = result.map_err(|error| self.publish_error(stream_type, error))?;
        Ok(self.issue_offer(NegotiationKind::Renegotiate, offer))
    }

    #[instrument(
        name = "subscribe.intent",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            target_session_id = field::Empty,
            source_count = field::Empty
        )
    )]
    pub(super) async fn subscribe(
        &self,
        target_user_id: UserId,
        states: DownloadStates,
    ) -> Result<UserOutput, UserError> {
        let target_user_id = target_user_id.normalized_for_runtime();
        let span = Span::current();
        span.record("target_session_id", field::debug(&target_user_id));
        let source_intents = DiscussStream::all()
            .filter_map(|stream| stream.subscription_intent_if_requested(&states))
            .collect::<BTreeMap<_, _>>();
        span.record("source_count", source_intents.len());
        self.session
            .subscribe(&target_user_id, &source_intents)
            .await
            .map_err(|error| self.subscribe_error(&target_user_id, error))?;
        Ok(UserOutput::new())
    }

    #[instrument(
        name = "transport.offer.create",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id()
        )
    )]
    pub(super) async fn run_initial_offer(&mut self) -> Result<UserOutput, UserError> {
        let offer = self
            .session
            .establish()
            .await
            .map_err(|e| self.negotiation_error(NegotiationKind::Offer, None, e))?;
        Ok(self.issue_offer(NegotiationKind::Offer, offer))
    }

    fn issue_offer(
        &mut self,
        kind: NegotiationKind,
        offer: Option<NegotiationOffer>,
    ) -> UserOutput {
        offer.map_or_else(UserOutput::new, |offer| {
            vec![self.negotiation.issue(kind, offer)]
        })
    }
}

#[derive(Default)]
pub(super) struct ServerNegotiation {
    next: u64,
    pending: Option<PendingServerOffer>,
}

struct PendingServerOffer {
    request_id: RequestId,
    kind: NegotiationKind,
}

impl ServerNegotiation {
    fn issue(&mut self, kind: NegotiationKind, offer: NegotiationOffer) -> ServerEnvelope {
        let req_id = RequestId::new(format!("server-{}", self.next));
        self.next = self.next.saturating_add(1);
        let payload = session_description_payload(offer);
        let request = match kind {
            NegotiationKind::Offer => ServerRequest::Offer(payload),
            NegotiationKind::Renegotiate => ServerRequest::Renegotiate(payload),
        };
        let pending = PendingServerOffer {
            request_id: req_id.clone(),
            kind,
        };
        self.pending = Some(pending);
        ServerEnvelope::Request {
            request_id: req_id,
            request,
        }
    }

    fn expects_answer(&self, id: &RequestId, kind: NegotiationKind) -> bool {
        matches!(self.pending.as_ref(), Some(slot) if slot.request_id == *id && slot.kind == kind)
    }

    pub(super) fn clear_pending(&mut self) {
        self.pending = None;
    }
}

fn session_description_payload(offer: NegotiationOffer) -> SessionDescriptionPayload {
    SessionDescriptionPayload {
        sdp: offer.sdp,
        upload_slots: offer
            .upload_slots
            .into_iter()
            .map(protocol_upload_slot)
            .collect(),
    }
}

fn protocol_upload_slot(slot: UploadSlot) -> NegotiationUploadSlot {
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

fn protocol_upload_encoding(encoding: UploadEncoding) -> NegotiationUploadEncoding {
    NegotiationUploadEncoding {
        rid: encoding.rid,
        max_bitrate: encoding.max_bitrate.map(Bitrate::as_bps),
        resolution_scale: encoding.resolution_scale,
        max_framerate: encoding.max_framerate,
    }
}

impl User {
    fn negotiation_error(
        &self,
        kind: NegotiationKind,
        response_to: Option<&RequestId>,
        error: SessionError,
    ) -> UserError {
        let operation = match kind {
            NegotiationKind::Offer => "initial_offer_create",
            NegotiationKind::Renegotiate => "renegotiation_offer_create",
        };
        let outcome = match error {
            SessionError::NoPendingRequest => "no_pending_media_request",
            SessionError::Core(error) if error.is_client_error() => "client_negotiation_error",
            SessionError::Core(_) => "transport_error",
        };
        warn!(
            event = telemetry_event::NEGOTIATION_FAILED,
            operation,
            outcome,
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            response_to = ?response_to,
            ?error,
            "media session command failed"
        );
        user_error(error)
    }

    fn publish_error(&self, stream_type: StreamType, error: SessionError) -> UserError {
        warn!(
            event = telemetry_event::PUBLISH_ABORTED,
            operation = "publish_intent",
            outcome = "publish_rejected",
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            ?stream_type,
            ?error,
            "media session command failed"
        );
        user_error(error)
    }

    fn subscribe_error(&self, target_user_id: &UserId, error: SessionError) -> UserError {
        let outcome = match error {
            SessionError::Core(SfuCoreError::SubscriptionUpdateRejected) => "stale_connection",
            SessionError::NoPendingRequest | SessionError::Core(_) => "subscription_failed",
        };
        warn!(
            event = telemetry_event::SUBSCRIBE_REJECTED,
            operation = "consume_prepare",
            outcome,
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            ?target_user_id,
            ?error,
            "media session command failed"
        );
        user_error(error)
    }

    fn validate_negotiation_answer(
        &self,
        response_to: &RequestId,
        answer: &SessionDescriptionPayload,
    ) -> Result<(), UserError> {
        if answer.sdp.is_empty() {
            warn!(
                user_id = ?self.user_id(),
                connection_id = ?self.connection_id(),
                remote_address = self.remote_address.as_ref(),
                ?response_to,
                "received empty SDP answer for negotiation request"
            );
            return Err(UserError::ProtocolViolation);
        }
        Ok(())
    }

    fn log_unknown_answer(&self, response_to: &RequestId) {
        warn!(
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            ?response_to,
            "received negotiation answer for an unknown or stale request"
        );
    }
}

fn user_error(error: SessionError) -> UserError {
    if error.is_client_error() {
        UserError::ProtocolViolation
    } else {
        UserError::InternalError
    }
}
