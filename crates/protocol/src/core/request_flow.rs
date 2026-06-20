use super::{
    CommandBatch, Commands, FlushMode, NegotiationKind, NegotiationRejection, PendingRequestKind,
    ProtocolCore,
};
use crate::signaling::{
    ClientEnvelope, ClientRequest, ClientResponse, RecordingOptions, RequestId, ServerRequest,
    ServerResponse, SessionDescriptionPayload,
};

pub(super) fn start_recording(core: &mut ProtocolCore, options: RecordingOptions) -> Commands {
    begin_request(
        core,
        ClientRequest::StartRecording(options),
        PendingRequestKind::StartRecording,
    )
}

pub(super) fn stop_recording(core: &mut ProtocolCore) -> Commands {
    begin_request(
        core,
        ClientRequest::StopRecording,
        PendingRequestKind::StopRecording,
    )
}

pub(super) fn submit_negotiation_answer(
    core: &mut ProtocolCore,
    request_id: &RequestId,
    kind: NegotiationKind,
    sdp: impl Into<String>,
) -> Commands {
    if !core.can_send_client_messages() || !core.phase.resolve_negotiation(request_id, kind) {
        return Vec::new();
    }
    let sdp = sdp.into();
    let response = match kind {
        NegotiationKind::Offer => ClientResponse::Offer(SessionDescriptionPayload {
            sdp,
            upload_slots: Vec::new(),
        }),
        NegotiationKind::Renegotiate => ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp,
            upload_slots: Vec::new(),
        }),
    };
    let Some(envelope) = ClientEnvelope::Response {
        response_to: request_id.clone(),
        response,
    }
    .into_envelope()
    .ok() else {
        return Vec::new();
    };
    core.enqueue_envelope(envelope, FlushMode::Immediate)
}

pub(super) fn handle_server_request(
    core: &mut ProtocolCore,
    request_id: RequestId,
    request: ServerRequest,
) -> Commands {
    match request {
        ServerRequest::Offer(payload) => {
            handle_negotiation_request(core, request_id, NegotiationKind::Offer, payload)
        }
        ServerRequest::Renegotiate(payload) => {
            handle_negotiation_request(core, request_id, NegotiationKind::Renegotiate, payload)
        }
    }
}

pub(super) fn handle_server_response(
    core: &mut ProtocolCore,
    response_to: &RequestId,
    response: ServerResponse,
) -> Commands {
    match response {
        ServerResponse::StartRecording(payload) => resolve_request(
            core,
            response_to,
            PendingRequestKind::StartRecording,
            payload.ok,
        ),
        ServerResponse::StopRecording(payload) => resolve_request(
            core,
            response_to,
            PendingRequestKind::StopRecording,
            payload.ok,
        ),
    }
}

fn handle_negotiation_request(
    core: &mut ProtocolCore,
    request_id: RequestId,
    kind: NegotiationKind,
    payload: SessionDescriptionPayload,
) -> Commands {
    match core.phase.accept_negotiation(&request_id, kind) {
        Ok(()) => {}
        Err(NegotiationRejection::Ignored) => return Vec::new(),
        Err(NegotiationRejection::ProtocolError) => {
            return CommandBatch::close_for_protocol_error().into_vec();
        }
    }
    match kind {
        NegotiationKind::Offer => CommandBatch::initial_offer(request_id, payload),
        NegotiationKind::Renegotiate => CommandBatch::renegotiation(request_id, payload),
    }
    .into_vec()
}

fn begin_request(
    core: &mut ProtocolCore,
    request: ClientRequest,
    kind: PendingRequestKind,
) -> Commands {
    if !core.can_send_client_messages() || core.request_tracker.has_pending_kind(kind) {
        return Vec::new();
    }
    let request_start = core.request_tracker.begin_request(kind);
    let Some(envelope) = ClientEnvelope::Request {
        request_id: request_start.request_id.clone(),
        request,
    }
    .into_envelope()
    .ok() else {
        return Vec::new();
    };

    CommandBatch::begin_pending_request(
        request_start,
        core.enqueue_envelope(envelope, FlushMode::Batched),
    )
    .into_vec()
}

fn resolve_request(
    core: &mut ProtocolCore,
    response_to: &RequestId,
    expected_kind: PendingRequestKind,
    ok: bool,
) -> Commands {
    core.request_tracker
        .resolve_response(response_to, expected_kind, ok)
}
