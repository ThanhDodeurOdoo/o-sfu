use super::{
    Command, Commands, FlushMode, NegotiationKind, PendingNegotiation, PendingRequestKind,
    ProtocolCore, REQUEST_TIMEOUT_MS, protocol_error_commands,
};
use crate::{
    bundle_api::BundleConnectionState,
    signaling::{
        ClientEnvelope, ClientRequest, ClientResponse, RecordingOptions, RequestId, ServerRequest,
        ServerResponse, SessionDescriptionPayload,
    },
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
    sdp: String,
) -> Commands {
    if !core.can_send_client_messages() {
        return Vec::new();
    }
    let Some(pending_negotiation) = core.pending_negotiation.as_ref() else {
        return Vec::new();
    };
    if pending_negotiation.request_id != *request_id || pending_negotiation.kind != kind {
        return Vec::new();
    }
    core.pending_negotiation = None;
    let response = match kind {
        NegotiationKind::Offer => ClientResponse::Offer(SessionDescriptionPayload { sdp }),
        NegotiationKind::Renegotiate => {
            ClientResponse::Renegotiate(SessionDescriptionPayload { sdp })
        }
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
    if !matches!(
        core.state,
        BundleConnectionState::Authenticated | BundleConnectionState::Connected
    ) {
        return Vec::new();
    }
    if core.pending_negotiation.is_some() {
        return protocol_error_commands();
    }
    let pending_request_id = request_id.clone();
    core.pending_negotiation = Some(PendingNegotiation { request_id, kind });
    let mut commands = Vec::new();
    if kind == NegotiationKind::Offer && core.state == BundleConnectionState::Authenticated {
        commands.push(Command::CreatePeerConnection);
    }
    commands.push(Command::ApplyNegotiation {
        request_id: pending_request_id,
        kind,
        sdp: payload.sdp,
    });
    commands
}

fn begin_request(
    core: &mut ProtocolCore,
    request: ClientRequest,
    kind: PendingRequestKind,
) -> Commands {
    if !core.can_send_client_messages() || core.request_tracker.has_pending_kind(kind) {
        return Vec::new();
    }
    let registered_request = core.request_tracker.register_request(kind);
    let Some(envelope) = ClientEnvelope::Request {
        request_id: registered_request.request_id.clone(),
        request,
    }
    .into_envelope()
    .ok() else {
        return Vec::new();
    };

    let mut commands = vec![
        Command::RegisterPendingRequest {
            request_id: registered_request.request_id,
            kind,
        },
        Command::ScheduleTimer {
            id: registered_request.timeout_timer_id,
            ms: REQUEST_TIMEOUT_MS,
        },
    ];
    commands.extend(core.enqueue_envelope(envelope, FlushMode::Batched));
    commands
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
