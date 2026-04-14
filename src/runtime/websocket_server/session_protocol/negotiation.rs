use o_sfu_router::MediaCapabilities;

use crate::signaling::protocol::{RequestId, ServerRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PendingNegotiationAction {
    EstablishSession {
        fallback_client_rtp_capabilities: MediaCapabilities,
    },
    RefreshSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingNegotiationRequest {
    pub(super) request_id: RequestId,
    pub(super) request: ServerRequest,
    pub(super) action: PendingNegotiationAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NegotiationPhase {
    BeforeInitialOffer,
    AwaitingAnswer {
        pending: PendingNegotiationRequest,
        queued_renegotiation: bool,
    },
    Stable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RenegotiationDisposition {
    Skip,
    QueueOnly,
    SendNow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedNegotiation {
    pub(super) pending: PendingNegotiationRequest,
    pub(super) queued_renegotiation: bool,
}

#[derive(Debug)]
pub(super) struct NegotiationState {
    phase: NegotiationPhase,
}

impl Default for NegotiationState {
    fn default() -> Self {
        Self {
            phase: NegotiationPhase::BeforeInitialOffer,
        }
    }
}

impl NegotiationState {
    pub(super) const fn awaiting_answer(&self) -> bool {
        matches!(self.phase, NegotiationPhase::AwaitingAnswer { .. })
    }

    pub(super) fn issue(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
        action: PendingNegotiationAction,
    ) {
        self.phase = NegotiationPhase::AwaitingAnswer {
            pending: PendingNegotiationRequest {
                request_id,
                request,
                action,
            },
            queued_renegotiation: false,
        };
    }

    pub(super) fn request_renegotiation(&mut self) -> RenegotiationDisposition {
        match &mut self.phase {
            NegotiationPhase::BeforeInitialOffer => RenegotiationDisposition::Skip,
            NegotiationPhase::Stable => RenegotiationDisposition::SendNow,
            NegotiationPhase::AwaitingAnswer {
                queued_renegotiation,
                ..
            } => {
                *queued_renegotiation = true;
                RenegotiationDisposition::QueueOnly
            }
        }
    }

    pub(super) fn resolve_answer(
        &mut self,
        response_to: &RequestId,
    ) -> Option<ResolvedNegotiation> {
        let NegotiationPhase::AwaitingAnswer {
            pending,
            queued_renegotiation,
        } = self.phase.clone()
        else {
            return None;
        };
        if pending.request_id != *response_to {
            return None;
        }
        self.phase = NegotiationPhase::Stable;
        Some(ResolvedNegotiation {
            pending,
            queued_renegotiation,
        })
    }
}
