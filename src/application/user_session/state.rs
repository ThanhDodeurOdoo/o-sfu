//! signaling and compatibility state for one websocket connection
//!
//! this module owns the pure state machines used to sequence server-authored
//! requests and track the browser-facing track snapshot, it has no knowledge of
//! transport resources or media core transactions, which keeps the transition
//! logic deterministic and easy to test

mod negotiation;
mod wire;

use negotiation::UserRequestIdSequencer;
pub(super) use negotiation::{
    PendingUserAction, PendingUserRequest, RenegotiationDisposition, ResolvedUserNegotiation,
    UserNegotiationState,
};
use o_sfu_protocol::signaling::RequestId;
pub(super) use wire::{UserWireMessages, UserWireState};

/// connection-scoped state composed by the `User` orchestration facade
///
/// `negotiation_state` owns the offer sequencing invariant and `wire_state`
/// owns the browser-facing track snapshot while `User` still decides which state
/// transition is legal for a client or room event
#[derive(Debug, Default)]
pub(super) struct UserState {
    request_id_sequencer: UserRequestIdSequencer,
    pub negotiation_state: UserNegotiationState,
    pub wire_state: UserWireState,
}

impl UserState {
    pub fn next_request_id(&mut self) -> RequestId {
        self.request_id_sequencer.next()
    }
}
