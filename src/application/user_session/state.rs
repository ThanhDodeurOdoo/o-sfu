//! connection-scoped state for one websocket session
//!
//! this module only composes state from neighboring user-session workflows.
//! negotiation sequencing lives beside negotiation workflow code and
//! compatibility track snapshots live beside room-event projection

use o_sfu_protocol::wire::RequestId;

use super::{
    negotiation::{UserNegotiationState, UserRequestIdSequencer},
    room_events::UserWireState,
};

/// connection-scoped state composed by the [`super::User`] facade
///
/// [`UserState::negotiation_state`] belongs to negotiation workflow code and
/// [`UserState::wire_state`] belongs to room-event workflow code.
/// [`super::User`] still keeps them together so one websocket connection has
/// one request sequence and one browser-facing track snapshot
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
