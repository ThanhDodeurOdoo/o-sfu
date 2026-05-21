//! pure negotiation state for one browser session
//!
//! this module enforces the websocket-side offer ordering rule: only one
//! server-authored offer may be awaiting a browser answer, so publish requests
//! and generic renegotiation requests that arrive during that window become
//! follow-up work instead of overlapping protocol requests

use std::{
    collections::BTreeSet,
    mem::{replace, take},
};

use o_sfu_protocol::wire::{RequestId, StreamType};

use crate::core::prelude::InitialOffer;

/// generator for monotonic server-authored request ids
#[derive(Debug, Default)]
pub(super) struct UserRequestIdSequencer {
    next_request_counter: u64,
}

impl UserRequestIdSequencer {
    pub(super) fn next(&mut self) -> RequestId {
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
