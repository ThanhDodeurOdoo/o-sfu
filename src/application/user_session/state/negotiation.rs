//! pure negotiation state for one browser session
//!
//! this module enforces the websocket-side offer ordering rule: only one
//! server-authored offer may be awaiting a browser answer, so publish requests
//! and generic renegotiation requests that arrive during that window become
//! follow-up work instead of overlapping protocol requests

use std::{collections::BTreeSet, mem::take};

use o_sfu_protocol::{
    shared::StreamType,
    signaling::{RequestId, ServerRequest},
};

use crate::core::OfferedMediaCapabilities;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application::user_session) enum PendingUserAction {
    /// the request expects to establish the first transport session
    EstablishSession {
        /// capabilities offered by the server for initial codec negotiation
        offered_capabilities: OfferedMediaCapabilities,
    },
    /// the request only refreshes an existing transport session
    RefreshSession,
}

/// metadata for one unresolved server-authored request
///
/// this is stored until the browser answers
/// the request id guards stale answers, while the action preserves which core
/// operation is allowed to consume the answer SDP
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application::user_session) struct PendingUserRequest {
    /// unique id sent to the client
    pub request_id: RequestId,
    /// original request payload used for re-validation on answer
    pub request: ServerRequest,
    /// orchestration intent that triggered the request
    pub action: PendingUserAction,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application::user_session) struct ResolvedUserNegotiation {
    pub pending: PendingUserRequest,
    pub queued_renegotiation: bool,
}

/// negotiation state machine for one browser session
///
/// the machine ensures that only one server offer is pending at a time, it
/// handles queuing for publish intents and room events that arrive while an
/// answer is outstanding
#[derive(Debug)]
pub(in crate::application::user_session) enum UserNegotiationState {
    /// the connection is authenticated but the first offer is not yet issued
    BeforeInitialOffer {
        /// publish intents received before startup
        queued_publish_slots: BTreeSet<StreamType>,
    },
    /// the session is active and no server requests are pending
    Stable {
        /// follow-up publish intents waiting for the next stable window
        queued_publish_slots: BTreeSet<StreamType>,
    },
    /// an offer or renegotiation request has been sent to the browser
    Negotiating {
        /// metadata needed to apply the browser's answer
        pending: PendingUserRequest,
        /// follow-up publish intents received while this request is pending
        queued_publish_slots: BTreeSet<StreamType>,
        /// whether a generic renegotiation is needed after this answer
        queued_renegotiation: bool,
    },
}

impl Default for UserNegotiationState {
    fn default() -> Self {
        Self::BeforeInitialOffer {
            queued_publish_slots: BTreeSet::default(),
        }
    }
}

impl UserNegotiationState {
    pub const fn awaiting_answer(&self) -> bool {
        matches!(self, Self::Negotiating { .. })
    }

    pub fn has_queued_publish(&self, stream_type: StreamType) -> bool {
        self.queued_publish_slots().contains(&stream_type)
    }

    pub fn queue_publish_slot(&mut self, stream_type: StreamType) {
        self.queued_publish_slots_mut().insert(stream_type);
    }

    pub fn clear_queued_publish(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_slots_mut().remove(&stream_type)
    }

    pub fn take_queued_publish_slots(&mut self) -> Vec<StreamType> {
        take(self.queued_publish_slots_mut()).into_iter().collect()
    }

    /// record a newly issued server request in the state machine
    ///
    /// this moves the session to [`Self::Negotiating`] and preserves any existing
    /// publish queue
    pub fn issue(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
        action: PendingUserAction,
    ) {
        let queued_publish_slots = self.take_phase_queue();
        *self = Self::Negotiating {
            pending: PendingUserRequest {
                request_id,
                request,
                action,
            },
            queued_publish_slots,
            queued_renegotiation: false,
        };
    }

    /// assess whether a new renegotiation offer can be sent right now
    ///
    /// returns [`RenegotiationDisposition::SendNow`] if the state is stable,
    /// otherwise it flags the machine to trigger a follow-up after the current
    /// answer arrives
    pub fn schedule_renegotiation(&mut self) -> RenegotiationDisposition {
        match self {
            Self::BeforeInitialOffer { .. } => RenegotiationDisposition::Skip,
            Self::Stable { .. } => RenegotiationDisposition::SendNow,
            Self::Negotiating {
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
        let previous = take(self);
        match previous {
            Self::Negotiating {
                pending,
                queued_publish_slots,
                queued_renegotiation,
            } if pending.request_id == *response_to => {
                *self = Self::Stable {
                    queued_publish_slots,
                };
                Some(ResolvedUserNegotiation {
                    pending,
                    queued_renegotiation,
                })
            }
            other => {
                *self = other;
                None
            }
        }
    }

    fn queued_publish_slots(&self) -> &BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_slots,
            }
            | Self::Stable {
                queued_publish_slots,
            }
            | Self::Negotiating {
                queued_publish_slots,
                ..
            } => queued_publish_slots,
        }
    }

    fn queued_publish_slots_mut(&mut self) -> &mut BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_slots,
            }
            | Self::Stable {
                queued_publish_slots,
            }
            | Self::Negotiating {
                queued_publish_slots,
                ..
            } => queued_publish_slots,
        }
    }

    fn take_phase_queue(&mut self) -> BTreeSet<StreamType> {
        take(self.queued_publish_slots_mut())
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::{
        shared::StreamType,
        signaling::{RequestId, ServerRequest, SessionDescriptionPayload},
    };

    use super::*;
    use crate::core::OfferedMediaCapabilities;

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
        state.issue(
            request_id.clone(),
            ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0"),
                upload_slots: Vec::new(),
            }),
            PendingUserAction::EstablishSession {
                offered_capabilities: OfferedMediaCapabilities::default(),
            },
        );

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
        state.issue(
            request_id,
            ServerRequest::Renegotiate(SessionDescriptionPayload {
                sdp: String::from("v=0"),
                upload_slots: Vec::new(),
            }),
            PendingUserAction::RefreshSession,
        );

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
