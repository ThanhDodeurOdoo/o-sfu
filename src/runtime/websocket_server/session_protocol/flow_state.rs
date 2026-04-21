//! Session-scoped control state for the post-auth websocket protocol.
//!
//! This module exist to keep one websocket session's negotiation and queued
//! client-intent lifecycle at a single place
//!
//! The important invariant is that at most one `offer` or `renegotiate` request may be
//! awaiting an answer at a time, while publish intents that arrive during that
//! window must stay queued and deduplicated until the current answer lands.

use std::{collections::BTreeSet, mem::take};

use o_sfu_protocol::{
    shared::{DownloadStates, SessionId, StreamType},
    signaling::{RequestId, ServerRequest},
};
use o_sfu_router::MediaCapabilities;

/// Describes what the in-flight negotiation request is trying to achieve once
/// the matching answer is accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PendingFlowAction {
    EstablishSession {
        /// the server offer's router view must be kept so the answer can be
        /// projected back into client RTP capabilities without falling back to
        /// router defaults
        offered_router_rtp_capabilities: MediaCapabilities,
    },
    RefreshSession,
}

/// captures the single negotiation request that is currently allowed to be in
/// flight for a websocket session
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingFlowRequest {
    /// Only the matching response may advance the session back to `Stable`.
    pub(super) request_id: RequestId,
    pub(super) request: ServerRequest,
    pub(super) action: PendingFlowAction,
}

/// Represents one websocket-originated session change before it is translated
/// into channel or transport work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FlowChange {
    Publish(StreamType),
    Unpublish(StreamType),
    Subscribe {
        target_session_id: SessionId,
        states: DownloadStates,
    },
}

/// Tells the caller whether a topology-triggered refresh can be sent now or
/// must stay behind the already in-flight answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RenegotiationDisposition {
    Skip,
    QueueOnly,
    SendNow,
}

/// Result of accepting an answer for the current in-flight negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedFlowState {
    pub(super) pending: PendingFlowRequest,
    /// A later topology change arrived while this answer was still pending, so
    /// a follow-up renegotiation must be considered immediately after commit.
    pub(super) queued_renegotiation: bool,
}

/// The complete session-level negotiation and queued-publish state machine.
///
/// It answers the lifecycle questions:
///
/// - is an answer currently outstanding?
/// - which answer is legal to accept
/// - which publish intents must survive until that answer lands?
/// - should a follow-up renegotiation flush once the current answer commits?
#[derive(Debug)]
pub(super) enum SessionFlowState {
    BeforeInitialOffer {
        /// Publish intents may already arrive before the first server offer is
        /// answered, but they cannot stage transport changes yet.
        queued_publish_streams: BTreeSet<StreamType>,
    },
    Stable {
        /// The queue stays on the stable state so publish deduplication and
        /// follow-up staging can reuse one storage shape across all phases.
        queued_publish_streams: BTreeSet<StreamType>,
    },
    Negotiating {
        /// The current outstanding request owns the only answer that may be
        /// accepted without treating the response as stale.
        pending: PendingFlowRequest,
        /// Publish intents are deduplicated here while the current answer is in
        /// flight so the follow-up staging pass can replay each stream once.
        queued_publish_streams: BTreeSet<StreamType>,
        /// This keeps topology-triggered refresh demand visible without
        /// allowing overlapping SDP exchanges on the same websocket session.
        queued_renegotiation: bool,
    },
}

impl Default for SessionFlowState {
    fn default() -> Self {
        Self::BeforeInitialOffer {
            queued_publish_streams: BTreeSet::default(),
        }
    }
}

impl SessionFlowState {
    /// Returns whether the websocket session is currently blocked on an answer
    /// to an earlier server request.
    pub(super) const fn awaiting_answer(&self) -> bool {
        matches!(self, Self::Negotiating { .. })
    }

    /// Records a publish intent that must survive until the next staging pass.
    pub(super) fn has_queued_publish(&self, stream_type: StreamType) -> bool {
        self.queued_publish_streams().contains(&stream_type)
    }

    /// Queues a publish intent in a deduplicated form so repeated client
    /// publishes for the same stream do not create duplicate follow-up work.
    pub(super) fn queue_publish_stream(&mut self, stream_type: StreamType) {
        self.queued_publish_streams_mut().insert(stream_type);
    }

    /// Removes a queued publish when an explicit unpublish cancels it before
    /// the staged transport work is committed.
    pub(super) fn clear_queued_publish(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_streams_mut().remove(&stream_type)
    }

    /// Drains the queued publish set for the next staging pass after an answer
    /// returns the session to a stable state.
    pub(super) fn take_queued_publish_streams(&mut self) -> Vec<StreamType> {
        let queued_publish_streams = self.queued_publish_streams().iter().copied().collect();
        self.queued_publish_streams_mut().clear();
        queued_publish_streams
    }

    /// Moves the session into the single allowed in-flight negotiation state.
    ///
    /// Callers must use this only after the request has been sent
    /// successfully, because the stored `request_id` becomes the sole answer
    /// that `resolve_answer` will accept.
    pub(super) fn issue(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
        action: PendingFlowAction,
    ) {
        let queued_publish_streams = self.take_phase_queue();
        *self = Self::Negotiating {
            pending: PendingFlowRequest {
                request_id,
                request,
                action,
            },
            queued_publish_streams,
            queued_renegotiation: false,
        };
    }

    /// Applies the one-outstanding-negotiation rule for topology refreshes.
    ///
    /// If a request is already awaiting an answer, the refresh is remembered so
    /// the caller can flush it after the current answer commits instead of
    /// starting an overlapping SDP exchange.
    pub(super) fn request_renegotiation(&mut self) -> RenegotiationDisposition {
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

    /// Resolves the current in-flight request back to `Stable` when the answer
    /// matches the stored request id.
    ///
    /// Stale answers are intentionally ignored without mutating the state
    /// machine, because same-session replacement and delayed network delivery
    /// can still surface old responses after ownership has moved on.
    pub(super) fn resolve_answer(&mut self, response_to: &RequestId) -> Option<ResolvedFlowState> {
        let previous = take(self);
        match previous {
            Self::Negotiating {
                pending,
                queued_publish_streams,
                queued_renegotiation,
            } if pending.request_id == *response_to => {
                *self = Self::Stable {
                    queued_publish_streams,
                };
                Some(ResolvedFlowState {
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

    fn queued_publish_streams(&self) -> &BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_streams,
            }
            | Self::Stable {
                queued_publish_streams,
            }
            | Self::Negotiating {
                queued_publish_streams,
                ..
            } => queued_publish_streams,
        }
    }

    fn queued_publish_streams_mut(&mut self) -> &mut BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_streams,
            }
            | Self::Stable {
                queued_publish_streams,
            }
            | Self::Negotiating {
                queued_publish_streams,
                ..
            } => queued_publish_streams,
        }
    }

    fn take_phase_queue(&mut self) -> BTreeSet<StreamType> {
        match take(self) {
            Self::BeforeInitialOffer {
                queued_publish_streams,
            }
            | Self::Stable {
                queued_publish_streams,
            }
            | Self::Negotiating {
                queued_publish_streams,
                ..
            } => queued_publish_streams,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingFlowAction, RenegotiationDisposition, SessionFlowState};
    use o_sfu_protocol::{
        shared::StreamType,
        signaling::{RequestId, ServerRequest, SessionDescriptionPayload},
    };
    use o_sfu_router::MediaCapabilities;

    #[test]
    fn queued_publish_streams_are_unique() {
        let mut state = SessionFlowState::default();

        state.queue_publish_stream(StreamType::Camera);
        state.queue_publish_stream(StreamType::Camera);

        assert_eq!(
            state.take_queued_publish_streams(),
            vec![StreamType::Camera]
        );
    }

    #[test]
    fn clearing_a_queued_publish_only_affects_that_stream() {
        let mut state = SessionFlowState::default();

        state.queue_publish_stream(StreamType::Audio);
        state.queue_publish_stream(StreamType::Screen);

        assert!(state.clear_queued_publish(StreamType::Audio));
        assert!(!state.has_queued_publish(StreamType::Audio));
        assert!(state.has_queued_publish(StreamType::Screen));
    }

    #[test]
    fn resolving_answer_keeps_queued_publish_streams_for_follow_up_staging() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = SessionFlowState::default();
        state.queue_publish_stream(StreamType::Camera);
        state.issue(
            request_id.clone(),
            ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0"),
            }),
            PendingFlowAction::EstablishSession {
                offered_router_rtp_capabilities: MediaCapabilities::default(),
            },
        );

        let resolved = state.resolve_answer(&request_id);

        assert!(resolved.is_some());
        assert!(matches!(
            state.request_renegotiation(),
            RenegotiationDisposition::SendNow
        ));
        assert_eq!(
            state.take_queued_publish_streams(),
            vec![StreamType::Camera]
        );
    }

    #[test]
    fn stale_answers_keep_the_current_pending_request() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = SessionFlowState::default();
        state.issue(
            request_id,
            ServerRequest::Renegotiate(SessionDescriptionPayload {
                sdp: String::from("v=0"),
            }),
            PendingFlowAction::RefreshSession,
        );

        assert!(
            state
                .resolve_answer(&RequestId::new(String::from("server-2")))
                .is_none()
        );
        assert!(state.awaiting_answer());
        assert!(matches!(
            state.request_renegotiation(),
            RenegotiationDisposition::QueueOnly
        ));
    }
}
