//! tracks in-flight request/response RPCs plus their timeout timers
//!
//! the protocol core uses this for request-shaped operations where a client
//! request must resolve exactly once by response or by timeout
//!
//! the invariant is:
//!
//! - every live request has exactly one timeout timer
//! - every live timeout timer points back to exactly one request
//! - resolving by response or by timeout tears down both sides of that pair
//!
//! the tracker keeps two maps because response handling wants
//! `request_id -> timer`, while timeout handling wants `timer -> request_id`

use std::collections::BTreeMap;

use super::{Command, Commands, PendingRequestKind, REQUEST_TIMEOUT_TIMER_ID_BASE};
use crate::signaling::RequestId;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRequestState {
    kind: PendingRequestKind,
    timeout_timer_id: u32,
}

/// pending request registration returned to the caller
///
/// callers keep this only long enough to build the host registration and timer
/// commands for the matching outbound request
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestRegistration {
    pub(super) request_id: RequestId,
    pub(super) kind: PendingRequestKind,
    pub(super) timeout_timer_id: u32,
}

/// small state machine for request-shaped protocol operations
///
/// this owns request ids, timeout ids and the resolve-once rule shared by the
/// higher-level protocol core
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestTracker {
    next_request_counter: u64,
    next_timeout_timer_id: u32,
    pending_requests: BTreeMap<RequestId, PendingRequestState>,
    request_timeouts: BTreeMap<u32, RequestId>,
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestTracker {
    pub(super) fn new() -> Self {
        Self {
            next_request_counter: 0,
            next_timeout_timer_id: REQUEST_TIMEOUT_TIMER_ID_BASE,
            pending_requests: BTreeMap::new(),
            request_timeouts: BTreeMap::new(),
        }
    }

    /// returns whether a request of this semantic kind is still in flight
    ///
    /// callers can use this as a guard for operations that should not be
    /// duplicated while an earlier request of the same kind is still pending
    pub(super) fn has_pending_kind(&self, kind: PendingRequestKind) -> bool {
        self.pending_requests
            .values()
            .any(|pending_request| pending_request.kind == kind)
    }

    /// registers a new pending request and allocates its timeout timer id
    ///
    /// callers use the returned ids together to send the request under
    /// `request_id` and schedule the timeout under `timeout_timer_id`
    pub(super) fn register_request(&mut self, kind: PendingRequestKind) -> RequestRegistration {
        let request_id = self.next_request_id();
        let timeout_timer_id = self.next_timeout_timer_id();
        self.pending_requests.insert(
            request_id.clone(),
            PendingRequestState {
                kind,
                timeout_timer_id,
            },
        );
        self.request_timeouts
            .insert(timeout_timer_id, request_id.clone());
        RequestRegistration {
            request_id,
            kind,
            timeout_timer_id,
        }
    }

    /// resolves a server response if it matches a live pending request
    ///
    /// a response only counts when:
    ///
    /// - the `response_to` id is still pending
    /// - the pending request kind matches `expected_kind`
    ///
    /// anything else is treated as stale or mismatched input and becomes a
    /// no-op so crossed responses cannot resolve the wrong request
    ///
    /// success always cancels the matching timeout before resolving the request
    pub(super) fn resolve_response(
        &mut self,
        response_to: &RequestId,
        expected_kind: PendingRequestKind,
        ok: bool,
    ) -> Commands {
        let Some(pending_request) = self.pending_requests.remove(response_to) else {
            return Vec::new();
        };
        if pending_request.kind != expected_kind {
            self.pending_requests
                .insert(response_to.clone(), pending_request);
            return Vec::new();
        }
        self.request_timeouts
            .remove(&pending_request.timeout_timer_id);
        vec![
            Command::CancelTimer {
                id: pending_request.timeout_timer_id,
            },
            Command::ResolvePendingRequest {
                request_id: response_to.clone(),
                ok,
            },
        ]
    }

    /// resolves a pending request through its timeout timer
    ///
    /// unknown timer ids return `None`, which lets the caller distinguish
    /// "not one of ours" from "this timer belonged to us and produced no new
    /// commands (it can happen if the timer path wins a race after the
    /// request entry was already removed elsewhere)
    pub(super) fn resolve_timeout(&mut self, timer_id: u32) -> Option<Commands> {
        let request_id = self.request_timeouts.remove(&timer_id)?;
        let commands = if self.pending_requests.remove(&request_id).is_some() {
            vec![
                Command::CancelTimer { id: timer_id },
                Command::ResolvePendingRequest {
                    request_id,
                    ok: false,
                },
            ]
        } else {
            Vec::new()
        };
        Some(commands)
    }

    /// drops all pending tracker state without emitting host commands
    ///
    /// this is the clean reset path for callers that already know the outer
    /// user is being torn down and do not need per-request cancellation
    /// commands anymore
    pub(super) fn clear(&mut self) {
        self.pending_requests.clear();
        self.request_timeouts.clear();
    }

    /// clears the tracker and emits one failure resolution per live request
    ///
    /// each pending request gets exactly one timer cancellation and one failed
    /// resolution so the host side cannot leak promises or wait on stale timers
    pub(super) fn clear_with_commands(&mut self) -> Commands {
        let negotiation_request_ids: Vec<RequestId> =
            self.pending_requests.keys().cloned().collect();
        let mut commands = Vec::new();
        for request_id in negotiation_request_ids {
            let Some(pending_request) = self.pending_requests.remove(&request_id) else {
                continue;
            };
            self.request_timeouts
                .remove(&pending_request.timeout_timer_id);
            commands.push(Command::CancelTimer {
                id: pending_request.timeout_timer_id,
            });
            commands.push(Command::ResolvePendingRequest {
                request_id,
                ok: false,
            });
        }
        commands
    }

    fn next_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(self.next_request_counter.to_string());
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }

    fn next_timeout_timer_id(&mut self) -> u32 {
        let timer_id = self.next_timeout_timer_id;
        self.next_timeout_timer_id = self
            .next_timeout_timer_id
            .saturating_add(1)
            .max(REQUEST_TIMEOUT_TIMER_ID_BASE);
        timer_id
    }
}
