//! Tracks in-flight request/response RCPs plus their timeout timers.
//!
//! The protocol core uses this for request-shaped operations like
//! `startRecording` and `stopRecording` where the client sends a request, waits
//! for either a matching response or a timeout, then resolves exactly once.
//!
//! The important invariant here is:
//!
//! - every live request has exactly one timeout timer
//! - every live timeout timer points back to exactly one request
//! - resolving by response or by timeout tears down both sides of that pair
//!
//! That pairing is why the tracker keep two maps instead of trying to derive
//! everything from one side on the fly. Response handling wants `request_id ->
//! timer`, timeout handling wants `timer -> request_id` and both paths need to
//! stay no-op safe for stale or mismatched events.
//!
//! Example:
//!
//! ```text
//! register_request(StartRecording)
//!   -> request_id = "0", timer_id = REQUEST_TIMEOUT_TIMER_ID_BASE
//!
//! resolve_response("0", StartRecording, true)
//!   -> cancel that timer
//!   -> resolve that request once
//! ```
//!
//! ```text
//! register_request(StopRecording)
//! resolve_timeout(timer_id)
//!   -> resolve that request as failed
//! ```

use std::collections::BTreeMap;

use crate::signaling::RequestId;

use super::{Command, Commands, PendingRequestKind, REQUEST_TIMEOUT_TIMER_ID_BASE};

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRequestState {
    kind: PendingRequestKind,
    timeout_timer_id: u32,
}

/// Request identity returned to the caller after registration.
///
/// The caller keeps these ids only long enough to wire follow-up commands like
/// `ScheduleTimer` and later match server responses against the right pending
/// request entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RegisteredRequest {
    pub(super) request_id: RequestId,
    pub(super) timeout_timer_id: u32,
}

/// Small state machine for request-shaped protocol operations.
///
/// It does not know anything about websocket IO or specific server payloads.
/// It only owns request ids, timeout ids and the "resolve once" rule shared by the higher-level protocol core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestTracker {
    next_request_counter: u64,
    next_request_timeout_timer_id: u32,
    pending_requests: BTreeMap<RequestId, PendingRequestState>,
    request_timeouts: BTreeMap<u32, RequestId>,
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestTracker {
    /// Creates an empty tracker with fresh request and timer counters.
    pub(super) fn new() -> Self {
        Self {
            next_request_counter: 0,
            next_request_timeout_timer_id: REQUEST_TIMEOUT_TIMER_ID_BASE,
            pending_requests: BTreeMap::new(),
            request_timeouts: BTreeMap::new(),
        }
    }

    /// Returns whether a request of this semantic kind is still in flight.
    ///
    /// Callers cam use this as a guard for operations that should not be
    /// duplicated while an earlier request of the same kind is still pending.
    pub(super) fn has_pending_kind(&self, kind: PendingRequestKind) -> bool {
        self.pending_requests
            .values()
            .any(|pending_request| pending_request.kind == kind)
    }

    /// Registers a new pending request and allocates its timeout timer id.
    ///
    /// Callers are expected to use the returned ids together: send the request
    /// under `request_id` and schedule the timeout under `timeout_timer_id`.
    pub(super) fn register_request(&mut self, kind: PendingRequestKind) -> RegisteredRequest {
        let request_id = self.next_request_id();
        let timeout_timer_id = self.next_request_timeout_timer_id();
        self.pending_requests.insert(
            request_id.clone(),
            PendingRequestState {
                kind,
                timeout_timer_id,
            },
        );
        self.request_timeouts
            .insert(timeout_timer_id, request_id.clone());
        RegisteredRequest {
            request_id,
            timeout_timer_id,
        }
    }

    /// Resolves a server response if it matches a live pending request.
    ///
    /// A response only counts if;:
    ///
    /// - the `response_to` id is still pending
    /// - the pending request kind matches `expected_kind`
    ///
    /// Anything else is treated as stale or mismatched input and becomes a
    /// no-op. That keeps crossed responses from resolving the wrong request if
    /// the caller reused the same response handler shape for multiple RPC kinds.
    ///
    /// On success this always cancels the matching timeout first, then resolves
    /// the request.
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

    /// Resolves a pending request through its timeout timer.
    ///
    /// Unknown timer ids return `None`, which lets the caller distinguish
    /// "not one of ours" from "this timer belonged to us and produced no new
    /// commands (it can happen if the timer path wins a race after the
    /// request entry was already removed elsewhere).
    pub(super) fn resolve_timeout(&mut self, timer_id: u32) -> Option<Commands> {
        let request_id = self.request_timeouts.remove(&timer_id)?;
        let commands = if self.pending_requests.remove(&request_id).is_some() {
            vec![Command::ResolvePendingRequest {
                request_id,
                ok: false,
            }]
        } else {
            Vec::new()
        };
        Some(commands)
    }

    /// Drops all pending tracker state without emitting host commands.
    ///
    /// This is the clean reset path for callers that already know the outer
    /// session is being torn down and do not need per-request cancellation
    /// commands anymore.
    pub(super) fn clear(&mut self) {
        self.pending_requests.clear();
        self.request_timeouts.clear();
    }

    /// Clears the tracker and emits one failure resolution per live request.
    ///
    /// This is the graceful shutdown path. Each pending request gets exactly
    /// one timer cancellation and one failed resolution, which keeps the host
    /// side from leaking promises or waiting on timers that no longer matter.
    pub(super) fn clear_with_commands(&mut self) -> Commands {
        let pending_request_ids: Vec<RequestId> = self.pending_requests.keys().cloned().collect();
        let mut commands = Vec::new();
        for request_id in pending_request_ids {
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

    fn next_request_timeout_timer_id(&mut self) -> u32 {
        let timer_id = self.next_request_timeout_timer_id;
        self.next_request_timeout_timer_id = self
            .next_request_timeout_timer_id
            .saturating_add(1)
            .max(REQUEST_TIMEOUT_TIMER_ID_BASE);
        timer_id
    }
}
