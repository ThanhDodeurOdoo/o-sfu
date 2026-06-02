//! keyframe request tracking for RTC producer sources
//!
//! duplicate feedback is absorbed while one request is pending
//! a retry is emitted only when another request arrived before the deadline

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, Instant},
};

use str0m::media::{KeyframeRequestKind, Rid};

use crate::engine::media_transport::TransportMediaId;

pub(super) const KEYFRAME_REQUEST_RETRY_DELAY: Duration = Duration::from_secs(1);
const KEYFRAME_RETRY_DRAIN_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct SourceKeyframeRequest {
    pub(in crate::engine::media_transport::rtc) source_transport_media_id: TransportMediaId,
    pub(in crate::engine::media_transport::rtc) rid: Option<Rid>,
    pub(in crate::engine::media_transport::rtc) kind: KeyframeRequestKind,
}

impl SourceKeyframeRequest {
    fn targets(self, source_transport_media_id: TransportMediaId, rid: Option<Rid>) -> bool {
        self.source_transport_media_id == source_transport_media_id && self.rid == rid
    }
}

#[derive(Debug, Default)]
pub(in crate::engine::media_transport::rtc) struct KeyframeRequestTracker {
    pending: Vec<KeyframeRequestState>,
    deadlines: BinaryHeap<Reverse<KeyframeRequestDeadline>>,
    next_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct KeyframeRequestDeadline {
    deadline: Instant,
    id: u64,
}

pub(in crate::engine::media_transport::rtc) fn coalesce_keyframe_kind(
    current: KeyframeRequestKind,
    incoming: KeyframeRequestKind,
) -> KeyframeRequestKind {
    match (current, incoming) {
        (KeyframeRequestKind::Fir, _) | (_, KeyframeRequestKind::Fir) => KeyframeRequestKind::Fir,
        _ => current,
    }
}

impl KeyframeRequestTracker {
    pub fn track(
        &mut self,
        source_transport_media_id: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: Instant,
    ) -> KeyframeRequestDecision {
        let Some(request) = self
            .pending
            .iter_mut()
            .find(|request| request.request.targets(source_transport_media_id, rid))
        else {
            let request = SourceKeyframeRequest {
                source_transport_media_id,
                rid,
                kind,
            };
            let pending = KeyframeRequestState {
                request,
                deadline: now + KEYFRAME_REQUEST_RETRY_DELAY,
                id: self.next_id,
                retry_on_timeout: false,
            };
            self.next_id = self.next_id.saturating_add(1);
            self.deadlines.push(Reverse(pending.deadline()));
            self.pending.push(pending);
            return KeyframeRequestDecision::Forward;
        };
        request.request.kind = coalesce_keyframe_kind(request.request.kind, kind);
        request.retry_on_timeout = true;
        KeyframeRequestDecision::Absorb
    }

    pub fn forget(&mut self, source_transport_media_id: TransportMediaId, rid: Option<Rid>) {
        if let Some(index) = self
            .pending
            .iter()
            .position(|request| request.request.targets(source_transport_media_id, rid))
        {
            self.pending.swap_remove(index);
        }
    }

    pub fn forget_source(&mut self, source_transport_media_id: TransportMediaId) {
        self.pending.retain(|request| {
            request.request.source_transport_media_id != source_transport_media_id
        });
    }

    pub fn observe_refresh(
        &mut self,
        source_transport_media_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> usize {
        let before = self.pending.len();
        self.pending.retain(|request| {
            request.request.source_transport_media_id != source_transport_media_id
                || (request.request.rid.is_some() && request.request.rid != rid)
        });
        before - self.pending.len()
    }

    pub fn drain_due(&mut self, now: Instant, retries: &mut Vec<SourceKeyframeRequest>) {
        let mut remaining = KEYFRAME_RETRY_DRAIN_LIMIT;
        while matches!(
            self.deadlines.peek(),
            Some(Reverse(deadline)) if deadline.deadline <= now
        ) && remaining > 0
        {
            remaining -= 1;
            let Some(Reverse(deadline)) = self.deadlines.pop() else {
                break;
            };
            let Some(index) = self
                .pending
                .iter()
                .position(|request| request.matches_deadline(deadline))
            else {
                continue;
            };
            let Some(request) = self.pending.get_mut(index) else {
                continue;
            };
            if !request.retry_on_timeout {
                self.pending.swap_remove(index);
                continue;
            }
            request.retry_on_timeout = false;
            request.deadline = now + KEYFRAME_REQUEST_RETRY_DELAY;
            request.id = self.next_id;
            self.next_id = self.next_id.saturating_add(1);
            self.deadlines.push(Reverse(request.deadline()));
            retries.push(request.request);
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines
            .peek()
            .map(|Reverse(deadline)| deadline.deadline)
    }
}

#[derive(Debug, Clone, Copy)]
struct KeyframeRequestState {
    request: SourceKeyframeRequest,
    deadline: Instant,
    id: u64,
    retry_on_timeout: bool,
}

impl KeyframeRequestState {
    fn deadline(self) -> KeyframeRequestDeadline {
        KeyframeRequestDeadline {
            deadline: self.deadline,
            id: self.id,
        }
    }

    fn matches_deadline(self, deadline: KeyframeRequestDeadline) -> bool {
        self.deadline == deadline.deadline && self.id == deadline.id
    }
}
