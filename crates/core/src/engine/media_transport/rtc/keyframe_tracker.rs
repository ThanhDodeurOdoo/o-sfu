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
pub enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKeyframeRequest {
    pub src_media: TransportMediaId,
    pub rid: Option<Rid>,
    pub kind: KeyframeRequestKind,
}

impl SourceKeyframeRequest {
    fn targets(self, src_media: TransportMediaId, rid: Option<Rid>) -> bool {
        self.src_media == src_media && self.rid == rid
    }
}

#[derive(Debug, Default)]
pub struct KeyframeRequestTracker {
    pending: Vec<KeyframeRequestState>,
    deadlines: BinaryHeap<Reverse<KeyframeRequestDeadline>>,
    next_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct KeyframeRequestDeadline {
    deadline: Instant,
    id: u64,
}

pub fn coalesce_kf_kind(
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
        src_media: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: Instant,
    ) -> KeyframeRequestDecision {
        let Some(request) = self
            .pending
            .iter_mut()
            .find(|request| request.request.targets(src_media, rid))
        else {
            let request = SourceKeyframeRequest {
                src_media,
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
        request.request.kind = coalesce_kf_kind(request.request.kind, kind);
        request.retry_on_timeout = true;
        KeyframeRequestDecision::Absorb
    }

    pub fn forget(&mut self, src_media: TransportMediaId, rid: Option<Rid>) {
        self.remove_pending(|request| request.request.targets(src_media, rid));
    }

    pub fn forget_source(&mut self, src_media: TransportMediaId) {
        self.remove_pending(|request| request.request.src_media == src_media);
    }

    pub fn observe_refresh(&mut self, src_media: TransportMediaId, rid: Option<Rid>) -> usize {
        self.remove_pending(|request| {
            request.request.src_media == src_media
                && (request.request.rid.is_none() || request.request.rid == rid)
        })
    }

    pub fn drain_due(&mut self, now: Instant, retries: &mut Vec<SourceKeyframeRequest>) {
        let mut remaining = KEYFRAME_RETRY_DRAIN_LIMIT;
        while matches!(
            self.deadlines.peek(),
            Some(Reverse(deadline)) if deadline.deadline <= now
        ) && remaining > 0
        {
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
            remaining -= 1;
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

    fn remove_pending(&mut self, mut remove: impl FnMut(&KeyframeRequestState) -> bool) -> usize {
        let mut removed = 0;
        let mut index = 0;
        while let Some(request) = self.pending.get(index) {
            if remove(request) {
                let id = self.pending.swap_remove(index).id;
                self.deadlines.retain(|Reverse(deadline)| deadline.id != id);
                removed += 1;
            } else {
                index += 1;
            }
        }
        removed
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
