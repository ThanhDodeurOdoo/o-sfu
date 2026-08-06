//! keyframe request tracking for RTC producer sources
//!
//! duplicate feedback is absorbed while one request is pending
//! bounded feedback retries expire while decoder transitions remain demand-driven

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, Instant},
};

use str0m::media::{KeyframeRequestKind, Rid};

use crate::engine::media_transport::TransportMediaId;

pub(super) const KEYFRAME_REQUEST_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(super) const KEYFRAME_REQUEST_RETRY_ATTEMPTS: u8 = 5;
const KEYFRAME_RETRY_DRAIN_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport) enum KeyframeRequestOrigin {
    ConsumerFeedback,
    RecoveryHint,
    DecoderTransition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKeyframeRequest {
    pub src_media: TransportMediaId,
    pub rid: Option<Rid>,
    pub kind: KeyframeRequestKind,
    pub(super) origin: KeyframeRequestOrigin,
}

impl SourceKeyframeRequest {
    fn targets(self, src_media: TransportMediaId, rid: Option<Rid>) -> bool {
        self.src_media == src_media && self.rid == rid
    }
}

#[derive(Debug, Default)]
pub struct KeyframeRequestTracker {
    pending: Vec<KeyframeRequestState>,
    cooldowns: Vec<KeyframeRequestCooldown>,
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
        origin: KeyframeRequestOrigin,
        now: Instant,
    ) -> KeyframeRequestDecision {
        self.cooldowns.retain(|cooldown| cooldown.until > now);
        if origin == KeyframeRequestOrigin::ConsumerFeedback
            && self.cooldowns.iter().any(|cooldown| {
                cooldown.src_media == src_media && (rid.is_none() || cooldown.rid == rid)
            })
        {
            return KeyframeRequestDecision::Absorb;
        }
        let Some(request) = self
            .pending
            .iter_mut()
            .find(|request| request.request.targets(src_media, rid))
        else {
            let request = SourceKeyframeRequest {
                src_media,
                rid,
                kind,
                origin,
            };
            let pending = KeyframeRequestState {
                request,
                deadline: now + KEYFRAME_REQUEST_RETRY_DELAY,
                id: self.next_id,
                retry_policy: RetryPolicy::for_origin(origin),
            };
            self.next_id = self.next_id.saturating_add(1);
            self.deadlines.push(Reverse(pending.deadline()));
            self.pending.push(pending);
            return KeyframeRequestDecision::Forward;
        };
        request.request.kind = coalesce_kf_kind(request.request.kind, kind);
        if origin == KeyframeRequestOrigin::DecoderTransition {
            request.request.origin = origin;
            request.retry_policy = RetryPolicy::WhileDemand;
        }
        KeyframeRequestDecision::Absorb
    }

    pub fn forget(&mut self, src_media: TransportMediaId, rid: Option<Rid>) {
        self.remove_pending(|request| request.request.targets(src_media, rid));
    }

    pub fn forget_source(&mut self, src_media: TransportMediaId) {
        self.remove_pending(|request| request.request.src_media == src_media);
        self.cooldowns
            .retain(|cooldown| cooldown.src_media != src_media);
    }

    pub fn observe_refresh(
        &mut self,
        src_media: TransportMediaId,
        rid: Option<Rid>,
        now: Instant,
    ) -> usize {
        let removed = self.remove_pending(|request| {
            request.request.src_media == src_media
                && (request.request.rid.is_none() || request.request.rid == rid)
        });
        self.cooldowns.retain(|cooldown| cooldown.until > now);
        let until = now + KEYFRAME_REQUEST_RETRY_DELAY;
        if let Some(cooldown) = self
            .cooldowns
            .iter_mut()
            .find(|cooldown| cooldown.src_media == src_media && cooldown.rid == rid)
        {
            cooldown.until = until;
        } else {
            self.cooldowns.push(KeyframeRequestCooldown {
                src_media,
                rid,
                until,
            });
        }
        removed
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
            if matches!(request.retry_policy, RetryPolicy::Bounded(0)) {
                self.pending.swap_remove(index);
                continue;
            }
            let retry = request.request;
            let reschedule = match &mut request.retry_policy {
                RetryPolicy::Bounded(retries_remaining) => {
                    *retries_remaining -= 1;
                    *retries_remaining > 0
                }
                RetryPolicy::WhileDemand => true,
            };
            if reschedule {
                request.deadline = now + KEYFRAME_REQUEST_RETRY_DELAY;
                request.id = self.next_id;
                self.next_id = self.next_id.saturating_add(1);
                self.deadlines.push(Reverse(request.deadline()));
            } else {
                self.pending.swap_remove(index);
            }
            retries.push(retry);
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
    retry_policy: RetryPolicy,
}

#[derive(Debug, Clone, Copy)]
enum RetryPolicy {
    Bounded(u8),
    WhileDemand,
}

impl RetryPolicy {
    const fn for_origin(origin: KeyframeRequestOrigin) -> Self {
        match origin {
            KeyframeRequestOrigin::ConsumerFeedback | KeyframeRequestOrigin::RecoveryHint => {
                Self::Bounded(KEYFRAME_REQUEST_RETRY_ATTEMPTS)
            }
            KeyframeRequestOrigin::DecoderTransition => Self::WhileDemand,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct KeyframeRequestCooldown {
    src_media: TransportMediaId,
    rid: Option<Rid>,
    until: Instant,
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
