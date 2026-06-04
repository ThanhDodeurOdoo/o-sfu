use std::time::{Duration, Instant};

use str0m::media::{KeyframeRequestKind, Rid};

use crate::engine::media_transport::TransportMediaId;

pub(super) const KEYFRAME_REQUEST_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(super) const KEYFRAME_RETRY_DRAIN_LIMIT: usize = 64;
pub(super) type KeyframeRequestDeadline = (Instant, u64, TransportMediaId);
pub(super) type SourceKeyframeDeadline = (Instant, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct SourceKeyframeRequest {
    pub(in crate::engine::media_transport::rtc) src_media: TransportMediaId,
    pub(in crate::engine::media_transport::rtc) rid: Option<Rid>,
    pub(in crate::engine::media_transport::rtc) kind: KeyframeRequestKind,
}

#[derive(Debug, Default)]
pub(super) struct SourceKeyframeRequests {
    pending: Vec<KeyframeRequestState>,
}

pub(in crate::engine::media_transport::rtc) fn coalesce_kf_kind(
    current: KeyframeRequestKind,
    incoming: KeyframeRequestKind,
) -> KeyframeRequestKind {
    match (current, incoming) {
        (KeyframeRequestKind::Fir, _) | (_, KeyframeRequestKind::Fir) => KeyframeRequestKind::Fir,
        _ => current,
    }
}

impl SourceKeyframeRequests {
    pub(super) fn track(
        &mut self,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: Instant,
        id: u64,
    ) -> (KeyframeRequestDecision, Option<SourceKeyframeDeadline>) {
        let Some(request) = self.pending.iter_mut().find(|request| request.rid == rid) else {
            let deadline = now + KEYFRAME_REQUEST_RETRY_DELAY;
            let pending = KeyframeRequestState {
                rid,
                kind,
                deadline,
                id,
                retry_on_timeout: false,
            };
            self.pending.push(pending);
            return (KeyframeRequestDecision::Forward, Some((deadline, id)));
        };
        request.kind = coalesce_kf_kind(request.kind, kind);
        request.retry_on_timeout = true;
        (KeyframeRequestDecision::Absorb, None)
    }

    pub(super) fn forget(&mut self, rid: Option<Rid>) {
        if let Some(index) = self.pending.iter().position(|request| request.rid == rid) {
            self.pending.swap_remove(index);
        }
    }

    pub(super) fn observe_refresh(&mut self, rid: Option<Rid>) -> usize {
        let before = self.pending.len();
        self.pending
            .retain(|request| request.rid.is_some() && request.rid != rid);
        before - self.pending.len()
    }

    pub(super) fn drain_due(
        &mut self,
        deadline: Instant,
        id: u64,
        src_media: TransportMediaId,
        now: Instant,
        next_id: u64,
    ) -> Option<(SourceKeyframeRequest, KeyframeRequestDeadline)> {
        let index = self
            .pending
            .iter()
            .position(|request| request.matches_deadline(deadline, id))?;
        let request = self.pending.get_mut(index)?;
        if !request.retry_on_timeout {
            self.pending.swap_remove(index);
            return None;
        }
        request.retry_on_timeout = false;
        request.deadline = now + KEYFRAME_REQUEST_RETRY_DELAY;
        request.id = next_id;
        Some((
            SourceKeyframeRequest {
                src_media,
                rid: request.rid,
                kind: request.kind,
            },
            (request.deadline, request.id, src_media),
        ))
    }

    pub(super) fn has_deadline(&self, deadline: SourceKeyframeDeadline) -> bool {
        let (deadline, id) = deadline;
        self.pending
            .iter()
            .any(|request| request.matches_deadline(deadline, id))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[derive(Debug, Clone, Copy)]
struct KeyframeRequestState {
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
    deadline: Instant,
    id: u64,
    retry_on_timeout: bool,
}

impl KeyframeRequestState {
    fn matches_deadline(self, deadline: Instant, id: u64) -> bool {
        self.deadline == deadline && self.id == id
    }
}
