use std::{
    cmp::{Ordering as CmpOrdering, Reverse},
    collections::{BTreeMap, BinaryHeap},
    time::Instant,
};

use str0m::media::Rid;

use super::source::RouteSource;
use crate::engine::media_transport::TransportMediaId;

/// selected-rid refresh queue with source-local cancellation
///
/// the heap is only the wake index
/// `RouteSource::producer.pending_rid_refreshes` is the authority
/// stale heap entries are skipped after teardown or direct draining
#[derive(Debug, Default)]
pub(super) struct RidRefreshQueue {
    heap: BinaryHeap<Reverse<RidKeyframeRefresh>>,
    next_id: u64,
}

impl RidRefreshQueue {
    pub(super) fn schedule(
        &mut self,
        source: &mut RouteSource,
        source_id: TransportMediaId,
        rid: Rid,
        request_at: Instant,
    ) {
        let refresh = RidKeyframeRefresh {
            request_at,
            id: self.next_id,
            source_id,
            rid,
        };
        self.next_id = self.next_id.saturating_add(1);
        source.producer.pending_rid_refreshes.push(refresh);
        self.heap.push(Reverse(refresh));
    }

    pub(super) fn drain_due(
        &mut self,
        sources: &mut BTreeMap<TransportMediaId, RouteSource>,
        now: Instant,
    ) -> Vec<(TransportMediaId, Rid)> {
        let mut due = Vec::new();
        while matches!(self.heap.peek(), Some(Reverse(refresh)) if refresh.request_at <= now) {
            let Some(Reverse(refresh)) = self.heap.pop() else {
                break;
            };
            if let Some(source) = sources.get_mut(&refresh.source_id)
                && let Some(position) = source
                    .producer
                    .pending_rid_refreshes
                    .iter()
                    .position(|pending| *pending == refresh)
            {
                source.producer.pending_rid_refreshes.swap_remove(position);
                due.push((refresh.source_id, refresh.rid));
            }
        }
        due
    }

    pub(super) fn next_deadline(
        &mut self,
        sources: &BTreeMap<TransportMediaId, RouteSource>,
    ) -> Option<Instant> {
        loop {
            let Reverse(refresh) = self.heap.peek()?;
            if sources
                .get(&refresh.source_id)
                .is_some_and(|source| source.producer.pending_rid_refreshes.contains(refresh))
            {
                return Some(refresh.request_at);
            }
            self.heap.pop();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RidKeyframeRefresh {
    pub(super) request_at: Instant,
    id: u64,
    source_id: TransportMediaId,
    pub(super) rid: Rid,
}

impl Ord for RidKeyframeRefresh {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (self.request_at, self.id).cmp(&(other.request_at, other.id))
    }
}

impl PartialOrd for RidKeyframeRefresh {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
