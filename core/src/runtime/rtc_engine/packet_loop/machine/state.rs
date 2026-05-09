use std::{
    cmp::{Ordering as CmpOrdering, Reverse},
    time::Duration,
};

use o_sfu_router::MediaStream;
use str0m::media::{MediaKind as Str0mMediaKind, Rid};

use super::{super::time::PacketLoopTime, source_facts::PacketLoopSourceFacts};
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

#[allow(
    private_interfaces,
    reason = "PacketLoopState is publicly re-exported only for packet-loop verification, while direct field access remains an rtc_engine-only implementation contract"
)]
pub(in crate::runtime::rtc_engine) mod internal {
    use std::{
        cmp::Reverse,
        collections::{BTreeMap, BTreeSet, BinaryHeap},
    };

    use str0m::{media::Rid, rtp::Ssrc};

    use super::{PendingRidKeyframeRefresh, ProducerRidLiveness};
    use crate::runtime::{
        media_transport::{TransportMediaId, TransportSessionKey},
        rtc_engine::{
            demux::{MediaRouteEntry, MediaRouteKey, RemoteAddrDemux},
            media_registry::{
                ConsumerMidLookupKey, ProducerMidLookupKey, ProducerSsrcLookupKey,
                RegisteredMediaHandle, RemoteSourceRegistration,
            },
            packet_loop::{machine::source_facts::PacketLoopSourceFacts, time::PacketLoopTime},
            relay_registry::RelaySourceRegistration,
            route_control::RouteControlState,
        },
    };

    /// persistant packet-loop indexes and schedulers owned by one RTC worker
    ///
    /// the state survives across turns. turn-local packet buffers, temporary
    /// fanout plans and staged effects belong in `PacketLoopScratch`
    #[derive(Default)]
    pub struct PacketLoopState {
        /// source-owned fanout graph used after an ingress packet resolves to media
        ///
        /// entries are keyed by source [`TransportMediaId`] and must be removed
        /// when the source leaves
        pub media_route_index: BTreeMap<MediaRouteKey, MediaRouteEntry>,
        /// per-source route policy for layer gates, audio activity and
        /// keyframe coalescing
        pub route_control: RouteControlState,
        /// reverse index from a local producer session plus MID to its source
        /// media id
        ///
        /// this is the stable answer-side lookup before negotiated SSRC
        /// bindings are available
        pub producer_mid_registry: BTreeMap<ProducerMidLookupKey, TransportMediaId>,
        /// reverse index from a local producer session plus negotiated SSRC to
        /// its source media id
        ///
        /// `UDP` ingress uses this when packets do not carry a usable MID
        pub producer_ssrc_registry: BTreeMap<ProducerSsrcLookupKey, TransportMediaId>,
        /// companion RID index for negotiated producer SSRC bindings
        ///
        /// route control uses it to recover layer metadata when packets do not
        /// expose a RID extension
        pub producer_ssrc_rid_registry: BTreeMap<ProducerSsrcLookupKey, Rid>,
        /// owned SSRC list for each source media id
        ///
        /// teardown uses the list to clear every SSRC lookup without scanning
        /// the whole producer registry
        pub producer_ssrcs_by_media: BTreeMap<TransportMediaId, Vec<Ssrc>>,
        /// recently observed RID liveness for each producer source
        ///
        /// selected-RID gates use this to wait for real packets before
        /// activating a chosen layer
        pub live_producer_rids: BTreeMap<TransportMediaId, Vec<ProducerRidLiveness>>,
        /// authoritative set of scheduled selected-RID refreshes per source
        ///
        /// this validates heap entries after lazy cancellation
        pub pending_rid_keyframe_refreshes:
            BTreeMap<TransportMediaId, Vec<PendingRidKeyframeRefresh>>,
        /// deadline heap for selected-RID refreshes
        ///
        /// entries may be stale, so callers must confirm them against
        /// `pending_rid_keyframe_refreshes` before firing
        pub pending_rid_keyframe_refresh_queue: BinaryHeap<Reverse<PendingRidKeyframeRefresh>>,
        /// deterministic tie-breaker for scheduled RID refreshes
        ///
        /// equal deadlines must still drain in a stable order
        pub next_rid_keyframe_refresh_id: u64,
        /// reverse index from a consumer session plus consumer-local MID to the
        /// source media id it receives
        ///
        /// keyframe feedback depends on this mapping
        pub consumer_mid_registry: BTreeMap<ConsumerMidLookupKey, TransportMediaId>,
        /// remote ownership records for sources imported from another worker or
        /// node
        ///
        /// keyframe feedback and teardown use this as the control route back to
        /// the source owner
        pub remote_source_registry: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
        /// relay target topology keyed by source media id
        ///
        /// route snapshots clone active targets only when the topology
        /// generation changes
        pub relay_targets: BTreeMap<TransportMediaId, RelaySourceRegistration>,
        /// dirty generation for relay target topology
        ///
        /// this changes only when the active relay set can affect snapshot
        /// refresh
        pub relay_topology_generation: u64,
        /// cached packet facts derived from source kind and negotiated
        /// parameters
        ///
        /// packet observation reads this without walking media registration
        /// state
        pub source_facts: BTreeMap<TransportMediaId, PacketLoopSourceFacts>,
        /// sources that have already emitted ingress RTP
        ///
        /// this gates first-packet diagnostics and is cleared with source
        /// liveness
        pub observed_incoming_media: BTreeSet<TransportMediaId>,
        /// remote-address pins and recovery indexes for packets that cannot be
        /// resolved by media identifiers yet
        ///
        /// worker teardown must clear session-owned pins
        pub remote_addr_demux: RemoteAddrDemux,
        /// primary media-handle table keyed by allocated media id
        ///
        /// every reverse media index must point back to entries owned here
        pub mid_registry: BTreeMap<u64, RegisteredMediaHandle>,
        /// sessions that need another host poll after a turn mutates RTC state
        ///
        /// the scheduler drains, sorts and deduplicates this list before
        /// polling
        pub dirty_sessions: Vec<TransportSessionKey>,
        /// authoritative timeout deadline per session
        ///
        /// this validates heap entries so updating a deadline can leave stale
        /// queue entries behind
        pub session_timeouts: BTreeMap<TransportSessionKey, PacketLoopTime>,
        /// deadline heap for session wakeups
        ///
        /// entries are lazily discarded when they no longer match
        /// `session_timeouts`
        pub timeout_queue: BinaryHeap<Reverse<(PacketLoopTime, TransportSessionKey)>>,
        /// next worker-local media id allocated by media registration
        ///
        /// the loop driver seeds it from the worker shard base
        pub next_media_id: u64,
    }
}

pub use internal::PacketLoopState;

impl PacketLoopState {
    pub fn mark_session_dirty(&mut self, session_key: &TransportSessionKey) {
        self.dirty_sessions.push(session_key.clone());
    }

    #[must_use]
    pub fn has_dirty_sessions(&self) -> bool {
        !self.dirty_sessions.is_empty()
    }

    pub fn drain_ready_sessions(
        &mut self,
        now: PacketLoopTime,
        ready_sessions: &mut Vec<TransportSessionKey>,
    ) {
        ready_sessions.clear();
        ready_sessions.append(&mut self.dirty_sessions);
        while let Some((deadline, session_key)) = self.next_valid_timeout_head() {
            if deadline > now {
                break;
            }
            self.timeout_queue.pop();
            self.session_timeouts.remove(&session_key);
            ready_sessions.push(session_key);
        }
        ready_sessions.sort();
        ready_sessions.dedup();
    }

    pub fn update_session_timeout(
        &mut self,
        session_key: &TransportSessionKey,
        next_timeout: Option<PacketLoopTime>,
    ) {
        self.session_timeouts.remove(session_key);
        if let Some(next_timeout) = next_timeout {
            self.session_timeouts
                .insert(session_key.clone(), next_timeout);
            self.timeout_queue
                .push(Reverse((next_timeout, session_key.clone())));
        }
    }

    #[must_use]
    pub fn next_timeout_deadline(&mut self) -> Option<PacketLoopTime> {
        self.next_valid_timeout_head()
            .map(|(deadline, _session_key)| deadline)
    }

    pub fn clear_session_schedule(&mut self, session_key: &TransportSessionKey) {
        self.dirty_sessions
            .retain(|dirty_session| dirty_session != session_key);
        self.session_timeouts.remove(session_key);
    }

    pub(in crate::runtime::rtc_engine) const fn relay_topology_generation(&self) -> u64 {
        self.relay_topology_generation
    }

    pub(in crate::runtime::rtc_engine) fn bump_relay_topology_generation(&mut self) {
        self.relay_topology_generation = self.relay_topology_generation.wrapping_add(1);
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn dirty_session_capacity(&self) -> usize {
        self.dirty_sessions.capacity()
    }

    pub(in crate::runtime::rtc_engine) fn observe_producer_rid_packet(
        &mut self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        now: PacketLoopTime,
    ) -> bool {
        let live_rids = self
            .live_producer_rids
            .entry(transport_media_id)
            .or_default();
        if let Some(liveness) = live_rids.iter_mut().find(|liveness| liveness.rid() == rid) {
            liveness.observe(now);
            return false;
        }
        live_rids.push(ProducerRidLiveness::new(rid, now));
        true
    }

    pub(in crate::runtime::rtc_engine) fn producer_rid_is_ready(
        &self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        now: PacketLoopTime,
        max_age: Duration,
    ) -> bool {
        self.live_producer_rids
            .get(&transport_media_id)
            .and_then(|rids| rids.iter().find(|liveness| liveness.rid() == rid))
            .is_some_and(|liveness| liveness.is_ready(now, max_age))
    }

    pub(in crate::runtime::rtc_engine) fn collect_ready_producer_rids(
        &self,
        transport_media_id: TransportMediaId,
        now: PacketLoopTime,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        ready_rids.clear();
        let Some(live_rids) = self.live_producer_rids.get(&transport_media_id) else {
            return;
        };
        ready_rids.extend(
            live_rids
                .iter()
                .filter(|liveness| liveness.is_ready(now, max_age))
                .map(ProducerRidLiveness::rid),
        );
    }

    pub(in crate::runtime::rtc_engine) fn forget_live_producer_rids(
        &mut self,
        transport_media_id: TransportMediaId,
    ) {
        self.live_producer_rids.remove(&transport_media_id);
        self.pending_rid_keyframe_refreshes
            .remove(&transport_media_id);
        self.observed_incoming_media.remove(&transport_media_id);
    }

    pub(in crate::runtime::rtc_engine) fn set_source_kind(
        &mut self,
        transport_media_id: TransportMediaId,
        kind: Str0mMediaKind,
    ) {
        self.source_facts
            .entry(transport_media_id)
            .or_default()
            .set_kind_from_str0m(kind);
    }

    pub(in crate::runtime::rtc_engine) fn set_source_facts_from_parameters(
        &mut self,
        transport_media_id: TransportMediaId,
        parameters: &MediaStream,
    ) {
        self.source_facts
            .entry(transport_media_id)
            .or_default()
            .set_from_parameters(parameters);
    }

    pub(in crate::runtime::rtc_engine) fn source_facts(
        &self,
        transport_media_id: TransportMediaId,
    ) -> PacketLoopSourceFacts {
        self.source_facts
            .get(&transport_media_id)
            .copied()
            .unwrap_or_default()
    }

    pub(in crate::runtime::rtc_engine) fn forget_source_facts(
        &mut self,
        transport_media_id: TransportMediaId,
    ) {
        self.source_facts.remove(&transport_media_id);
    }

    pub(in crate::runtime::rtc_engine) fn observe_incoming_media(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> bool {
        self.observed_incoming_media.insert(transport_media_id)
    }

    pub(in crate::runtime::rtc_engine) fn schedule_rid_keyframe_refresh(
        &mut self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        request_at: PacketLoopTime,
    ) {
        let refresh = PendingRidKeyframeRefresh::new(
            self.next_rid_keyframe_refresh_id,
            transport_media_id,
            rid,
            request_at,
        );
        self.next_rid_keyframe_refresh_id = self.next_rid_keyframe_refresh_id.saturating_add(1);
        self.pending_rid_keyframe_refreshes
            .entry(transport_media_id)
            .or_default()
            .push(refresh);
        self.pending_rid_keyframe_refresh_queue
            .push(Reverse(refresh));
    }

    pub(in crate::runtime::rtc_engine) fn drain_due_rid_keyframe_refreshes(
        &mut self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        now: PacketLoopTime,
    ) -> usize {
        let Some(refreshes) = self
            .pending_rid_keyframe_refreshes
            .get_mut(&transport_media_id)
        else {
            return 0;
        };
        let mut due_count = 0;
        refreshes.retain(|refresh| {
            let due = refresh.rid() == rid && refresh.is_due(now);
            if due {
                due_count += 1;
            }
            !due
        });
        if refreshes.is_empty() {
            self.pending_rid_keyframe_refreshes
                .remove(&transport_media_id);
        }
        due_count
    }

    pub(in crate::runtime::rtc_engine) fn drain_due_rid_keyframe_refreshes_for_all(
        &mut self,
        now: PacketLoopTime,
    ) -> Vec<(TransportMediaId, Rid)> {
        let mut due_refreshes = Vec::new();
        while let Some(Reverse(refresh)) = self.pending_rid_keyframe_refresh_queue.peek().copied() {
            if !refresh.is_due(now) {
                break;
            }
            self.pending_rid_keyframe_refresh_queue.pop();
            if self.remove_pending_rid_keyframe_refresh(refresh) {
                due_refreshes.push((refresh.transport_media_id(), refresh.rid()));
            }
        }
        due_refreshes
    }

    pub(in crate::runtime::rtc_engine) fn next_rid_keyframe_refresh_deadline(
        &mut self,
    ) -> Option<PacketLoopTime> {
        while let Some(Reverse(refresh)) = self.pending_rid_keyframe_refresh_queue.peek().copied() {
            if self.has_pending_rid_keyframe_refresh(refresh) {
                return Some(refresh.request_at());
            }
            self.pending_rid_keyframe_refresh_queue.pop();
        }
        None
    }

    fn has_pending_rid_keyframe_refresh(&self, refresh: PendingRidKeyframeRefresh) -> bool {
        self.pending_rid_keyframe_refreshes
            .get(&refresh.transport_media_id())
            .is_some_and(|refreshes| refreshes.contains(&refresh))
    }

    fn remove_pending_rid_keyframe_refresh(&mut self, refresh: PendingRidKeyframeRefresh) -> bool {
        let Some(refreshes) = self
            .pending_rid_keyframe_refreshes
            .get_mut(&refresh.transport_media_id())
        else {
            return false;
        };
        let Some(position) = refreshes.iter().position(|pending| *pending == refresh) else {
            return false;
        };
        refreshes.swap_remove(position);
        if refreshes.is_empty() {
            self.pending_rid_keyframe_refreshes
                .remove(&refresh.transport_media_id());
        }
        true
    }

    fn next_valid_timeout_head(&mut self) -> Option<(PacketLoopTime, TransportSessionKey)> {
        while let Some(Reverse((deadline, session_key))) = self.timeout_queue.peek().cloned() {
            if self.session_timeouts.get(&session_key).copied() == Some(deadline) {
                return Some((deadline, session_key));
            }
            self.timeout_queue.pop();
        }
        None
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime::rtc_engine) struct ProducerRidLiveness {
    rid: Rid,
    last_seen: PacketLoopTime,
}

impl ProducerRidLiveness {
    fn new(rid: Rid, observed_at: PacketLoopTime) -> Self {
        Self {
            rid,
            last_seen: observed_at,
        }
    }

    const fn rid(&self) -> Rid {
        self.rid
    }

    fn observe(&mut self, observed_at: PacketLoopTime) {
        self.last_seen = observed_at;
    }

    fn is_ready(&self, now: PacketLoopTime, max_age: Duration) -> bool {
        now.saturating_duration_since(self.last_seen) <= max_age
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_engine) struct PendingRidKeyframeRefresh {
    id: u64,
    transport_media_id: TransportMediaId,
    rid: Rid,
    request_at: PacketLoopTime,
}

impl PendingRidKeyframeRefresh {
    fn new(
        id: u64,
        transport_media_id: TransportMediaId,
        rid: Rid,
        request_at: PacketLoopTime,
    ) -> Self {
        Self {
            id,
            transport_media_id,
            rid,
            request_at,
        }
    }

    const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    const fn rid(self) -> Rid {
        self.rid
    }

    const fn request_at(self) -> PacketLoopTime {
        self.request_at
    }

    fn is_due(self, now: PacketLoopTime) -> bool {
        self.request_at <= now
    }
}

impl PartialOrd for PendingRidKeyframeRefresh {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingRidKeyframeRefresh {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.request_at
            .cmp(&other.request_at)
            .then_with(|| self.id.cmp(&other.id))
    }
}
