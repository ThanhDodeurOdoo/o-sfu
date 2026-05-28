//! worker-owned state for the RTC engine transport layer
//!
//! this module holds the mutable facts that must stay local to one packet-loop
//! worker:
//!
//! ```text
//! room and router intent
//!   |
//!   v
//! worker commands
//!   |
//!   v
//! PacketLoopState
//!   |
//!   +--> str0m sessions
//!   +--> media route indexes
//!   +--> demux hints
//!   +--> packet-loop schedules
//! ```
//!
//! `PacketLoopState` is the authoritative hot-path state
//! callers outside the worker observe selected facts through
//! `RtcSnapshotState`, bitrate counters and diagnostics instead of mutating
//! this state directly
//!
//! `test_support` owns deterministic lifecycle bookkeeping and state mutators
//! that only exist for adapter tests

#[cfg(test)]
pub(in crate::engine::rtc) mod test_support;

use std::{
    cmp::{Ordering as CmpOrdering, Reverse},
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::{
    Rtc,
    change::SdpPendingOffer,
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tokio::net::UdpSocket;

use super::{
    bitrate::MediaBitrateCounter,
    demux::{MediaRouteEntry, MediaRouteKey, RemoteAddrDemux},
    local_send_rewrite::ConsumerStreamStore,
    media_registry::{DecoderRefreshCodec, RemoteSourceRegistration, SessionMediaRegistry},
    relay_registry::RelaySourceRegistration,
    route_control::RouteControlState,
    slots::{MediaStore, SessionHandle, SessionStore},
};
pub use crate::engine::media_transport::TransportSessionHealth;
use crate::{
    Bitrate,
    engine::media_transport::{
        ReceiverBandwidthSnapshot, SessionUploadSlot, TransportMediaId, TransportQualitySample,
        TransportQualitySnapshot, TransportSessionKey,
    },
};

/// shared UDP socket owned by one RTC worker
///
/// every live session on the worker advertises the same candidate address and
/// uses `Rtc::accepts()` to decide whether an inbound datagram belongs to that
/// session
pub(super) struct SharedRtcSocket {
    /// tokio socket used by the packet loop after worker bootstrap
    pub(super) socket: Arc<UdpSocket>,
    /// public candidate tuple inserted into local SDP for sessions on this worker
    pub(super) candidate_addr: SocketAddr,
}

/// worker-owned `str0m::Rtc` state for one transport session
///
/// this state is single-threaded under [`PacketLoopState`]
/// control commands mutate negotiation or routing facts before the packet loop
/// polls `rtc`, which keeps `str0m` access ordered without a per-session lock
pub(super) struct RtcSessionState {
    /// sans-I/O WebRTC engine driven only by the packet-loop worker
    pub(super) rtc: Rtc,
    /// creation time used for transport lifetime metrics during session teardown
    pub(super) started_at: Instant,
    /// local ICE fragment registered in demux recovery hints
    pub(super) local_ice_ufrag: String,
    #[cfg(test)]
    /// last inbound bitrate cap applied by tests that inspect negotiation refreshes
    pub(super) max_bitrate_in: Option<Bitrate>,
    #[cfg(test)]
    /// outbound bitrate cap used to build the session in deterministic tests
    pub(super) max_bitrate_out: Option<Bitrate>,
    /// whether a remote answer has committed DTLS and media packet routing can start
    pub(super) dtls_started: bool,
    /// scheduler bit that prevents duplicate dirty-session wakeups
    pub(super) packet_loop_dirty: bool,
    /// staged SDP state owned by the worker-local offer and answer paths
    pub(super) sdp_negotiation: SessionSdpNegotiationState,
    /// monotonic RTP identity state keyed by consumer transport media
    ///
    /// this belongs to the destination session because the browser sees one
    /// local RTP stream per consumer route, independent from whichever
    /// publisher SSRC or RID currently feeds that route
    pub(super) consumer_streams: ConsumerStreamStore,
}

/// offer and answer staging state for one worker-owned session
///
/// this preserves `str0m`'s one-outstanding-offer rule while media lifecycle
/// code can stage additions or removals through serialized worker commands
#[derive(Default)]
pub(super) struct SessionSdpNegotiationState {
    /// initial audio and video MIDs reused across repeated bootstrap offer attempts
    pub(super) bootstrap_mids: Vec<Mid>,
    /// `str0m` token that must be accepted by the next remote answer
    pub(super) pending_offer: Option<SdpPendingOffer>,
    /// follow-up local offer prepared by media lifecycle and not yet delivered
    pub(super) staged_offer_sdp: Option<String>,
    /// upload slots that belong to the currently staged local offer
    pub(super) staged_offer_upload_slots: Vec<SessionUploadSlot>,
    /// whether the remote side has answered the initial transport offer
    pub(super) initial_offer_applied: bool,
    /// producer recv identities that must be rebound after answer application
    ///
    /// answer-time `str0m` updates can recreate `StreamRx` bindings, so the
    /// worker keeps the intended SSRC and RID pairs until the matching answer
    /// has been applied
    pub(super) pending_recv_streams: BTreeMap<Mid, Vec<PendingRecvStream>>,
    /// negotiated producer RTP parameters keyed by producer MID
    ///
    /// answer application refreshes this table so transport handles can report
    /// the exact upload encodings that are now committed
    pub(super) negotiated_producer_parameters: BTreeMap<Mid, RouterRtpParameters>,
    /// negotiated MIDs whose inactive offer must wait for the current answer
    pub(super) queued_removal_mids: BTreeSet<Mid>,
}

/// receive stream identity staged before a producer media addition is answered
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingRecvStream {
    /// producer SSRC expected by the receiving `str0m` media line
    pub(super) ssrc: Ssrc,
    /// simulcast RID for the producer encoding when the source uses RID identity
    pub(super) rid: Option<Rid>,
}

/// authoritative mutable state for one RTC packet-loop worker
///
/// the packet loop owns this value without a mutex
/// control commands, UDP ingress, `str0m` polling and relay fanout all pass
/// through one mutable borrow so the media indexes can be updated together
///
/// command-facing APIs keep stable transport ids while hot queues use
/// generation-checked handles
#[derive(Default)]
pub(super) struct PacketLoopState {
    /// lazily bound worker socket cleared when the last session leaves
    pub(super) shared_socket: Option<SharedRtcSocket>,
    /// live worker-owned RTC sessions
    pub(super) users: SessionStore,
    /// source media id to local consumer destinations for packet fanout
    pub(super) media_route_index: BTreeMap<MediaRouteKey, MediaRouteEntry>,
    /// packet-layer source policy already projected from room decisions
    pub(super) route_control: RouteControlState,
    /// session-scoped media lookup vectors for packet source resolution
    pub(super) session_media: SessionMediaRegistry,
    /// producer SSRC bindings owned by each media id for teardown
    pub(super) producer_ssrcs_by_media: BTreeMap<TransportMediaId, Vec<Ssrc>>,
    /// packet-level decoder-refresh classifier for local or remote sources
    pub(super) source_decoder_refresh_codecs: BTreeMap<TransportMediaId, DecoderRefreshCodec>,
    /// recently observed producer RIDs keyed by source media id
    ///
    /// this is packet-path liveness, not signaling truth
    /// readiness decides when a strict selected-RID gate can become effective
    /// and when a stale selected gate must go pending again
    pub(super) live_producer_rids: BTreeMap<TransportMediaId, Vec<ProducerRidLiveness>>,
    /// delayed selected-RID keyframe refreshes owned by the packet-loop clock
    ///
    /// chrome can need more than one PLI after a RID first becomes live
    /// the queue is source keyed so retries survive normal packet-loop turns
    /// without adding room-policy work to the hot path
    pub(super) pending_rid_keyframe_refreshes:
        BTreeMap<TransportMediaId, Vec<PendingRidKeyframeRefresh>>,
    /// deadline heap for selected-RID keyframe refreshes with lazy cancellation
    pub(super) pending_rid_keyframe_refresh_queue: BinaryHeap<Reverse<PendingRidKeyframeRefresh>>,
    /// tie-breaker that keeps refresh heap ordering stable for equal deadlines
    pub(super) next_rid_keyframe_refresh_id: u64,
    /// reusable selected-RID readiness scratch vectors
    pub(super) rid_readiness_scratch: RidReadinessScratch,
    /// packet-loop write handles for incoming media bitrate accounting
    pub(super) incoming_bitrate_counters: BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
    /// packet-loop write handles for per-session egress bitrate accounting
    pub(super) egress_bitrate_counters: BTreeMap<TransportSessionKey, Arc<MediaBitrateCounter>>,
    /// command path for source media owned by another worker
    pub(super) remote_source_registry: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
    /// cross-worker relay destinations indexed by local source media id
    pub(super) relay_targets: BTreeMap<TransportMediaId, RelaySourceRegistration>,
    /// worker-local UDP ingress demux hints
    pub(super) remote_addr_demux: RemoteAddrDemux,
    /// primary media handle table keyed by stable transport media id
    pub(super) mid_registry: MediaStore,
    /// sessions that must be polled before the worker waits again
    pub(super) dirty_sessions: Vec<SessionHandle>,
    /// latest `str0m` timeout deadline per live session handle
    pub(super) session_timeouts: BTreeMap<SessionHandle, Instant>,
    /// timeout heap that may contain stale entries invalidated by `session_timeouts`
    pub(super) timeout_queue: BinaryHeap<Reverse<(Instant, SessionHandle)>>,
    /// next worker-local media id from the disjoint range assigned at boot
    pub(super) next_media_id: u64,
}

impl PacketLoopState {
    /// schedule a live session for the next packet-loop poll
    ///
    /// missing sessions are ignored because teardown may race with already
    /// queued wakeups
    /// each live session can appear at most once until
    /// [`Self::collect_ready_sessions`] clears its dirty bit
    pub(super) fn mark_session_dirty(&mut self, session_key: &TransportSessionKey) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        let Some(session_state) = self.users.get_mut(session_key) else {
            return;
        };
        if session_state.packet_loop_dirty {
            return;
        }
        session_state.packet_loop_dirty = true;
        self.dirty_sessions.push(session_handle);
    }

    /// report whether the worker has session work that is due immediately
    pub(super) fn has_dirty_sessions(&self) -> bool {
        !self.dirty_sessions.is_empty()
    }

    /// drain dirty sessions and due `str0m` timeouts into caller-owned scratch
    ///
    /// this method is the session scheduler for the packet loop
    /// it clears dirty bits for live sessions, skips removed sessions and lazily
    /// discards timeout heap entries whose deadline no longer matches
    /// [`Self::session_timeouts`]
    ///
    /// stale handles are skipped before replacement sessions can be polled
    ///
    /// the output is sorted and deduplicated so a session that is both dirty
    /// and timed out is polled once in the current turn
    pub(super) fn collect_ready_sessions(
        &mut self,
        now: Instant,
        ready_sessions: &mut Vec<SessionHandle>,
    ) {
        for session_handle in self.dirty_sessions.drain(..) {
            if let Some(session_state) = self.users.get_mut_by_handle(session_handle) {
                session_state.packet_loop_dirty = false;
                ready_sessions.push(session_handle);
            }
        }
        let session_timeouts = &mut self.session_timeouts;
        let users = &self.users;
        while let Some((deadline, session_handle)) = pop_due_deadline(
            &mut self.timeout_queue,
            now,
            |(deadline, _session_handle)| *deadline,
        ) {
            if session_timeouts.get(&session_handle).copied() == Some(deadline) {
                session_timeouts.remove(&session_handle);
                if users.key_for_handle(session_handle).is_some() {
                    ready_sessions.push(session_handle);
                }
            }
        }
        ready_sessions.sort_unstable();
        ready_sessions.dedup();
    }

    /// replace the next `str0m` timeout deadline by worker-local handle
    ///
    /// stale handles are ignored because the session has already left this
    /// worker or the slot now belongs to a later generation
    pub(super) fn update_session_timeout_by_handle(
        &mut self,
        session_handle: SessionHandle,
        next_timeout: Option<Instant>,
    ) {
        if self.users.key_for_handle(session_handle).is_none() {
            return;
        }
        self.session_timeouts.remove(&session_handle);
        if let Some(next_timeout) = next_timeout {
            self.session_timeouts.insert(session_handle, next_timeout);
            self.timeout_queue
                .push(Reverse((next_timeout, session_handle)));
        }
    }

    #[cfg(test)]
    pub(super) fn update_session_timeout(
        &mut self,
        session_key: &TransportSessionKey,
        next_timeout: Option<Instant>,
    ) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        self.update_session_timeout_by_handle(session_handle, next_timeout);
    }

    /// return the earliest live `str0m` timeout deadline
    ///
    /// stale heap entries are removed while searching
    /// this includes entries for handles whose slot generation no longer names
    /// a live session
    /// callers may invoke this before awaiting because it does not borrow any
    /// session state after returning
    pub(super) fn next_timeout_deadline(&mut self) -> Option<Instant> {
        let session_timeouts = &self.session_timeouts;
        let users = &self.users;
        next_live_deadline(
            &mut self.timeout_queue,
            |(deadline, session_key)| {
                session_timeouts.get(session_key).copied() == Some(*deadline)
                    && users.key_for_handle(*session_key).is_some()
            },
            |(deadline, _session_key)| *deadline,
        )
    }

    /// remove all explicit scheduler state for a session being torn down
    ///
    /// stale timeout heap entries can remain because the deadline map no longer
    /// validates them
    /// stale handles are also rejected by generation checks before polling
    pub(super) fn clear_session_schedule(&mut self, session_key: &TransportSessionKey) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        self.dirty_sessions.retain(|dirty| *dirty != session_handle);
        if let Some(session_state) = self.users.get_mut(session_key) {
            session_state.packet_loop_dirty = false;
        }
        self.session_timeouts.remove(&session_handle);
    }

    /// record packet-path liveness for one producer RID
    ///
    /// returns `true` only when this source media id has not observed that RID
    /// before
    /// callers use that edge to apply selected-RID readiness work once per new
    /// layer while later packets still refresh freshness
    pub(super) fn observe_producer_rid_packet(
        &mut self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        now: Instant,
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

    /// report whether a producer RID has recent packet liveness
    ///
    /// readiness is intentionally time-bound
    /// a RID that was live earlier may become stale after encoder adaptation
    pub(super) fn producer_rid_is_ready(
        &self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        now: Instant,
        max_age: Duration,
    ) -> bool {
        self.live_producer_rids
            .get(&transport_media_id)
            .and_then(|rids| rids.iter().find(|liveness| liveness.rid() == rid))
            .is_some_and(|liveness| liveness.is_ready(now, max_age))
    }

    /// copy every fresh producer RID for one source into caller-owned scratch
    ///
    /// the scratch vector is cleared first so callers can reuse one allocation
    /// across packet-loop turns
    pub(super) fn collect_ready_producer_rids(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
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

    /// forget packet-path RID memory for a removed source media id
    ///
    /// delayed keyframe refreshes are invalidated through the source map
    /// the heap may keep stale entries until the next deadline query drains them
    pub(super) fn forget_live_producer_rids(&mut self, transport_media_id: TransportMediaId) {
        self.live_producer_rids.remove(&transport_media_id);
        self.pending_rid_keyframe_refreshes
            .remove(&transport_media_id);
    }

    /// schedule one best-effort keyframe refresh for a selected producer RID
    ///
    /// the source map owns cancellation while the heap owns wakeup ordering
    /// source teardown removes the map entry and later heap scans skip the stale
    /// refresh
    pub(super) fn schedule_rid_keyframe_refresh(
        &mut self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        request_at: Instant,
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

    /// drain due refreshes for one source and RID after observing that RID
    ///
    /// returns how many delayed refreshes became due
    /// heap entries are lazy-cancelled through the source map and cleaned by
    /// deadline queries
    pub(super) fn drain_due_rid_keyframe_refreshes(
        &mut self,
        transport_media_id: TransportMediaId,
        rid: Rid,
        now: Instant,
    ) -> usize {
        let Some(refreshes) = self
            .pending_rid_keyframe_refreshes
            .get_mut(&transport_media_id)
        else {
            return 0;
        };
        let mut due_count = 0;
        refreshes.retain(|refresh| {
            let due = refresh.rid == rid && refresh.request_at <= now;
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

    /// drain every due selected-RID keyframe refresh across sources
    ///
    /// this is driven by the packet-loop timeout clock so refreshes can fire
    /// even when the selected RID stops sending packets
    /// stale entries are discarded when the source map no longer contains the
    /// exact refresh
    pub(super) fn drain_due_rid_keyframe_refreshes_for_all(
        &mut self,
        now: Instant,
    ) -> Vec<(TransportMediaId, Rid)> {
        let mut due_refreshes = Vec::new();
        let pending_refreshes = &mut self.pending_rid_keyframe_refreshes;
        while let Some(refresh) = pop_due_deadline(
            &mut self.pending_rid_keyframe_refresh_queue,
            now,
            |refresh| refresh.request_at,
        ) {
            if remove_pending_rid_keyframe_refresh(pending_refreshes, refresh) {
                due_refreshes.push((refresh.transport_media_id, refresh.rid));
            }
        }
        due_refreshes
    }

    /// return the next selected-RID refresh deadline
    ///
    /// the heap uses lazy cancellation, so invalidated entries are removed while
    /// searching for the next live refresh
    pub(super) fn next_rid_keyframe_refresh_deadline(&mut self) -> Option<Instant> {
        let pending_refreshes = &self.pending_rid_keyframe_refreshes;
        next_live_deadline(
            &mut self.pending_rid_keyframe_refresh_queue,
            |refresh| has_pending_rid_keyframe_refresh(pending_refreshes, *refresh),
            |refresh| refresh.request_at,
        )
    }
}

fn pop_due_deadline<T>(
    heap: &mut BinaryHeap<Reverse<T>>,
    now: Instant,
    mut deadline: impl FnMut(&T) -> Instant,
) -> Option<T>
where
    T: Ord,
{
    if !matches!(heap.peek(), Some(Reverse(entry)) if deadline(entry) <= now) {
        return None;
    }
    heap.pop().map(|Reverse(entry)| entry)
}

fn next_live_deadline<T>(
    heap: &mut BinaryHeap<Reverse<T>>,
    mut is_live_entry: impl FnMut(&T) -> bool,
    mut deadline: impl FnMut(&T) -> Instant,
) -> Option<Instant>
where
    T: Ord,
{
    loop {
        let Reverse(entry) = heap.peek()?;
        if is_live_entry(entry) {
            return Some(deadline(entry));
        }
        heap.pop();
    }
}

fn has_pending_rid_keyframe_refresh(
    pending_refreshes: &BTreeMap<TransportMediaId, Vec<PendingRidKeyframeRefresh>>,
    refresh: PendingRidKeyframeRefresh,
) -> bool {
    pending_refreshes
        .get(&refresh.transport_media_id)
        .is_some_and(|refreshes| refreshes.contains(&refresh))
}

fn remove_pending_rid_keyframe_refresh(
    pending_refreshes: &mut BTreeMap<TransportMediaId, Vec<PendingRidKeyframeRefresh>>,
    refresh: PendingRidKeyframeRefresh,
) -> bool {
    let Some(refreshes) = pending_refreshes.get_mut(&refresh.transport_media_id) else {
        return false;
    };
    let Some(position) = refreshes.iter().position(|pending| *pending == refresh) else {
        return false;
    };
    refreshes.swap_remove(position);
    if refreshes.is_empty() {
        pending_refreshes.remove(&refresh.transport_media_id);
    }
    true
}

/// reusable selected-RID readiness vectors owned by packet-loop state
///
/// selected-RID route updates need several temporary RID sets while scanning a
/// source route
/// keeping the vectors here lets the packet loop clear and reuse capacity
/// instead of allocating during steady media flow
#[derive(Default)]
pub(super) struct RidReadinessScratch {
    /// producer RIDs whose liveness is fresh enough to become effective route gates
    pub(super) ready: Vec<Rid>,
    /// selected RIDs that became stale and need a recovery keyframe request
    pub(super) stale: Vec<Rid>,
    /// selected RIDs still waiting for packet-path liveness
    pub(super) pending_selected: Vec<Rid>,
}

impl RidReadinessScratch {
    /// clear every scratch vector while preserving capacity
    pub(super) fn clear(&mut self) {
        self.ready.clear();
        self.stale.clear();
        self.pending_selected.clear();
    }
}

/// packet-path readiness for one producer RID
///
/// this is intentionally time based
/// a RID that was live once may go quiet after browser encoder adaptation, so
/// strict gates consult freshness instead of treating the first packet as
/// permanent readiness
#[derive(Debug, Clone)]
pub(super) struct ProducerRidLiveness {
    /// producer RID observed on the packet path
    rid: Rid,
    /// last packet time for freshness checks
    last_seen: Instant,
}

impl ProducerRidLiveness {
    /// create liveness state from the first observed packet for a RID
    fn new(rid: Rid, observed_at: Instant) -> Self {
        Self {
            rid,
            last_seen: observed_at,
        }
    }

    /// return the RID this liveness entry represents
    const fn rid(&self) -> Rid {
        self.rid
    }

    /// refresh liveness after another packet for the same RID
    fn observe(&mut self, observed_at: Instant) {
        self.last_seen = observed_at;
    }

    /// report whether the RID is fresh enough for selected-RID routing
    fn is_ready(&self, now: Instant, max_age: Duration) -> bool {
        now.duration_since(self.last_seen) <= max_age
    }
}

/// scheduled keyframe retry for one selected producer RID
///
/// the retry is tied to the source media id by the surrounding map
/// it is packet-loop recovery work, not a room policy update and must not
/// outlive source removal
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingRidKeyframeRefresh {
    /// packet-loop deadline for the refresh
    request_at: Instant,
    /// monotonic identity used to order equal deadlines
    id: u64,
    /// source media id that owns the selected RID
    transport_media_id: TransportMediaId,
    /// selected producer RID that should receive another refresh request
    rid: Rid,
}

impl PendingRidKeyframeRefresh {
    /// create a refresh owned by source-keyed packet-loop state
    fn new(id: u64, transport_media_id: TransportMediaId, rid: Rid, request_at: Instant) -> Self {
        Self {
            request_at,
            id,
            transport_media_id,
            rid,
        }
    }
}

impl Ord for PendingRidKeyframeRefresh {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (self.request_at, self.id).cmp(&(other.request_at, other.id))
    }
}

impl PartialOrd for PendingRidKeyframeRefresh {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// read-side RTC transport snapshot shared outside the packet loop
///
/// this state mirrors facts that diagnostics, placement and transport policy
/// need without exposing mutable [`PacketLoopState`]
/// it is protected by a cold-path mutex while packet-path state remains
/// worker-owned and single-threaded
#[derive(Debug, Default)]
pub struct RtcSnapshotState {
    /// demux hints visible to diagnostics and recovery tooling
    pub(super) remote_addr_demux: RemoteAddrDemux,
    /// sessions that currently have worker-owned RTC state
    pub(super) live_sessions: BTreeSet<TransportSessionKey>,
    /// latest observed transport health by session
    transport_health_by_session: BTreeMap<TransportSessionKey, TransportSessionHealth>,
    /// latest receiver bandwidth estimate by session
    receiver_bandwidth_by_session: BTreeMap<TransportSessionKey, Bitrate>,
    /// latest sampled media quality by session
    transport_quality_by_session: BTreeMap<TransportSessionKey, TransportQualitySample>,
}

impl RtcSnapshotState {
    /// mark a session as visible in read-side RTC state
    pub(super) fn add_session(&mut self, session_key: &TransportSessionKey) {
        self.live_sessions.insert(session_key.clone());
    }

    /// remove every read-side fact owned by one session
    ///
    /// returns the previous transport health so teardown metrics can record the
    /// final transition without doing a second lookup
    pub(super) fn remove_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.live_sessions.remove(session_key);
        self.remote_addr_demux.forget_user_remote_addrs(session_key);
        self.remote_addr_demux
            .forget_user_local_ice_ufrag(session_key);
        self.remote_addr_demux
            .forget_user_remote_candidate_addrs(session_key);
        self.receiver_bandwidth_by_session.remove(session_key);
        self.transport_quality_by_session.remove(session_key);
        self.transport_health_by_session.remove(session_key)
    }

    /// replace the latest transport health observation for one session
    ///
    /// returns the previous value so callers can record health transitions
    pub(super) fn set_transport_health(
        &mut self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) -> Option<TransportSessionHealth> {
        self.transport_health_by_session
            .insert(session_key.clone(), health)
    }

    /// return the latest health observation for a session
    ///
    /// missing health means the packet loop has not observed a transport event
    /// for that session or the session was removed
    pub fn transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.transport_health_by_session.get(session_key).copied()
    }

    /// replace the latest receiver bandwidth estimate for one session
    pub(super) fn set_receiver_bandwidth(
        &mut self,
        session_key: &TransportSessionKey,
        estimate: Bitrate,
    ) -> Option<Bitrate> {
        self.receiver_bandwidth_by_session
            .insert(session_key.clone(), estimate)
    }

    /// build a receiver bandwidth snapshot for the requested sessions
    ///
    /// sessions without an estimate are omitted so callers can distinguish
    /// missing observations from a zero bitrate estimate
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        ReceiverBandwidthSnapshot {
            per_session: session_keys
                .iter()
                .filter_map(|session_key| {
                    self.receiver_bandwidth_by_session
                        .get(session_key)
                        .copied()
                        .map(|estimate| (session_key.clone(), estimate))
                })
                .collect(),
        }
    }

    /// update sampled transport-quality observations for one session
    pub(super) fn update_transport_quality(
        &mut self,
        session_key: &TransportSessionKey,
        update: impl FnOnce(&mut TransportQualitySample),
    ) {
        let sample = self
            .transport_quality_by_session
            .entry(session_key.clone())
            .or_default();
        sample.sample_count = sample.sample_count.saturating_add(1);
        update(sample);
    }

    /// build a transport-quality snapshot for the requested sessions
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        TransportQualitySnapshot {
            per_session: session_keys
                .iter()
                .filter_map(|session_key| {
                    self.transport_quality_by_session
                        .get(session_key)
                        .copied()
                        .map(|sample| (session_key.clone(), sample))
                })
                .collect(),
        }
    }
}
