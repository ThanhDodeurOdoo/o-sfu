//! Pure state types and user scheduling for the RTC transport shard.
//!
//! `test_support` owns the transport lifecycle bookkeeping and state mutators
//! that exist only for deterministic adapter tests.

#[cfg(test)]
pub(in crate::runtime::rtc_engine) mod test_support;

use std::{
    cmp::{Ordering as CmpOrdering, Reverse},
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap},
    mem::take,
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
    local_send_rewrite::{ConsumerStream, ConsumerStreamKey},
    media_registry::{
        ConsumerMidLookupKey, ProducerMidLookupKey, ProducerSsrcLookupKey, RegisteredMediaHandle,
        RemoteSourceRegistration,
    },
    relay_registry::RelaySourceRegistration,
    route_control::RouteControlState,
};
pub use crate::transport::TransportSessionHealth;
use crate::{
    Bitrate,
    runtime::media_transport::{
        ReceiverBandwidthSnapshot, SessionUploadSlot, TransportMediaId, TransportSessionKey,
    },
};

const PACKET_LOOP_LAG_SAMPLE_TTL: Duration = Duration::from_secs(1);

pub(super) struct SharedRtcSocket {
    pub(super) socket: Arc<UdpSocket>,
    pub(super) candidate_addr: SocketAddr,
}

pub(super) struct RtcSessionState {
    pub(super) rtc: Rtc,
    pub(super) started_at: Instant,
    pub(super) local_ice_ufrag: String,
    #[cfg(test)]
    pub(super) max_bitrate_in: Option<Bitrate>,
    #[cfg(test)]
    pub(super) max_bitrate_out: Option<Bitrate>,
    pub(super) dtls_started: bool,
    pub(super) sdp_negotiation: SessionSdpNegotiationState,
    /// Monotonic RTP identity state keyed by consumer transport media.
    ///
    /// This belongs to the destination session because the browser sees one
    /// local RTP stream per consumer route, independent from whichever
    /// publisher SSRC or RID currently feeds that route.
    pub(super) consumer_streams: HashMap<ConsumerStreamKey, ConsumerStream>,
}

#[derive(Default)]
pub(super) struct SessionSdpNegotiationState {
    pub(super) bootstrap_mids: Vec<Mid>,
    pub(super) pending_offer: Option<SdpPendingOffer>,
    pub(super) staged_offer_sdp: Option<String>,
    pub(super) staged_offer_upload_slots: Vec<SessionUploadSlot>,
    pub(super) initial_offer_applied: bool,
    pub(super) pending_recv_streams: BTreeMap<Mid, Vec<PendingRecvStream>>,
    pub(super) negotiated_producer_parameters: BTreeMap<Mid, RouterRtpParameters>,
    pub(super) queued_removal_mids: BTreeSet<Mid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingRecvStream {
    pub(super) ssrc: Ssrc,
    pub(super) rid: Option<Rid>,
}

#[derive(Default)]
pub(super) struct RtcBootstrapState {
    pub(super) shared_socket: Option<SharedRtcSocket>,
    pub(super) users: BTreeMap<TransportSessionKey, RtcSessionState>,
    pub(super) media_route_index: BTreeMap<MediaRouteKey, MediaRouteEntry>,
    pub(super) route_control: RouteControlState,
    pub(super) producer_mid_registry: BTreeMap<ProducerMidLookupKey, TransportMediaId>,
    pub(super) producer_ssrc_registry: BTreeMap<ProducerSsrcLookupKey, TransportMediaId>,
    pub(super) producer_ssrc_rid_registry: BTreeMap<ProducerSsrcLookupKey, Rid>,
    pub(super) producer_ssrcs_by_media: BTreeMap<TransportMediaId, Vec<Ssrc>>,
    /// Recently observed producer RIDs, keyed by source media id.
    ///
    /// This is packet-path liveness, not signaling truth. It decides when a
    /// strict selected-RID gate can become effective and when a stale selected
    /// gate must go pending again.
    pub(super) live_producer_rids: BTreeMap<TransportMediaId, Vec<ProducerRidLiveness>>,
    /// Delayed selected-RID keyframe refreshes owned by the packet loop clock.
    ///
    /// Chrome can need more than one PLI after a RID first becomes live. The
    /// queue is source keyed so retries survive normal packet-loop turns
    /// without adding room-policy work to the hot path.
    pub(super) pending_rid_keyframe_refreshes:
        BTreeMap<TransportMediaId, Vec<PendingRidKeyframeRefresh>>,
    pub(super) pending_rid_keyframe_refresh_queue: BinaryHeap<Reverse<PendingRidKeyframeRefresh>>,
    pub(super) next_rid_keyframe_refresh_id: u64,
    pub(super) rid_readiness_scratch: RidReadinessScratch,
    pub(super) incoming_bitrate_counters: BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
    pub(super) egress_bitrate_counters: BTreeMap<TransportSessionKey, Arc<MediaBitrateCounter>>,
    pub(super) consumer_mid_registry: BTreeMap<ConsumerMidLookupKey, TransportMediaId>,
    pub(super) remote_source_registry: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
    pub(super) relay_targets: BTreeMap<TransportMediaId, RelaySourceRegistration>,
    pub(super) remote_addr_demux: RemoteAddrDemux,
    pub(super) mid_registry: BTreeMap<u64, RegisteredMediaHandle>,
    pub(super) dirty_sessions: BTreeSet<TransportSessionKey>,
    pub(super) session_timeouts: BTreeMap<TransportSessionKey, Instant>,
    pub(super) timeout_queue: BinaryHeap<Reverse<(Instant, TransportSessionKey)>>,
    pub(super) next_media_id: u64,
}

impl RtcBootstrapState {
    pub(super) fn mark_session_dirty(&mut self, session_key: &TransportSessionKey) {
        self.dirty_sessions.insert(session_key.clone());
    }

    pub(super) fn has_dirty_sessions(&self) -> bool {
        !self.dirty_sessions.is_empty()
    }

    pub(super) fn take_ready_sessions(&mut self, now: Instant) -> BTreeSet<TransportSessionKey> {
        let mut ready_sessions = take(&mut self.dirty_sessions);
        while let Some(Reverse((deadline, session_key))) = self.timeout_queue.peek().cloned() {
            let Some(current_deadline) = self.session_timeouts.get(&session_key).copied() else {
                self.timeout_queue.pop();
                continue;
            };
            if current_deadline != deadline {
                self.timeout_queue.pop();
                continue;
            }
            if deadline > now {
                break;
            }
            self.timeout_queue.pop();
            self.session_timeouts.remove(&session_key);
            ready_sessions.insert(session_key);
        }
        ready_sessions
    }

    pub(super) fn update_session_timeout(
        &mut self,
        session_key: &TransportSessionKey,
        next_timeout: Option<Instant>,
    ) {
        self.session_timeouts.remove(session_key);
        if let Some(next_timeout) = next_timeout {
            self.session_timeouts
                .insert(session_key.clone(), next_timeout);
            self.timeout_queue
                .push(Reverse((next_timeout, session_key.clone())));
        }
    }

    pub(super) fn next_timeout_deadline(&mut self) -> Option<Instant> {
        while let Some(Reverse((deadline, session_key))) = self.timeout_queue.peek().cloned() {
            let Some(current_deadline) = self.session_timeouts.get(&session_key).copied() else {
                self.timeout_queue.pop();
                continue;
            };
            if current_deadline != deadline {
                self.timeout_queue.pop();
                continue;
            }
            return Some(deadline);
        }
        None
    }

    pub(super) fn clear_session_schedule(&mut self, session_key: &TransportSessionKey) {
        self.dirty_sessions.remove(session_key);
        self.session_timeouts.remove(session_key);
    }

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

    pub(super) fn forget_live_producer_rids(&mut self, transport_media_id: TransportMediaId) {
        self.live_producer_rids.remove(&transport_media_id);
        self.pending_rid_keyframe_refreshes
            .remove(&transport_media_id);
    }

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

    pub(super) fn drain_due_rid_keyframe_refreshes_for_all(
        &mut self,
        now: Instant,
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

    pub(super) fn next_rid_keyframe_refresh_deadline(&mut self) -> Option<Instant> {
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
}

#[derive(Default)]
pub(super) struct RidReadinessScratch {
    pub(super) ready: Vec<Rid>,
    pub(super) stale: Vec<Rid>,
    pub(super) pending_selected: Vec<Rid>,
}

impl RidReadinessScratch {
    pub(super) fn clear(&mut self) {
        self.ready.clear();
        self.stale.clear();
        self.pending_selected.clear();
    }
}

/// Packet-path readiness for one producer RID.
///
/// This is intentionally time based. A RID that was live once may go quiet
/// after browser encoder adaptation, so strict gates consult freshness instead
/// of treating the first packet as permanent readiness.
#[derive(Debug, Clone)]
pub(super) struct ProducerRidLiveness {
    rid: Rid,
    last_seen: Instant,
}

impl ProducerRidLiveness {
    fn new(rid: Rid, observed_at: Instant) -> Self {
        Self {
            rid,
            last_seen: observed_at,
        }
    }

    const fn rid(&self) -> Rid {
        self.rid
    }

    fn observe(&mut self, observed_at: Instant) {
        self.last_seen = observed_at;
    }

    fn is_ready(&self, now: Instant, max_age: Duration) -> bool {
        now.duration_since(self.last_seen) <= max_age
    }
}

/// Scheduled keyframe retry for one selected producer RID.
///
/// The retry is tied to the source media id by the surrounding map. It is not a
/// room policy update and it should not outlive source removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingRidKeyframeRefresh {
    id: u64,
    transport_media_id: TransportMediaId,
    rid: Rid,
    request_at: Instant,
}

impl PendingRidKeyframeRefresh {
    fn new(id: u64, transport_media_id: TransportMediaId, rid: Rid, request_at: Instant) -> Self {
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

    const fn request_at(self) -> Instant {
        self.request_at
    }

    fn is_due(self, now: Instant) -> bool {
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

#[derive(Debug, Default)]
pub struct RtcSnapshotState {
    pub(super) remote_addr_demux: RemoteAddrDemux,
    pub(super) live_sessions: BTreeSet<TransportSessionKey>,
    transport_health_by_session: BTreeMap<TransportSessionKey, TransportSessionHealth>,
    receiver_bandwidth_by_session: BTreeMap<TransportSessionKey, Bitrate>,
    packet_loop_lag_ms: u64,
    packet_loop_lag_observed_at: Option<Instant>,
}

impl RtcSnapshotState {
    pub(super) fn add_session(&mut self, session_key: &TransportSessionKey) {
        self.live_sessions.insert(session_key.clone());
    }

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
        self.transport_health_by_session.remove(session_key)
    }

    pub(super) fn set_transport_health(
        &mut self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) -> Option<TransportSessionHealth> {
        self.transport_health_by_session
            .insert(session_key.clone(), health)
    }

    pub fn transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.transport_health_by_session.get(session_key).copied()
    }

    pub(super) fn set_receiver_bandwidth(
        &mut self,
        session_key: &TransportSessionKey,
        estimate: Bitrate,
    ) -> Option<Bitrate> {
        self.receiver_bandwidth_by_session
            .insert(session_key.clone(), estimate)
    }

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

    pub(super) fn set_packet_loop_lag_ms(&mut self, lag_ms: u64, observed_at: Instant) {
        self.packet_loop_lag_ms = lag_ms;
        self.packet_loop_lag_observed_at = Some(observed_at);
    }

    pub fn packet_loop_lag_ms_at(&self, now: Instant) -> u64 {
        match self.packet_loop_lag_observed_at {
            Some(observed_at)
                if now.saturating_duration_since(observed_at) <= PACKET_LOOP_LAG_SAMPLE_TTL =>
            {
                self.packet_loop_lag_ms
            }
            Some(_) | None => 0,
        }
    }
}
