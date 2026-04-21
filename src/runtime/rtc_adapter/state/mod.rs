//! Pure state types and session scheduling for the RTC transport adapter.
//!
//! `test_support` owns the transport lifecycle bookkeeping and state mutators
//! that exist only for deterministic adapter tests.

#[cfg(test)]
pub(in crate::runtime::rtc_adapter) mod test_support;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    mem::take,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::Rtc;
use str0m::change::SdpPendingOffer;
use str0m::media::{Mid, Rid};
use str0m::rtp::Ssrc;
use tokio::net::UdpSocket;

use crate::runtime::transport_adapter::{
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};
use o_sfu_router::MediaStream as RouterRtpParameters;

use super::demux::{MediaRouteEntry, MediaRouteKey, RemoteAddrDemux};
use super::media_registry::{
    ConsumerMidLookupKey, ProducerMidLookupKey, ProducerSsrcLookupKey, RegisteredMediaHandle,
    RemoteSourceRegistration,
};
use super::route_control::RouteControlState;

pub(super) const BITRATE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportSessionHealth {
    Connected,
    Disconnected,
}

pub(super) struct SharedRtcSocket {
    pub(super) socket: Arc<UdpSocket>,
    pub(super) candidate_addr: SocketAddr,
}

pub(super) struct RtcSessionState {
    pub(super) rtc: Rtc,
    pub(super) started_at: Instant,
    pub(super) local_ice_ufrag: String,
    #[cfg(test)]
    pub(super) max_bitrate_in_bps: Option<u64>,
    #[cfg(test)]
    pub(super) max_bitrate_out_bps: Option<u64>,
    pub(super) dtls_started: bool,
    pub(super) sdp_negotiation: SessionSdpNegotiationState,
}

#[derive(Default)]
pub(super) struct SessionSdpNegotiationState {
    pub(super) bootstrap_mids: Vec<Mid>,
    pub(super) pending_offer: Option<SdpPendingOffer>,
    pub(super) staged_offer_sdp: Option<String>,
    pub(super) initial_offer_applied: bool,
    pub(super) pending_recv_streams: BTreeMap<Mid, PendingRecvStream>,
    pub(super) negotiated_producer_parameters: BTreeMap<Mid, RouterRtpParameters>,
    pub(super) queued_removal_mids: BTreeSet<Mid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingRecvStream {
    pub(super) ssrc: Ssrc,
    pub(super) rid: Option<Rid>,
}

#[derive(Debug, Default)]
pub(super) struct SessionIncomingBitrates {
    per_media: BTreeMap<TransportMediaId, RecentBitrate>,
}

impl SessionIncomingBitrates {
    pub(super) fn record(
        &mut self,
        transport_media_id: TransportMediaId,
        now: Instant,
        payload_bytes: usize,
    ) -> bool {
        let first_observation = !self.per_media.contains_key(&transport_media_id);
        self.per_media
            .entry(transport_media_id)
            .or_default()
            .record(now, payload_bytes);
        first_observation
    }

    pub(super) fn snapshot(&self, now: Instant) -> Vec<(TransportMediaId, u64)> {
        self.per_media
            .iter()
            .filter_map(|(media_id, bitrate)| {
                let bits = bitrate.snapshot(now);
                if bits > 0 {
                    Some((*media_id, bits))
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn total(&self, now: Instant) -> u64 {
        self.per_media
            .values()
            .map(|bitrate| bitrate.snapshot(now))
            .sum()
    }
}

#[derive(Debug, Clone, Copy)]
struct RecentBitrate {
    window_start: Instant,
    bytes_in_window: u64,
}

impl Default for RecentBitrate {
    fn default() -> Self {
        Self {
            window_start: Instant::now(),
            bytes_in_window: 0,
        }
    }
}

impl RecentBitrate {
    fn record(&mut self, now: Instant, payload_bytes: usize) {
        if now.duration_since(self.window_start) >= BITRATE_WINDOW {
            self.window_start = now;
            self.bytes_in_window = 0;
        }
        self.bytes_in_window = self
            .bytes_in_window
            .saturating_add(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
    }

    fn snapshot(&self, now: Instant) -> u64 {
        if now.duration_since(self.window_start) >= BITRATE_WINDOW {
            0
        } else {
            self.bytes_in_window.saturating_mul(8)
        }
    }
}

#[derive(Default)]
pub(super) struct RtcBootstrapState {
    pub(super) shared_socket: Option<SharedRtcSocket>,
    pub(super) sessions: BTreeMap<TransportSessionKey, RtcSessionState>,
    pub(super) media_route_index: BTreeMap<MediaRouteKey, MediaRouteEntry>,
    pub(super) route_control: RouteControlState,
    pub(super) producer_mid_registry: BTreeMap<ProducerMidLookupKey, TransportMediaId>,
    pub(super) producer_ssrc_registry: BTreeMap<ProducerSsrcLookupKey, TransportMediaId>,
    pub(super) producer_ssrcs_by_media: BTreeMap<TransportMediaId, Vec<Ssrc>>,
    pub(super) consumer_mid_registry: BTreeMap<ConsumerMidLookupKey, TransportMediaId>,
    pub(super) remote_source_registry: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
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
}

#[derive(Debug, Default)]
pub(crate) struct RtcSnapshotState {
    pub(super) remote_addr_demux: RemoteAddrDemux,
    pub(super) live_sessions: BTreeSet<TransportSessionKey>,
    transport_health_by_session: BTreeMap<TransportSessionKey, TransportSessionHealth>,
}

#[derive(Debug, Default)]
pub(crate) struct RtcBitrateState {
    pub(super) incoming_bitrates_by_session: BTreeMap<TransportSessionKey, SessionIncomingBitrates>,
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
        self.remote_addr_demux
            .forget_session_remote_addrs(session_key);
        self.remote_addr_demux
            .forget_session_local_ice_ufrag(session_key);
        self.remote_addr_demux
            .forget_session_remote_candidate_addrs(session_key);
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

    pub(crate) fn transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.transport_health_by_session.get(session_key).copied()
    }
}

impl RtcBitrateState {
    pub(super) fn record_incoming_media(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        now: Instant,
        payload_bytes: usize,
    ) -> bool {
        self.incoming_bitrates_by_session
            .entry(session_key.clone())
            .or_default()
            .record(transport_media_id, now, payload_bytes)
    }

    pub(super) fn remove_session(&mut self, session_key: &TransportSessionKey) {
        self.incoming_bitrates_by_session.remove(session_key);
    }

    pub(crate) fn transport_bitrate_snapshot_at(
        &self,
        session_keys: &[TransportSessionKey],
        now: Instant,
    ) -> TransportBitrateSnapshot {
        let mut snapshot = TransportBitrateSnapshot::default();
        for session_key in session_keys {
            let Some(session_bitrates) = self.incoming_bitrates_by_session.get(session_key) else {
                continue;
            };
            snapshot.total = snapshot.total.saturating_add(session_bitrates.total(now));
            snapshot.per_media.extend(session_bitrates.snapshot(now));
        }
        snapshot
    }
}
