//! Pure state types and user scheduling for the RTC transport shard.
//!
//! `test_support` owns the transport lifecycle bookkeeping and state mutators
//! that exist only for deterministic adapter tests.

#[cfg(test)]
pub(in crate::runtime::rtc_engine) mod test_support;

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::{
    change::SdpPendingOffer,
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tokio::net::UdpSocket;

use super::{
    bitrate::MediaBitrateCounter, demux::RemoteAddrDemux,
    packet_loop::machine::state::PacketLoopState, session_adapter::RtcHostSession,
};
use crate::runtime::media_transport::{
    ReceiverBandwidthSnapshot, SessionUploadSlot, TransportMediaId, TransportSessionKey,
};
pub use crate::transport::TransportSessionHealth;

const PACKET_LOOP_LAG_SAMPLE_TTL: Duration = Duration::from_secs(1);

pub(super) struct SharedRtcSocket {
    pub(super) socket: Arc<UdpSocket>,
    pub(super) candidate_addr: SocketAddr,
}

pub(super) struct RtcSessionState {
    pub(super) host_session: RtcHostSession,
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
pub struct RtcBootstrapState {
    pub(super) shared_socket: Option<SharedRtcSocket>,
    pub(super) users: BTreeMap<TransportSessionKey, RtcSessionState>,
    pub(super) incoming_bitrate_counters: BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
    pub(super) egress_bitrate_counters: BTreeMap<TransportSessionKey, Arc<MediaBitrateCounter>>,
    pub(super) packet_loop: PacketLoopState,
}

#[derive(Debug, Default)]
pub struct RtcSnapshotState {
    pub(super) remote_addr_demux: RemoteAddrDemux,
    pub(super) live_sessions: BTreeSet<TransportSessionKey>,
    transport_health_by_session: BTreeMap<TransportSessionKey, TransportSessionHealth>,
    receiver_bandwidth_by_session: BTreeMap<TransportSessionKey, u64>,
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
        estimate_bps: u64,
    ) -> Option<u64> {
        self.receiver_bandwidth_by_session
            .insert(session_key.clone(), estimate_bps)
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
                        .map(|estimate_bps| (session_key.clone(), estimate_bps))
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
