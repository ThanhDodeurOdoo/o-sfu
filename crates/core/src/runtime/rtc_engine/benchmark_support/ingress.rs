use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::{
    crypto::from_feature_flags,
    ice::{StunMessage, TransId},
};

use super::super::{
    bootstrap,
    observation::{PacketLoopObservations, RtcObservationPublishers},
    packet_loop::{PacketRouteDatagram, route_packet_to_matching_session_at},
    routing_miss::DemuxRecoveryState,
    state::PacketLoopState,
    test_support::test_transport_session_key,
};
use crate::{
    Bitrate, MediaCodecFlags,
    runtime::{
        UserId,
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

pub const INGRESS_DEMUX_ATTEMPTS: usize = 256;

const RTP_HEADER_LEN: usize = 12;
const LARGE_RTP_PACKET_LEN: usize = 1200;

enum IngressRoutingMode {
    CachedAccepted,
    UnknownSourceMiss,
}

/// fixed UDP ingress fixture for packet-loop demux benchmarks
///
/// cached mode drives a pinned source tuple through `Rtc::accepts()` and
/// `Rtc::handle_input()`
/// unknown-source mode primes one recent miss before measurement so repeated
/// defensive traffic exercises the recent-miss cache
pub struct IngressRoutingBenchFixture {
    mode: IngressRoutingMode,
    state: PacketLoopState,
    observations: PacketLoopObservations,
    demux: DemuxRecoveryState,
    _metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: Vec<u8>,
    now: Instant,
}

impl IngressRoutingBenchFixture {
    #[must_use]
    pub fn cached_accepted_route() -> Self {
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 46_001));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_000));
        let session_key = test_transport_session_key(61, 0, 62, UserId::Integer(63));
        let mut state = PacketLoopState::default();
        let mut observations = PacketLoopObservations::new(RtcObservationPublishers::new());
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();

        let bootstrap_succeeded = bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &session_key,
            candidate_addr,
            Bitrate::from_mbps(10),
            MediaCodecFlags::default(),
        )
        .is_ok();
        let packet = if bootstrap_succeeded {
            let _ = state
                .remote_addr_demux
                .remember_remote_addr(source_addr, &session_key);
            observations.remember_remote_addr(source_addr, &session_key);
            state
                .users
                .get_mut(&session_key)
                .map(|session_state| session_state.rtc.direct_api().local_ice_credentials())
                .and_then(|local_ice_credentials| {
                    let username = format!("{}:remote-ufrag", local_ice_credentials.ufrag);
                    serialize_stun_message(
                        &StunMessage::binding_request(&username, TransId::new(), true, 1, 1, false),
                        Some(local_ice_credentials.pass.as_bytes()),
                    )
                })
                .unwrap_or_else(|| valid_rtp_packet(1, 11, RTP_HEADER_LEN))
        } else {
            valid_rtp_packet(1, 11, RTP_HEADER_LEN)
        };

        Self {
            mode: IngressRoutingMode::CachedAccepted,
            state,
            observations,
            demux: DemuxRecoveryState::new(),
            _metrics: metrics,
            rtc_metrics,
            source_addr,
            candidate_addr,
            packet,
            now: fixed_now(),
        }
    }

    #[must_use]
    pub fn repeated_unknown_source_miss() -> Self {
        Self::unknown_source_miss(valid_rtp_packet(1, 11, RTP_HEADER_LEN))
    }

    #[must_use]
    pub fn repeated_large_unknown_source_miss() -> Self {
        Self::unknown_source_miss(valid_rtp_packet(1, 11, LARGE_RTP_PACKET_LEN))
    }

    #[must_use]
    fn unknown_source_miss(packet: Vec<u8>) -> Self {
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 46_011));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_010));
        let state = PacketLoopState::default();
        let observations = PacketLoopObservations::new(RtcObservationPublishers::new());
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let mut fixture = Self {
            mode: IngressRoutingMode::UnknownSourceMiss,
            state,
            observations,
            demux: DemuxRecoveryState::new(),
            _metrics: metrics,
            rtc_metrics,
            source_addr,
            candidate_addr,
            packet,
            now: fixed_now(),
        };
        fixture.route_once();
        fixture
    }

    #[must_use]
    pub fn route_datagrams(&mut self) -> usize {
        for _ in 0..INGRESS_DEMUX_ATTEMPTS {
            self.route_once();
        }
        match self.mode {
            IngressRoutingMode::CachedAccepted => usize::from(self.state.has_dirty_sessions()),
            IngressRoutingMode::UnknownSourceMiss => INGRESS_DEMUX_ATTEMPTS,
        }
    }

    fn route_once(&mut self) {
        route_packet_to_matching_session_at(
            &mut self.state,
            &mut self.observations,
            &mut self.demux,
            &self.rtc_metrics,
            PacketRouteDatagram::new(
                self.source_addr,
                self.candidate_addr,
                &self.packet,
                self.now,
            ),
        );
    }
}

fn fixed_now() -> Instant {
    Instant::now() + Duration::from_secs(1)
}

fn valid_rtp_packet(sequence_number: u16, ssrc: u32, packet_len: usize) -> Vec<u8> {
    let sequence_number = sequence_number.to_be_bytes();
    let ssrc = ssrc.to_be_bytes();
    let mut packet = Vec::with_capacity(packet_len);
    packet.extend_from_slice(&[
        0x80,
        96,
        sequence_number[0],
        sequence_number[1],
        0,
        0,
        0,
        1,
        ssrc[0],
        ssrc[1],
        ssrc[2],
        ssrc[3],
    ]);
    for byte_index in packet.len()..packet_len {
        let mixed = byte_index
            .wrapping_mul(31)
            .wrapping_add(byte_index.rotate_left(5))
            .wrapping_add(17);
        packet.push(u8::try_from(mixed & 0xff).unwrap_or(0));
    }
    packet
}

fn serialize_stun_message(message: &StunMessage<'_>, password: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut buffer = [0_u8; 1024];
    let crypto_provider = from_feature_flags();
    let sha1_hmac = |key: &[u8], payloads: &[&[u8]]| {
        crypto_provider.sha1_hmac_provider.sha1_hmac(key, payloads)
    };
    let len = message.to_bytes(password, &mut buffer, sha1_hmac).ok()?;
    buffer.get(..len).map(<[u8]>::to_vec)
}
