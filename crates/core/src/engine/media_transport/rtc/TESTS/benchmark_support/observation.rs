use std::{sync::Arc, time::Instant};

use str0m::media::Mid;

use super::super::{
    bitrate::BitrateRegistry,
    packet_loop::{PacketLoopBuffers, record_incoming_stats_for_benchmark},
    state::PacketLoopState,
    test_support::{
        MediaWorkerScenario, reset_packet_resolution,
        sample_forwarded_packet_with_rid_and_audio_activity, sample_forwarded_packet_without_mid,
        test_transport_session_key,
    },
};
use crate::engine::{
    UserId,
    media_transport::{SourcePolicySignal, SourcePolicyUpdateSubscription},
    metrics::{RtcMetricsRecorder, RtpMetricsRecorder, RuntimeMetrics},
};

const INCOMING_OBSERVATION_TURNS: usize = 512;

/// fixed packet-observation fixture for packet-loop ingress benchmarks
///
/// setup registers one producer, one incoming bitrate counter and two reusable
/// RTP packets. the first packet carries MID, RID and audio metadata while the
/// second relies on the SSRC binding learned from the first packet
pub struct IncomingObservationBenchFixture {
    state: PacketLoopState,
    buffers: PacketLoopBuffers,
    source_policy_signal: SourcePolicySignal,
    source_policy_updates: SourcePolicyUpdateSubscription,
    route_metrics: Arc<RtcMetricsRecorder>,
    rtp_metrics: Arc<RtpMetricsRecorder>,
}

impl IncomingObservationBenchFixture {
    #[must_use]
    pub fn mid_rid_then_ssrc() -> Self {
        let source_session = test_transport_session_key(101, 0, 102, UserId::Integer(103));
        let mut state = PacketLoopState::default();
        let mut scenario = MediaWorkerScenario::new(&mut state);
        let src_media = scenario.source(source_session.clone(), Mid::from("cam-up"));

        let now = Instant::now();
        let mut bitrate_registry = BitrateRegistry::default();
        let bitrate_counter =
            bitrate_registry.register_incoming_media(&source_session, src_media, now);
        state.register_incoming_bitrate_counter(src_media, bitrate_counter);

        let metrics = RuntimeMetrics::default();
        let route_metrics = metrics.register_rtc_worker();
        let rtp_metrics = metrics.register_rtp_worker();
        let source_policy_signal = SourcePolicySignal::default();
        let source_policy_updates = source_policy_signal.subscribe();
        let mut buffers = PacketLoopBuffers::new();
        buffers
            .pending_packets
            .push(sample_forwarded_packet_with_rid_and_audio_activity(
                source_session.clone(),
                "cam-up",
                Some("hi"),
                Some(true),
                Some(-24),
                b"observed-payload",
            ));
        buffers
            .pending_packets
            .push(sample_forwarded_packet_without_mid(
                source_session,
                4321,
                b"steady-payload",
            ));

        Self {
            state,
            buffers,
            source_policy_signal,
            source_policy_updates,
            route_metrics,
            rtp_metrics,
        }
    }

    #[must_use]
    pub fn observe_turns(&mut self) -> usize {
        for _ in 0..INCOMING_OBSERVATION_TURNS {
            for packet in &mut self.buffers.pending_packets {
                reset_packet_resolution(packet);
            }
            record_incoming_stats_for_benchmark(
                &mut self.state,
                &self.source_policy_signal,
                &self.route_metrics,
                &self.rtp_metrics,
                &mut self.buffers,
            );
        }
        self.source_policy_updates.take_pending_updates().len()
    }
}
