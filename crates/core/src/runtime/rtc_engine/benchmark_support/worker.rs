//! whole-worker fixtures for manual packet-loop profiling
//!
//! the fixtures in this file are intentionally heavier than the slice fixtures
//! they own a real current-thread `RtcWorker` so the benchmark can include
//! mailbox scheduling and worker command handling after setup
//!
//! callers should create fixtures in benchmark setup only
//! measured methods expose repeatable command work without allocating new
//! transport state or starting sockets

use std::{net::IpAddr, sync::Arc};

use tokio::runtime::{Builder, Runtime};

use super::super::{RtcWorker, test_support::test_transport_session_key};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    VideoBitrateLimits,
    runtime::{
        UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{
            MediaTransportConfig, MediaTransportDeps, SourcePolicySignal, TransportSessionKey,
        },
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

/// fixed command count for one worker investigation sample
///
/// this keeps the benchmark id and the measured work aligned so artifacts can
/// be compared across manual runs without reading the fixture code
pub const WORKER_COMMAND_ROUNDTRIPS: usize = 128;

/// current-thread packet-loop fixture for whole-worker investigation benchmarks
///
/// setup builds a real `RtcWorker`, starts its lazy packet-loop task on a
/// current-thread Tokio runtime and warms one bootstrap session before the
/// measured function runs
/// the measured path sends read-only worker commands through the real mailbox
/// so Callgrind sees packet-loop scheduling, command drain and response
/// delivery without counting fixture construction
pub struct WorkerLoopBenchFixture {
    runtime: Runtime,
    worker: RtcWorker,
    session_key: TransportSessionKey,
}

impl WorkerLoopBenchFixture {
    /// builds and warms one command-driven current-thread worker fixture
    ///
    /// # Panics
    ///
    /// panics when the benchmark runtime cannot be created or when the worker
    /// cannot create its bootstrap offer
    #[must_use]
    #[allow(
        clippy::panic,
        reason = "benchmark setup must fail loudly when the current-thread worker cannot boot"
    )]
    pub fn command_driven_current_thread() -> Self {
        let Ok(runtime) = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
        else {
            panic!("failed to build current-thread benchmark runtime")
        };
        let session_key = test_transport_session_key(91, 0, 92, UserId::Integer(93));
        let fixture = Self {
            runtime,
            worker: RtcWorker::new(
                &worker_config(),
                &worker_deps(),
                Arc::new(SourcePolicySignal::default()),
                0,
                0,
            ),
            session_key,
        };
        fixture.bootstrap_worker();
        let _ = fixture.run_command_roundtrips();
        fixture
    }

    /// sends read-only commands through the worker mailbox
    ///
    /// the method is the measured body used by `packet_loop_worker_callgrind`
    /// it assumes `command_driven_current_thread` already booted and warmed the
    /// worker
    ///
    /// it blocks the fixture runtime until each mailbox response arrives, so
    /// callers should keep it inside benchmark code rather than production tests
    #[must_use]
    pub fn run_command_roundtrips(&self) -> usize {
        self.runtime.block_on(async {
            let mut observed_sources = 0;
            for _ in 0..WORKER_COMMAND_ROUNDTRIPS {
                observed_sources += self.worker.active_speaker_source_snapshot().await.len();
            }
            observed_sources
        })
    }

    #[allow(
        clippy::panic,
        reason = "benchmark setup must fail loudly when the worker cannot create its bootstrap offer"
    )]
    fn bootstrap_worker(&self) {
        let result = self
            .runtime
            .block_on(self.worker.create_initial_session_offer(&self.session_key));
        assert!(
            result.is_ok(),
            "failed to bootstrap current-thread benchmark worker"
        );
    }
}

impl Drop for WorkerLoopBenchFixture {
    fn drop(&mut self) {
        let _ = self
            .runtime
            .block_on(self.worker.close_session_with_outcome(&self.session_key));
    }
}

fn worker_config() -> MediaTransportConfig {
    MediaTransportConfig {
        public_ip: IpAddr::from([127, 0, 0, 1]),
        bitrate_limits: SessionBitrateLimits::new(Bitrate::from_mbps(8), Bitrate::from_mbps(10)),
        video_bitrate_limits: VideoBitrateLimits::default(),
        rtc_port_range: RtcPortRange::new(46_200, 46_220),
        codec_flags: MediaCodecFlags::default(),
        codec_preferences: CodecPreferences::default(),
        media_quality_interval: None,
    }
}

fn worker_deps() -> MediaTransportDeps {
    MediaTransportDeps {
        diagnostics: Arc::new(DiagnosticsStore::default()),
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        metrics: Arc::new(RuntimeMetrics::default()),
    }
}
