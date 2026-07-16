//! construction and mailbox runtime for one RTC transport worker
//!
//! [`RtcWorker::start`] returns only after its packet-loop runtime has bound the
//! worker socket
//! callers can therefore use one direct handle without a start slot or missing
//! worker state
//!
//! command dispatch follows one pattern:
//!
//! ```text
//! transport command port
//!   |
//!   v
//! build RtcWorkerCommand with oneshot response
//!   |
//!   v
//! send through worker command mailbox
//!   |
//!   v
//! await worker response
//! ```
#[cfg(target_os = "linux")]
use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
};
use std::{
    net::IpAddr,
    sync::{Arc, Mutex, atomic::Ordering, mpsc as std_mpsc},
    thread,
    time::Instant,
};

use tokio::{
    runtime::Builder as TokioRuntimeBuilder,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{
    super::{
        RtcWorkerConfig, RtpProfile,
        bitrate::BitrateRegistry,
        bootstrap,
        commands::{RtcWorkerCommand, RtcWorkerResponse},
        packet_loop::{self, PacketLoopConfig},
        relay_registry::{RELAY_MAILBOX_CAPACITY, RelayPacketMailbox, sender_backlog_depth},
        state::{RtcSnapshotState, TransportSessionHealth},
    },
    RtcWorker, RtcWorkerHandle,
};
use crate::{
    Bitrate, MediaWorkerId, RtcPortRange, RtcUdpIoBackend,
    engine::media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, MediaTransportConfig,
        MediaTransportDeps, ReceiverBandwidthSnapshot, SourcePolicySignal, TransportAdapterError,
        TransportBitrateSnapshot, TransportMediaId, TransportPlacementPressureSnapshot,
        TransportQualitySnapshot, TransportSessionKey, TransportSourceActivitySnapshot,
        TransportWorkerPressureSnapshot,
    },
};

struct PacketLoopStartup {
    announced_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    config: PacketLoopConfig,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    inputs: packet_loop::PacketLoopInputReceivers,
}

impl PacketLoopStartup {
    async fn run(
        self,
        backend: RtcUdpIoBackend,
        startup_tx: std_mpsc::SyncSender<Result<(), TransportAdapterError>>,
    ) {
        let shared_socket = match bootstrap::bind_shared_rtc_socket(
            self.announced_ip,
            self.rtc_port_range,
            backend,
        ) {
            Ok(shared_socket) => shared_socket,
            Err(error) => {
                let _ = startup_tx.send(Err(error));
                return;
            }
        };
        if startup_tx.send(Ok(())).is_err() {
            return;
        }
        packet_loop::run_packet_loop(
            self.config,
            shared_socket,
            self.bitrate_registry,
            self.snapshot_state,
            self.inputs,
        )
        .await;
    }
}

fn spawn_tokio_packet_loop(
    thread_name: String,
    startup: PacketLoopStartup,
) -> Result<thread::JoinHandle<()>, TransportAdapterError> {
    let (startup_tx, startup_rx) = std_mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            match TokioRuntimeBuilder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(runtime) => {
                    runtime.block_on(startup.run(RtcUdpIoBackend::Tokio, startup_tx));
                }
                Err(error) => {
                    warn!(?error, "failed to boot rtc packet loop Tokio runtime");
                    let _ = startup_tx.send(Err(TransportAdapterError::TransportUnavailable));
                }
            }
        })
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    wait_for_packet_loop_startup(thread, &startup_rx)
}

#[cfg(target_os = "linux")]
fn spawn_io_uring_packet_loop(
    thread_name: String,
    startup: PacketLoopStartup,
) -> Result<thread::JoinHandle<()>, TransportAdapterError> {
    let (startup_tx, startup_rx) = std_mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let startup_err_tx = startup_tx.clone();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                tokio_uring::start(startup.run(RtcUdpIoBackend::IoUring, startup_tx));
            })) {
                warn!(
                    panic = panic_message(payload.as_ref()),
                    "rtc packet loop io_uring runtime panicked"
                );
                let _ = startup_err_tx.send(Err(TransportAdapterError::TransportUnavailable));
            }
        })
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    wait_for_packet_loop_startup(thread, &startup_rx)
}

fn wait_for_packet_loop_startup(
    thread: thread::JoinHandle<()>,
    startup_rx: &std_mpsc::Receiver<Result<(), TransportAdapterError>>,
) -> Result<thread::JoinHandle<()>, TransportAdapterError> {
    match startup_rx.recv() {
        Ok(Ok(())) => Ok(thread),
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error)
        }
        Err(_) => {
            let _ = thread.join();
            Err(TransportAdapterError::TransportUnavailable)
        }
    }
}

#[cfg(target_os = "linux")]
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "unknown panic payload"
    }
}

impl Drop for RtcWorker {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl RtcWorker {
    /// starts one packet-loop worker and binds its socket before returning
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when runtime
    /// creation, socket binding or packet-loop thread startup fails
    pub(crate) fn start(
        config: &MediaTransportConfig,
        profile: Arc<RtpProfile>,
        rtc_port_range: RtcPortRange,
        deps: &MediaTransportDeps,
        source_policy_signal: SourcePolicySignal,
        media_id_base: u64,
        media_worker_id: MediaWorkerId,
    ) -> Result<Self, TransportAdapterError> {
        let relay_target_id =
            super::RelayTargetId::new(super::NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed));
        let (command_tx, command_rx) = mpsc::channel(64);
        #[cfg(any(test, feature = "testing-transport"))]
        let debug_channels = super::super::test_support::RtcWorkerDebugChannels::new();
        let (relay_tx, relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
        // observability reads these side channels without entering the packet
        // loop, while authoritative state stays owned by the worker task
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let packet_loop_lag = Arc::new(packet_loop::PacketLoopLagSnapshot::new(Instant::now()));
        let shutdown = CancellationToken::new();
        let handle = RtcWorkerHandle {
            command_tx,
            #[cfg(any(test, feature = "testing-transport"))]
            debug_handle: debug_channels.handle(),
            relay_mailbox: RelayPacketMailbox::new(relay_tx),
            bitrate_registry: Arc::clone(&bitrate_registry),
            snapshot_state: Arc::clone(&snapshot_state),
            packet_loop_lag: Arc::clone(&packet_loop_lag),
        };
        let packet_loop_inputs =
            packet_loop::PacketLoopInputReceivers::new(command_rx, relay_rx, shutdown.clone());
        #[cfg(any(test, feature = "testing-transport"))]
        let packet_loop_inputs = debug_channels.install(packet_loop_inputs);
        let metrics = &deps.metrics;
        let rtc_metrics = metrics.register_rtc_worker();
        #[cfg(test)]
        let test_source_policy_signal = source_policy_signal.clone();
        let packet_loop_config = PacketLoopConfig {
            worker: RtcWorkerConfig {
                bitrate_limits: config.bitrate_limits,
                video_bitrate_limits: config.video_bitrate_limits,
                profile,
                media_quality_interval: config.media_quality_interval,
                media_id_base,
            },
            diagnostics: Arc::clone(&deps.diagnostics),
            packet_sink_registry: Arc::clone(&deps.packet_sink_registry),
            source_policy_signal,
            metrics: Arc::clone(metrics),
            rtp_metrics: metrics.register_rtp_worker_for_media_worker(media_worker_id.as_usize()),
            rtc_metrics: Arc::clone(&rtc_metrics),
            packet_loop_lag,
        };
        let startup = PacketLoopStartup {
            announced_ip: config.announced_ip,
            rtc_port_range,
            config: packet_loop_config,
            bitrate_registry,
            snapshot_state,
            inputs: packet_loop_inputs,
        };
        let thread_name = format!("rtc-packet-loop-{relay_target_id:?}");
        let thread = match config.rtc_udp_io_backend {
            RtcUdpIoBackend::Tokio => spawn_tokio_packet_loop(thread_name, startup)?,
            RtcUdpIoBackend::IoUring => {
                #[cfg(target_os = "linux")]
                {
                    spawn_io_uring_packet_loop(thread_name, startup)?
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(TransportAdapterError::TransportUnavailable);
                }
            }
        };
        info!(
            ?relay_target_id,
            announced_ip = %config.announced_ip,
            max_bitrate_in_bps = config.bitrate_limits.max_bitrate_in().as_bps(),
            max_bitrate_out_bps = config.bitrate_limits.max_bitrate_out().as_bps(),
            rtc_port_range_min = rtc_port_range.min(),
            rtc_port_range_max = rtc_port_range.max(),
            rtc_udp_io_backend = config.rtc_udp_io_backend.wire_name(),
            "started rtc packet loop worker"
        );
        Ok(Self {
            relay_target_id,
            handle,
            shutdown,
            thread: Some(thread),
            #[cfg(any(test, feature = "testing-transport"))]
            metrics: Arc::clone(metrics),
            rtc_metrics,
            #[cfg(test)]
            source_policy_signal: test_source_policy_signal,
        })
    }

    /// sends a request command to the ready worker
    ///
    /// # Errors
    ///
    /// returns the command handler error, or
    /// [`TransportAdapterError::TransportUnavailable`] when command delivery or
    /// response receipt fails
    pub async fn request_worker<T, F>(&self, build_command: F) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
    {
        let (response_tx, response_rx) = oneshot::channel();
        self.handle
            .command_tx
            .send(build_command(response_tx))
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }
}

impl RtcWorker {
    /// reads bitrate counters for the requested sessions
    ///
    /// an unavailable registry or missing session contributes no bitrate
    /// callers must treat the result as a recent transport observation rather
    /// than an accounting source of truth
    pub fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let Ok(bitrate_registry) = self.handle.bitrate_registry.lock() else {
            return TransportBitrateSnapshot::default();
        };
        bitrate_registry.transport_bitrate_snapshot_at(session_keys, Instant::now())
    }

    /// reads receiver bandwidth estimates from the worker snapshot state
    ///
    /// an empty result means the worker has no current estimate or the snapshot
    /// side channel is unavailable
    /// it does not mean the room has no receivers
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return ReceiverBandwidthSnapshot::default();
        };
        snapshot_state.receiver_bandwidth_snapshot(session_keys)
    }

    /// reads sampled media quality from the worker snapshot state
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return TransportQualitySnapshot::default();
        };
        snapshot_state.transport_quality_snapshot(session_keys)
    }

    /// builds a placement-pressure snapshot for selected sessions
    ///
    /// egress bitrate is scoped to the supplied sessions while packet-loop lag
    /// and mailbox backlogs describe the whole worker because those resources
    /// are shared by every session on the packet loop
    pub fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        let now = Instant::now();
        let egress_bitrate = match self.handle.bitrate_registry.lock() {
            Ok(bitrate_registry) => bitrate_registry.egress_bitrate_snapshot_at(session_keys, now),
            Err(_error) => Bitrate::zero(),
        };
        pressure_snapshot(&self.handle, egress_bitrate, now)
    }

    /// builds a pressure snapshot for the whole worker
    ///
    /// this is used by room placement to compare local workers
    /// the result is still best-effort and falls back to zero pressure when the
    /// worker observations are unavailable
    pub fn worker_pressure_snapshot(
        &self,
        media_worker_id: MediaWorkerId,
    ) -> TransportWorkerPressureSnapshot {
        let now = Instant::now();
        let egress_bitrate = match self.handle.bitrate_registry.lock() {
            Ok(bitrate_registry) => bitrate_registry.total_egress_bitrate_snapshot_at(now),
            Err(_error) => Bitrate::zero(),
        };
        TransportWorkerPressureSnapshot::new(
            media_worker_id,
            pressure_snapshot(&self.handle, egress_bitrate, now),
        )
    }

    /// reads the latest transport health side-channel entry for one session
    ///
    /// `None` means the snapshot lock is unavailable or no health event has
    /// been observed for the session
    pub fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return None;
        };
        snapshot_state.transport_health(session_key)
    }

    /// asks the packet loop for its current active-speaker source snapshot
    ///
    /// this command is read-only but still enters the worker mailbox because
    /// the source activity ordering lives beside route-control state
    /// dispatch failures return an empty snapshot
    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.request_worker(|response| RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response })
            .await
            .unwrap_or_default()
    }

    /// asks the packet loop for detailed active-speaker diagnostics
    ///
    /// diagnostics are read through the mailbox for the same ownership reason
    /// as source snapshots
    /// dispatch failures return an empty diagnostic set
    pub async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.request_worker(
            |response| RtcWorkerCommand::ActiveSpeakerDiagnosticSnapshot { response },
        )
        .await
        .unwrap_or_default()
    }

    /// asks the packet loop for packet activity on producer media ids
    ///
    /// diagnostics query this through the mailbox so steady RTP forwarding does
    /// not mirror per-packet source ages into shared state
    pub async fn source_activity_snapshot(
        &self,
        transport_media_ids: &[TransportMediaId],
    ) -> TransportSourceActivitySnapshot {
        self.request_worker(|response| RtcWorkerCommand::SourceActivitySnapshot {
            transport_media_ids: transport_media_ids.to_vec(),
            response,
        })
        .await
        .unwrap_or_default()
    }
}

/// combines command and relay mailbox saturation into one pressure score
///
/// the score is the max of both queues
/// one saturated input path should make placement avoid the worker even if the
/// other path is idle
fn worker_pressure_score(
    command_backlog_depth: usize,
    command_capacity: usize,
    relay_mailbox_depth: usize,
    relay_mailbox_capacity: usize,
) -> u8 {
    backlog_pressure_score(command_backlog_depth, command_capacity).max(backlog_pressure_score(
        relay_mailbox_depth,
        relay_mailbox_capacity,
    ))
}

fn pressure_snapshot(
    worker_handle: &RtcWorkerHandle,
    egress_bitrate: Bitrate,
    now: Instant,
) -> TransportPlacementPressureSnapshot {
    let packet_loop_lag_ms = worker_handle.packet_loop_lag.packet_loop_lag_ms_at(now);
    let command_backlog_depth = sender_backlog_depth(&worker_handle.command_tx);
    let relay_mailbox_depth = worker_handle.relay_mailbox.backlog_depth();
    TransportPlacementPressureSnapshot {
        egress_bitrate,
        packet_loop_lag_ms,
        command_backlog_depth,
        relay_mailbox_depth,
        worker_pressure_score: worker_pressure_score(
            command_backlog_depth,
            worker_handle.command_tx.max_capacity(),
            relay_mailbox_depth,
            RELAY_MAILBOX_CAPACITY,
        ),
    }
}

/// converts one bounded mailbox depth into a percentage pressure score
///
/// zero capacity is treated as no pressure because there is no usable divisor
/// and current Tokio bounded mailboxes always report a positive capacity
fn backlog_pressure_score(backlog_depth: usize, capacity: usize) -> u8 {
    if capacity == 0 {
        return 0;
    }
    let score = backlog_depth.saturating_mul(100) / capacity;
    u8::try_from(score.min(100)).unwrap_or(100)
}
