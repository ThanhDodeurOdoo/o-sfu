//! async driver for one worker-local packet loop
//!
//! this module contains the task that ties the RTC worker together
//! it is the only packet-loop file that awaits socket I/O or worker-channel
//! input
//! the rest of the packet-loop modules are synchronous helpers that run while
//! the worker owns mutable access to `PacketLoopState`
//!
//! the driver preserves the worker ordering contract:
//!
//! - apply one pending control, timeout or UDP datagram input
//! - pump dirty or timed-out sessions and bounded relay packets
//! - flush all staged UDP transmits outside any shared-state lock
//! - wait for the next shutdown, command, relay, timeout or UDP datagram
//! - carry that single input into the next turn
//!
//! shared observable state is updated through narrow side channels
//! the packet loop owns authoritative media state while snapshots, metrics,
//! diagnostics, packet sinks, relay target state and source-policy signals are
//! outputs or configuration dependencies

use std::{
    mem::take,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{sync::mpsc, time::timeout};
use tracing::warn;

use super::{
    super::{
        bitrate::BitrateRegistry,
        forwarded_packet::ForwardedPacket,
        forwarding_planner::plan_forwards,
        routing_miss::DemuxRecoveryState,
        state::{PacketLoopState, RtcSnapshotState},
        worker::{WorkerCommandContext, drain_due_rid_kf_refreshes},
    },
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at},
    input::{PacketLoopControlInput, PacketLoopInputReceivers, PacketLoopMailboxInput},
    keyframe_requests::{drain_due_kf_retries, flush_pending_kf_reqs},
    lag::{PacketLoopLagPublisher, PacketLoopLagSnapshot},
    session_drain::{SessionDrainContext, drain_ready_sessions},
    udp::{RtcUdpSocket, UdpDatagram, UdpIngress},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend, VideoBitrateLimits,
    engine::{
        diagnostics::DiagnosticsStore,
        media_transport::SourcePolicySignal,
        metrics::{RtcMetricsRecorder, RtpMetricsRecorder, RuntimeMetrics},
        packet_sink_registry::{PacketSinkRouteCache, RoomPacketSinkRegistry},
    },
};

/// immutable configuration and shared side channels for one packet-loop worker
///
/// the config is built by `RtcWorker` when the worker is booted
/// values copied into session creation are immutable worker settings
/// `Arc` fields are shared services that the packet loop may update or query
/// without exposing direct access to `PacketLoopState`
pub struct PacketLoopConfig {
    pub public_ip: IpAddr,
    pub max_bitrate_in: Bitrate,
    pub max_bitrate_out: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub rtc_udp_io_backend: RtcUdpIoBackend,
    pub codec_flags: MediaCodecFlags,
    pub codec_preferences: CodecPreferences,
    pub media_quality_interval: Option<Duration>,
    /// first transport media id allocated by this worker
    ///
    /// media ids are worker-local counters once the loop is running
    /// the values must be unique across workers because cross-worker relay state
    /// is keyed by the producing media id
    /// the worker manager assigns disjoint ranges before boot so per-packet
    /// routing does not need to carry a wider key
    pub media_id_base: u64,
    pub diagnostics: Arc<DiagnosticsStore>,
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub source_policy_signal: Arc<SourcePolicySignal>,
    pub metrics: Arc<RuntimeMetrics>,
    pub rtp_metrics: Arc<RtpMetricsRecorder>,
    pub rtc_metrics: Arc<RtcMetricsRecorder>,
    pub packet_loop_lag: Arc<PacketLoopLagSnapshot>,
}

/// packet-loop info allowed to cross into the async wait phase
///
/// `PacketLoopTurn::pump` builds this after mutable `PacketLoopState` access is
/// finished
/// the driver can then await with only the next internal wakeup deadline and the
/// turn start time used for lag accounting
///
/// keeping this type narrow makes it visible when a future change tries to
/// carry session state or demux recovery state across `.await`
pub(super) struct WaitPhaseSnapshot {
    pub next_timeout: Option<Instant>,
    pub turn_started_at: Instant,
}

pub(super) enum PacketLoopTurnInput {
    Timeout,
    Control(PacketLoopControlInput),
    RelayPacket,
    Datagram(UdpDatagram),
}

/// private contract for one packet-loop turn
///
/// the turn owns the allocation surface, fairness budgets and staged relay input
/// used by the driver
/// durable RTC state stays in `PacketLoopState` while async waits happen only
/// after the turn has released mutable worker-state borrows
pub(super) struct PacketLoopTurn {
    buffers: PacketLoopBuffers,
    packet_sink_cache: PacketSinkRouteCache,
    staged_relay_packet: Option<ForwardedPacket>,
    lag_publisher: PacketLoopLagPublisher,
    ready_now_budget: usize,
    udp_burst_budget: usize,
}

/// borrowed state needed to apply the input selected by the wait phase
///
/// grouping these borrows keeps the ingress function signatures small while
/// making it clear that no await happens while the context exists
pub(super) struct PacketLoopApplyContext<'a> {
    pub packet_loop_state: &'a mut PacketLoopState,
    pub bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    pub snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub config: &'a PacketLoopConfig,
    pub demux: &'a mut DemuxRecoveryState,
    pub inputs: &'a mut PacketLoopInputReceivers,
}

impl PacketLoopTurn {
    pub fn new(started_at: Instant) -> Self {
        Self {
            buffers: PacketLoopBuffers::new(),
            packet_sink_cache: PacketSinkRouteCache::default(),
            staged_relay_packet: None,
            lag_publisher: PacketLoopLagPublisher::new(started_at),
            ready_now_budget: MAX_READY_NOW_INPUTS_BEFORE_YIELD,
            udp_burst_budget: MAX_UDP_DATAGRAMS_PER_TURN,
        }
    }

    /// runs the synchronous work for one packet-loop turn
    ///
    /// this method is non-async because it holds mutable worker
    /// state
    /// it returns only the next deadline needed after that borrow ends
    pub fn pump(
        &mut self,
        state: &mut PacketLoopState,
        snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
        config: &PacketLoopConfig,
        relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    ) -> Option<WaitPhaseSnapshot> {
        let turn_started_at = Instant::now();
        self.buffers.clear();
        state.shared_socket.as_ref()?;
        if let Some(packet) = self.staged_relay_packet.take() {
            // the packet that woke the wait phase enters the same batch as packets
            // drained from the relay mailbox below
            self.buffers.pending_packets.push(packet);
        }
        let now = turn_started_at;
        let session_drain_context = SessionDrainContext {
            snapshot_state,
            diagnostics: &config.diagnostics,
            metrics: &config.metrics,
            source_policy_signal: &config.source_policy_signal,
        };
        drain_ready_sessions(state, &session_drain_context, &mut self.buffers, now);
        drain_relay_packets(
            relay_rx,
            &mut self.buffers.pending_packets,
            MAX_RELAY_PACKETS_PER_ITERATION,
            &config.rtc_metrics,
        );
        state.routes.flush_remote_pkt_gates();
        drain_due_rid_kf_refreshes(state, &*config.rtc_metrics, now);
        flush_pending_kf_reqs(state, &*config.rtc_metrics, &mut self.buffers);
        // packet observations must run before fanout planning because layer gates
        // and first-ingress keyframes depend on facts learned from this batch
        record_incoming_stats(
            state,
            &config.source_policy_signal,
            &*config.rtc_metrics,
            &config.rtp_metrics,
            &mut self.buffers,
        );
        drain_due_kf_retries(state, &*config.rtc_metrics, &mut self.buffers, now);
        // sink routes are refreshed once per turn so recording lookups do not take
        // the shared registry lock per packet
        self.packet_sink_cache
            .refresh_from(&config.packet_sink_registry);
        // planning is separated from flushing so all destinations for the batch are
        // known before any send mutates local session state
        for (pkt_idx, pkt) in self.buffers.pending_packets.iter_mut().enumerate() {
            plan_forwards(
                state,
                &self.packet_sink_cache,
                &*config.rtc_metrics,
                pkt_idx,
                pkt,
                &mut self.buffers.forwards,
            );
        }
        // flushing executes local RTC sends, relay sends and packet-sink fanout
        // planned destination order preserves payload reuse opportunities
        flush_forward_routes(
            state,
            &config.metrics,
            &config.rtp_metrics,
            &config.rtc_metrics,
            &self.buffers,
        );
        Some(WaitPhaseSnapshot {
            next_timeout: next_timeout_deadline_at(state, now),
            turn_started_at,
        })
    }

    /// flushes the async outputs produced by the pump phase
    ///
    /// UDP transmits and lag publication run after mutable worker-state access
    /// ends
    async fn flush_outputs(
        &mut self,
        snapshot: Option<&WaitPhaseSnapshot>,
        socket: Option<&RtcUdpSocket>,
        packet_loop_lag: &PacketLoopLagSnapshot,
    ) {
        let (Some(info), Some(socket)) = (snapshot, socket) else {
            return;
        };
        self.flush_staged_transmits(socket).await;
        self.lag_publisher
            .observe(packet_loop_lag, info.turn_started_at, Instant::now());
    }

    /// waits for the next event that should resume the worker loop
    ///
    /// shutdown and control input are biased ahead of ingress receive
    /// when no socket has been opened yet, only mailbox input or shutdown can wake
    /// the worker
    pub async fn wait_for_next_input(
        &mut self,
        snapshot: Option<WaitPhaseSnapshot>,
        ingress: Option<&mut UdpIngress>,
        inputs: &mut PacketLoopInputReceivers,
    ) -> Option<PacketLoopTurnInput> {
        if inputs.shutdown_cancelled() {
            return None;
        }
        if let Some(input) = inputs.try_recv_control() {
            return Some(PacketLoopTurnInput::Control(input));
        }

        let (Some(info), Some(ingress)) = (snapshot, ingress) else {
            // without a socket there is no media path to poll
            // the worker waits for lifecycle input that can create a session
            return inputs.recv_control_or_relay().await.map(mailbox_to_input);
        };
        if let Some(next_timeout) = info.next_timeout
            && next_timeout <= Instant::now()
            && self.ready_now_budget > 0
        {
            // timeout wakes keep due internal work moving but the budget prevents an
            // expired deadline from starving mailbox or ingress input forever
            self.ready_now_budget = self.ready_now_budget.saturating_sub(1);
            return Some(PacketLoopTurnInput::Timeout);
        }
        // after one awaited datagram wakes the loop, consume a bounded number of
        // already completed datagrams so bursts do not pay one ingress await per packet
        if let Some(next_input) = self.try_recv_queued_datagram(ingress) {
            return Some(next_input);
        }
        // mailbox input is biased so shutdown and lifecycle commands stay
        // responsive even while media traffic is heavy
        tokio::select! {
            biased;
            next_input = inputs.recv_control_or_relay() => next_input.map(mailbox_to_input),
            next_input = self.wait_for_ingress_input(&info, ingress) => Some(next_input),
        }
    }

    /// applies the event that woke the worker after the pump phase
    ///
    /// control inputs mutate authoritative worker state and conservatively
    /// invalidate ingress demux recovery hints
    /// queued UDP datagrams can resume following turns without another ingress await
    /// but every datagram still gets a pump between inputs
    /// relay input is staged for the next pump so it reuses the same bounded relay
    /// drain path as already queued relay packets
    pub fn apply_input(
        &mut self,
        context: &mut PacketLoopApplyContext<'_>,
        next_input: PacketLoopTurnInput,
    ) {
        // real external input proves the loop yielded to its environment, so
        // immediate internal wakeups get a fresh fairness budget
        if !matches!(next_input, PacketLoopTurnInput::Timeout) {
            self.ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
        }

        match next_input {
            PacketLoopTurnInput::Timeout => {}
            PacketLoopTurnInput::Control(command) => {
                handle_control_input(
                    context.packet_loop_state,
                    context.bitrate_registry,
                    context.snapshot_state,
                    context.config,
                    command,
                    context.demux,
                );
            }
            PacketLoopTurnInput::RelayPacket => {
                self.staged_relay_packet = context.inputs.take_woken_relay_packet();
            }
            PacketLoopTurnInput::Datagram(datagram) => {
                route_received_datagram(context, datagram);
            }
        }
    }

    /// flushes UDP transmits produced by the pump phase
    async fn flush_staged_transmits(&mut self, socket: &RtcUdpSocket) {
        for pending_transmit in self.buffers.pending_transmits_mut() {
            let packet = take(&mut pending_transmit.contents);
            if socket
                .send_to(packet, pending_transmit.destination)
                .await
                .is_err()
            {
                warn!(
                    destination = %pending_transmit.destination,
                    "failed to send packet-loop transport datagram"
                );
            }
        }
    }

    /// tries to consume one already queued UDP datagram without awaiting
    ///
    /// the burst budget allows short receive bursts while still forcing the worker
    /// back to the biased wait after a bounded number of datagrams
    fn try_recv_queued_datagram(
        &mut self,
        ingress: &mut UdpIngress,
    ) -> Option<PacketLoopTurnInput> {
        if self.udp_burst_budget == 0 {
            return None;
        }
        if let Some(datagram) = ingress.try_recv() {
            self.udp_burst_budget = self.udp_burst_budget.saturating_sub(1);
            Some(PacketLoopTurnInput::Datagram(datagram))
        } else {
            self.udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN;
            None
        }
    }

    /// waits for either completed ingress input or the next internal timeout
    ///
    /// ingress owns the socket receive operation, so cancelling this wait cannot
    /// cancel an in-flight UDP receive
    async fn wait_for_ingress_input(
        &mut self,
        info: &WaitPhaseSnapshot,
        ingress: &mut UdpIngress,
    ) -> PacketLoopTurnInput {
        let receive = ingress.recv();
        let result = if let Some(next_timeout) = info.next_timeout {
            // an exhausted timeout budget converts an already-due deadline into
            // a minimum ingress wait
            // that gives the biased `select!` a real executor yield before the
            // budget is replenished
            match timeout(ingress_wait_duration(next_timeout), receive).await {
                Ok(result) => result,
                Err(_elapsed) => {
                    self.ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
                    return PacketLoopTurnInput::Timeout;
                }
            }
        } else {
            receive.await
        };
        // successful receives reset the burst budget around this first datagram
        // so follow-up queued datagrams can be tried without awaiting again
        match result {
            Some(datagram) => {
                // one datagram is already consumed from the new burst
                self.udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN.saturating_sub(1);
                PacketLoopTurnInput::Datagram(datagram)
            }
            None => PacketLoopTurnInput::Timeout,
        }
    }
}

const MAX_UDP_DATAGRAMS_PER_TURN: usize = 16;
const MAX_READY_NOW_INPUTS_BEFORE_YIELD: usize = 32;

/// runs the worker-local media packet loop until shutdown or command-channel close
///
/// # Concurrency
///
/// this task owns `PacketLoopState`, demux recovery hints, `UdpIngress` and
/// `PacketLoopBuffers`
/// other tasks communicate with it through channels, shared read-side snapshots
/// and cancellation
/// no `MutexGuard` is held across socket sends or receives
///
/// # Hot-path behavior
///
/// the loop batches work in turns
/// a turn may produce many transmits and forwards but it waits for only one
/// next external input before looping
/// relay draining is bounded so a relay burst cannot starve commands or socket
/// ingress indefinitely
pub async fn run_packet_loop(
    config: PacketLoopConfig,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut inputs: PacketLoopInputReceivers,
) {
    // transport media ids must start from the worker-assigned range so relay
    // maps can use the media id alone across workers
    let mut packet_loop_state = PacketLoopState {
        next_media_id: config.media_id_base,
        ..PacketLoopState::default()
    };
    // demux recovery is cached outside durable RTC state because any topology
    // command can invalidate source-address or ICE-fragment ownership
    let mut demux = DemuxRecoveryState::new();
    let mut turn = PacketLoopTurn::new(Instant::now());
    let mut next_input = None;

    loop {
        if let Some(input) = next_input.take() {
            turn.apply_input(
                &mut PacketLoopApplyContext {
                    packet_loop_state: &mut packet_loop_state,
                    bitrate_registry: &bitrate_registry,
                    snapshot_state: &snapshot_state,
                    config: &config,
                    demux: &mut demux,
                    inputs: &mut inputs,
                },
                input,
            );
        }

        let snapshot = turn.pump(
            &mut packet_loop_state,
            &snapshot_state,
            &config,
            inputs.relay_rx(),
        );

        let socket = packet_loop_state
            .shared_socket
            .as_ref()
            .map(|shared_socket| shared_socket.socket.clone());
        turn.flush_outputs(snapshot.as_ref(), socket.as_ref(), &config.packet_loop_lag)
            .await;

        let ingress = packet_loop_state
            .shared_socket
            .as_mut()
            .map(|shared_socket| &mut shared_socket.ingress);
        let input = turn
            .wait_for_next_input(snapshot, ingress, &mut inputs)
            .await;
        let Some(input) = input else {
            // shutdown fired or the command receiver closed
            // both cases end the worker rather than spinning on media
            return;
        };
        next_input = Some(input);
    }
}

fn route_received_datagram(context: &mut PacketLoopApplyContext<'_>, datagram: UdpDatagram) {
    let packet = route_datagram_to_session(
        context.packet_loop_state,
        context.snapshot_state,
        context.demux,
        &context.config.rtc_metrics,
        datagram,
    );
    if let Some(shared_socket) = context.packet_loop_state.shared_socket.as_ref() {
        shared_socket.ingress.recycle(packet);
    }
}

fn route_datagram_to_session(
    packet_loop_state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    demux: &mut DemuxRecoveryState,
    rtc_metrics: &RtcMetricsRecorder,
    datagram: UdpDatagram,
) -> Vec<u8> {
    let UdpDatagram {
        source_addr,
        candidate_addr,
        received_at,
        packet,
    } = datagram;
    route_pkt_to_session_at(
        packet_loop_state,
        snapshot_state,
        demux,
        rtc_metrics,
        PacketRouteDatagram::new(source_addr, candidate_addr, packet.as_slice(), received_at),
    );
    packet
}

#[cfg(feature = "internal-benchmarks")]
pub fn route_queued_ingress_datagrams_for_benchmark(
    packet_loop_state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    demux: &mut DemuxRecoveryState,
    rtc_metrics: &RtcMetricsRecorder,
    ingress: &mut UdpIngress,
    max_datagrams: usize,
) -> usize {
    let mut routed = 0;
    while routed < max_datagrams {
        let Some(datagram) = ingress.try_recv() else {
            break;
        };
        let packet = route_datagram_to_session(
            packet_loop_state,
            snapshot_state,
            demux,
            rtc_metrics,
            datagram,
        );
        ingress.recycle(packet);
        routed += 1;
    }
    routed
}

/// applies control input and forgets cached demux evidence
///
/// this is conservative because control input can change which session owns a
/// source tuple or ICE username fragment
fn handle_control_input(
    packet_loop_state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    command: PacketLoopControlInput,
    demux: &mut DemuxRecoveryState,
) {
    command.dispatch(
        packet_loop_state,
        &WorkerCommandContext {
            bitrate_registry,
            snapshot_state,
            now: Instant::now(),
            public_ip: config.public_ip,
            max_bitrate_in: config.max_bitrate_in,
            max_bitrate_out: config.max_bitrate_out,
            video_bitrate_limits: config.video_bitrate_limits,
            rtc_port_range: config.rtc_port_range,
            rtc_udp_io_backend: config.rtc_udp_io_backend,
            codec_flags: config.codec_flags,
            codec_preferences: config.codec_preferences,
            media_quality_interval: config.media_quality_interval,
            metrics: &config.metrics,
        },
    );
    demux.clear_on_topology_change();
}

/// returns the next time the loop must wake without external input
///
/// dirty sessions are always due immediately because a previous input or local
/// send has queued more `str0m` output
/// otherwise the deadline is the earlier of the next `str0m` timeout and the
/// next delayed selected-RID keyframe refresh or pending keyframe retry
#[cfg(test)]
pub(super) fn next_timeout_deadline(state: &mut PacketLoopState) -> Option<Instant> {
    next_timeout_deadline_at(state, Instant::now())
}

fn next_timeout_deadline_at(state: &mut PacketLoopState, now: Instant) -> Option<Instant> {
    if state.has_dirty_sessions() {
        return Some(now);
    }
    [
        state.next_timeout_deadline(),
        state.routes.next_rid_refresh_deadline(),
        state.routes.next_kf_deadline(),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn mailbox_to_input(input: PacketLoopMailboxInput) -> PacketLoopTurnInput {
    match input {
        PacketLoopMailboxInput::Control(command) => PacketLoopTurnInput::Control(command),
        PacketLoopMailboxInput::Relay => PacketLoopTurnInput::RelayPacket,
    }
}

/// prevents an expired deadline from becoming a spin loop
///
/// a deadline that is already due becomes a one millisecond timeout
/// this gives Tokio a scheduling point before the next timeout turn
fn ingress_wait_duration(next_timeout: Instant) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(Instant::now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
