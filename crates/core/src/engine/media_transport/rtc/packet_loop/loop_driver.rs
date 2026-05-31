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
    io::{Error as IoError, ErrorKind},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tracing::warn;

use super::{
    super::{
        bitrate::BitrateRegistry,
        forwarded_packet::ForwardedPacket,
        forwarding_planner::populate_forward_routes_for_packet,
        routing_miss::DemuxRecoveryState,
        state::{PacketLoopState, RtcSnapshotState},
        worker::{WorkerCommandContext, drain_due_rid_keyframe_refreshes},
    },
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers, RECEIVE_BUFFER_LEN},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::route_packet_to_matching_session,
    input::{PacketLoopControlInput, PacketLoopInputReceivers, PacketLoopMailboxInput},
    keyframe_requests::flush_pending_keyframe_requests,
    lag::{PacketLoopLagPublisher, PacketLoopLagSnapshot},
    session_drain::{SessionDrainContext, drain_ready_sessions},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
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
pub(in crate::engine::media_transport::rtc) struct PacketLoopConfig {
    pub public_ip: IpAddr,
    pub max_bitrate_in: Bitrate,
    pub max_bitrate_out: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
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
/// the driver can then await with only a cloned socket, the candidate address
/// needed to route the next datagram, the next internal wakeup deadline and the
/// turn start time used for lag accounting
///
/// keeping this type narrow makes it visible when a future change tries to
/// carry session state, demux recovery state or reusable buffers across `.await`
pub(super) struct WaitPhaseSnapshot {
    pub socket: Arc<UdpSocket>,
    pub candidate_addr: SocketAddr,
    pub next_timeout: Option<Instant>,
    pub turn_started_at: Instant,
}

pub(super) enum PacketLoopTurnInput {
    Timeout,
    Control(PacketLoopControlInput),
    RelayPacket,
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
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
    pub receive_buffer: &'a mut [u8],
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
    /// it returns only the cloned socket and next deadline needed after that borrow
    /// ends
    pub fn pump(
        &mut self,
        state: &mut PacketLoopState,
        snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
        config: &PacketLoopConfig,
        relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    ) -> Option<WaitPhaseSnapshot> {
        let turn_started_at = Instant::now();
        self.buffers.clear();
        let (socket, candidate_addr) = {
            // no socket means no RTC session has reached transport bootstrap yet
            // the worker should wait only for control or relay input
            let shared_socket = state.shared_socket.as_ref()?;
            (
                Arc::clone(&shared_socket.socket),
                shared_socket.candidate_addr,
            )
        };
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
            socket: &socket,
        };
        drain_ready_sessions(state, &session_drain_context, &mut self.buffers, now);
        drain_relay_packets(
            relay_rx,
            &mut self.buffers.pending_packets,
            MAX_RELAY_PACKETS_PER_ITERATION,
            &config.rtc_metrics,
        );
        state.flush_pending_remote_source_packet_gates();
        drain_due_rid_keyframe_refreshes(state, &*config.rtc_metrics, now);
        flush_pending_keyframe_requests(state, &*config.rtc_metrics, &mut self.buffers);
        // packet observations must run before fanout planning because layer gates
        // and first-ingress keyframes depend on facts learned from this batch
        record_incoming_stats(
            state,
            &config.source_policy_signal,
            &*config.rtc_metrics,
            &config.rtp_metrics,
            &mut self.buffers,
        );
        // sink routes are refreshed once per turn so recording lookups do not take
        // the shared registry lock per packet
        self.packet_sink_cache
            .refresh_from(&config.packet_sink_registry);
        // planning is separated from flushing so all destinations for the batch are
        // known before any send mutates local session state
        for (packet_idx, packet) in self.buffers.pending_packets.iter_mut().enumerate() {
            populate_forward_routes_for_packet(
                state,
                &self.packet_sink_cache,
                &*config.rtc_metrics,
                packet_idx,
                packet,
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
            socket,
            candidate_addr,
            next_timeout: next_timeout_deadline_at(state, now),
            turn_started_at,
        })
    }

    /// flushes the async outputs produced by the pump phase
    ///
    /// fallback UDP transmits and lag publication run after mutable worker-state
    /// access ends
    async fn flush_outputs(
        &mut self,
        snapshot: Option<&WaitPhaseSnapshot>,
        packet_loop_lag: &PacketLoopLagSnapshot,
    ) {
        let Some(info) = snapshot else {
            return;
        };
        self.flush_staged_transmits(&info.socket).await;
        self.record_packet_loop_lag(packet_loop_lag, info.turn_started_at);
    }

    /// waits for the next event that should resume the worker loop
    ///
    /// shutdown and control input are biased ahead of socket receive
    /// when no socket has been opened yet, only mailbox input or shutdown can wake
    /// the worker
    pub async fn wait_for_next_input(
        &mut self,
        snapshot: Option<WaitPhaseSnapshot>,
        inputs: &mut PacketLoopInputReceivers,
        receive_buffer: &mut [u8],
    ) -> Option<PacketLoopTurnInput> {
        if inputs.shutdown_cancelled() {
            return None;
        }
        if let Some(input) = inputs.try_recv_control() {
            return Some(PacketLoopTurnInput::Control(input));
        }

        let Some(info) = snapshot else {
            // without a socket there is no media path to poll
            // the worker waits for lifecycle input that can create a session
            return inputs.recv_control_or_relay().await.map(mailbox_to_input);
        };
        if let Some(next_timeout) = info.next_timeout
            && next_timeout <= Instant::now()
            && self.ready_now_budget > 0
        {
            // timeout wakes keep due internal work moving but the budget prevents an
            // expired deadline from starving mailbox or socket input forever
            self.ready_now_budget = self.ready_now_budget.saturating_sub(1);
            return Some(PacketLoopTurnInput::Timeout);
        }
        // after one awaited datagram wakes the loop, drain a bounded number of
        // queued datagrams with `try_recv_from` so socket bursts do not pay one
        // await per packet
        if let Some(next_input) = self.try_recv_queued_datagram(&info, receive_buffer) {
            return Some(next_input);
        }
        // mailbox input is biased so shutdown and lifecycle commands stay
        // responsive even while media traffic is heavy
        tokio::select! {
            biased;
            next_input = inputs.recv_control_or_relay() => next_input.map(mailbox_to_input),
            next_input = self.wait_for_socket_input(&info, receive_buffer) => Some(next_input),
        }
    }

    /// applies the event that woke the worker after the pump phase
    ///
    /// control inputs mutate authoritative worker state and conservatively
    /// invalidate ingress demux recovery hints
    /// queued UDP datagrams can resume following turns without another socket await
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
            self.reset_ready_now_budget();
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
            PacketLoopTurnInput::Datagram {
                source_addr,
                candidate_addr,
                received_size,
            } => {
                route_received_datagram(context, source_addr, candidate_addr, received_size);
            }
        }
    }

    /// flushes UDP transmits that could not be sent with `try_send_to`
    ///
    /// `str0m` transmit bytes are copied into reusable slots only when the
    /// immediate socket send reports `WouldBlock`
    /// the async fallback happens after the pump phase so no borrow of
    /// `PacketLoopState` crosses `.await`
    async fn flush_staged_transmits(&self, socket: &UdpSocket) {
        for pending_transmit in self.buffers.pending_transmits() {
            // send errors are logged and dropped because `str0m` will drive future
            // retransmit or timeout behavior through later polls
            if socket
                .send_to(
                    pending_transmit.contents.as_slice(),
                    pending_transmit.destination,
                )
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
        info: &WaitPhaseSnapshot,
        receive_buffer: &mut [u8],
    ) -> Option<PacketLoopTurnInput> {
        if self.udp_burst_budget == 0 {
            return None;
        }
        match info.socket.try_recv_from(receive_buffer) {
            Ok((received_size, source_addr)) => {
                // each datagram gets its own following pump turn, so only the input
                // budget is decremented here
                self.udp_burst_budget = self.udp_burst_budget.saturating_sub(1);
                Some(PacketLoopTurnInput::Datagram {
                    source_addr,
                    candidate_addr: info.candidate_addr,
                    received_size,
                })
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                // no queued datagram means the next awaited receive starts a fresh
                // burst budget
                self.udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN;
                None
            }
            Err(_error) => {
                // receive errors should not end the worker
                // an immediate turn lets timeouts and command handling continue
                warn!("rtc packet loop failed to receive datagram");
                Some(PacketLoopTurnInput::Timeout)
            }
        }
    }

    /// waits for either socket input or the next internal timeout
    ///
    /// the socket receive future borrows only the fixed receive buffer and cloned
    /// socket handle from the wait snapshot
    async fn wait_for_socket_input(
        &mut self,
        info: &WaitPhaseSnapshot,
        receive_buffer: &mut [u8],
    ) -> PacketLoopTurnInput {
        let receive = info.socket.recv_from(receive_buffer);
        let result = if let Some(next_timeout) = info.next_timeout {
            // an exhausted timeout budget converts an already-due deadline into a
            // minimum socket wait through `socket_wait_duration`
            // that gives the biased `select!` a real executor yield before the
            // budget is replenished
            match timeout(socket_wait_duration(next_timeout), receive).await {
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
        self.handle_socket_receive_result(result, info.candidate_addr)
    }

    /// keeps socket receive errors recoverable
    ///
    /// a receive error becomes a timeout wake so the worker can continue handling
    /// control input and future timeouts
    fn handle_socket_receive_result(
        &mut self,
        result: Result<(usize, SocketAddr), IoError>,
        candidate_addr: SocketAddr,
    ) -> PacketLoopTurnInput {
        match result {
            Ok((received_size, source_addr)) => {
                // one datagram is already consumed from the new burst
                self.udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN.saturating_sub(1);
                PacketLoopTurnInput::Datagram {
                    source_addr,
                    candidate_addr,
                    received_size,
                }
            }
            Err(_error) => {
                warn!("rtc packet loop failed to receive datagram");
                PacketLoopTurnInput::Timeout
            }
        }
    }

    /// updates lag through the coalescing publisher
    ///
    /// this avoids writing the shared atomic snapshot on every loop turn
    fn record_packet_loop_lag(
        &mut self,
        snapshot: &PacketLoopLagSnapshot,
        turn_started_at: Instant,
    ) {
        self.lag_publisher
            .observe(snapshot, turn_started_at, Instant::now());
    }

    fn reset_ready_now_budget(&mut self) {
        self.ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
    }
}

const MAX_UDP_DATAGRAMS_PER_TURN: usize = 16;
const MAX_READY_NOW_INPUTS_BEFORE_YIELD: usize = 32;

/// runs the worker-local media packet loop until shutdown or command-channel close
///
/// # Concurrency
///
/// this task owns `PacketLoopState`, demux recovery hints, the UDP receive
/// buffer and `PacketLoopBuffers`
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
pub(in crate::engine::media_transport::rtc) async fn run_packet_loop(
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
    // datagram bytes are kept in a fixed buffer so socket receive does not
    // allocate while media is flowing
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
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
                    receive_buffer: &mut receive_buffer,
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

        turn.flush_outputs(snapshot.as_ref(), &config.packet_loop_lag)
            .await;

        let Some(input) = turn
            .wait_for_next_input(snapshot, &mut inputs, &mut receive_buffer)
            .await
        else {
            // shutdown fired or the command receiver closed
            // both cases end the worker rather than spinning on media
            return;
        };
        next_input = Some(input);
    }
}

/// guards the fixed receive buffer before demux can mutate a session
///
/// `UdpSocket::recv_from` should only report lengths inside
/// `RECEIVE_BUFFER_LEN`
/// the guard keeps malformed test input from slicing past the receive buffer
fn route_received_datagram(
    context: &mut PacketLoopApplyContext<'_>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    received_size: usize,
) {
    let Some(packet) = context.receive_buffer.get(..received_size) else {
        return;
    };
    // ingress routing owns demux recovery and calls `Rtc::accepts()` before a
    // packet can mutate a session
    route_packet_to_matching_session(
        context.packet_loop_state,
        context.snapshot_state,
        context.demux,
        &context.config.rtc_metrics,
        source_addr,
        candidate_addr,
        packet,
    );
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
/// next delayed selected-RID keyframe refresh
#[cfg(test)]
pub(super) fn next_timeout_deadline(state: &mut PacketLoopState) -> Option<Instant> {
    next_timeout_deadline_at(state, Instant::now())
}

fn next_timeout_deadline_at(state: &mut PacketLoopState, now: Instant) -> Option<Instant> {
    if state.has_dirty_sessions() {
        return Some(now);
    }
    match (
        state.next_timeout_deadline(),
        state.next_rid_keyframe_refresh_deadline(),
    ) {
        (Some(session_deadline), Some(refresh_deadline)) => {
            Some(session_deadline.min(refresh_deadline))
        }
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
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
fn socket_wait_duration(next_timeout: Instant) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(Instant::now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
