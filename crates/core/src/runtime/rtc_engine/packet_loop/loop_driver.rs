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
//! - drain already queued commands before touching media
//! - pump dirty or timed-out sessions and bounded relay packets
//! - flush all staged UDP transmits outside any shared-state lock
//! - wait for the next shutdown, command, relay, timeout or UDP datagram
//! - try to route one received datagram into its owning `str0m::Rtc`
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
        routing_miss::PacketLoopRoutingState,
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
    runtime::{
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
pub(in crate::runtime::rtc_engine) struct PacketLoopConfig {
    pub public_ip: IpAddr,
    pub max_bitrate_in: Bitrate,
    pub max_bitrate_out: Bitrate,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub codec_flags: MediaCodecFlags,
    pub codec_preferences: CodecPreferences,
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
/// `snapshot_and_pump` builds this after mutable `PacketLoopState` access is
/// finished
/// the driver can then await with only a cloned socket, the candidate address
/// needed to route the next datagram, the next internal wakeup deadline and the
/// turn start time used for lag accounting
///
/// keeping this type narrow makes it visible when a future change tries to
/// carry session state, routing state or reusable buffers across `.await`
struct WaitPhaseSnapshot {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<Instant>,
    turn_started_at: Instant,
}

enum NextLoopInput {
    ReadyNow,
    Control(PacketLoopControlInput),
    RelayPacket,
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
}

/// reusable state carried across packet-loop turns
///
/// this context owns the allocation surface plus fairness budgets that should
/// survive across turns
/// durable RTC state still stays in `PacketLoopState`
struct PacketLoopTurnContext {
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
struct PacketLoopApplyContext<'a> {
    packet_loop_state: &'a mut PacketLoopState,
    bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    config: &'a PacketLoopConfig,
    routing_state: &'a mut PacketLoopRoutingState,
    inputs: &'a mut PacketLoopInputReceivers,
    receive_buffer: &'a mut [u8],
}

impl PacketLoopTurnContext {
    fn new(started_at: Instant) -> Self {
        Self {
            buffers: PacketLoopBuffers::new(),
            packet_sink_cache: PacketSinkRouteCache::default(),
            staged_relay_packet: None,
            lag_publisher: PacketLoopLagPublisher::new(started_at),
            ready_now_budget: MAX_READY_NOW_INPUTS_BEFORE_YIELD,
            udp_burst_budget: MAX_UDP_DATAGRAMS_PER_TURN,
        }
    }

    fn reset_ready_now_budget(&mut self) {
        self.ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
    }
}

const MAX_CONTROL_INPUTS_PER_TURN: usize = 64;
const MAX_UDP_DATAGRAMS_PER_TURN: usize = 16;
const MAX_READY_NOW_INPUTS_BEFORE_YIELD: usize = 32;

/// runs the worker-local media packet loop until shutdown or command-channel close
///
/// # Concurrency
///
/// this task owns `PacketLoopState`, routing hints, the UDP receive buffer and
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
pub(in crate::runtime::rtc_engine) async fn run_packet_loop(
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
    // routing recovery is cached outside durable RTC state because any topology
    // command can invalidate source-address or ICE-fragment ownership
    let mut routing_state = PacketLoopRoutingState::new();
    // datagram bytes are kept in a fixed buffer so socket receive does not
    // allocate while media is flowing
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    let mut turn = PacketLoopTurnContext::new(Instant::now());

    loop {
        // queued control comes first so close, negotiation and route updates
        // become visible before the worker pumps more media (what we do with them
        // depends on the state of the worker which is controlled by the commands)
        if !drain_queued_control_inputs(
            &mut packet_loop_state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut inputs,
            &mut routing_state,
        ) {
            return;
        }

        let snapshot = snapshot_and_pump(
            &mut packet_loop_state,
            &snapshot_state,
            &config,
            inputs.relay_rx(),
            &mut turn,
        );

        // a wait snapshot exists only after a shared UDP socket has been opened
        // so this turn may have fallback sends and a meaningful lag sample
        if let Some(info) = snapshot.as_ref() {
            // fallback transmits are sent after the state borrow ends because
            // async sends must not hold packet-loop mutable state
            flush_staged_transmits(&info.socket, &turn.buffers).await;
            // lag includes command drain, media pump and fallback async sends
            // because that is the delay visible to the next packet-loop turn
            record_packet_loop_lag(
                &config.packet_loop_lag,
                &mut turn.lag_publisher,
                info.turn_started_at,
            );
        }

        let Some(next_input) = wait_for_next_loop_input(
            snapshot,
            &mut inputs,
            &mut receive_buffer,
            &mut turn.ready_now_budget,
            &mut turn.udp_burst_budget,
        )
        .await
        else {
            // shutdown fired or the command receiver closed
            // both cases end the worker rather than spinning on media
            return;
        };
        // real external input proves the loop yielded to its environment, so
        // immediate internal wakeups get a fresh fairness budget
        if !matches!(next_input, NextLoopInput::ReadyNow) {
            turn.reset_ready_now_budget();
        }

        // applying the selected input is synchronous and prepares state for the
        // next turn
        // no await may happen while this context borrows worker state
        apply_ingress_for_next_turn(
            &mut PacketLoopApplyContext {
                packet_loop_state: &mut packet_loop_state,
                bitrate_registry: &bitrate_registry,
                snapshot_state: &snapshot_state,
                config: &config,
                routing_state: &mut routing_state,
                inputs: &mut inputs,
                receive_buffer: &mut receive_buffer,
            },
            next_input,
            &mut turn.staged_relay_packet,
        );
    }
}

/// flushes UDP transmits that could not be sent with `try_send_to`
///
/// `str0m` transmit bytes are copied into reusable slots only when the
/// immediate socket send reports `WouldBlock`
/// the async fallback happens after the pump phase so no borrow of
/// `PacketLoopState` crosses `.await`
async fn flush_staged_transmits(socket: &UdpSocket, buffers: &PacketLoopBuffers) {
    for pending_transmit in buffers.pending_transmits() {
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

/// drains already queued control inputs before media pumping
///
/// the cap prevents a large command burst from starving media forever
/// returning `false` means shutdown was requested before the worker should do
/// more packet-loop work
fn drain_queued_control_inputs(
    packet_loop_state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    inputs: &mut PacketLoopInputReceivers,
    routing_state: &mut PacketLoopRoutingState,
) -> bool {
    for _ in 0..MAX_CONTROL_INPUTS_PER_TURN {
        // shutdown wins even over already queued commands so drained workers
        // stop promptly
        if inputs.shutdown_cancelled() {
            return false;
        }
        let Some(input) = inputs.try_recv_control() else {
            return true;
        };
        handle_control_input_and_clear_routing_cache(
            packet_loop_state,
            bitrate_registry,
            snapshot_state,
            config,
            input,
            routing_state,
        );
    }
    true
}

/// applies the event that woke the worker after the pump phase
///
/// control inputs mutate authoritative worker state and conservatively
/// invalidate ingress routing hints
/// queued UDP datagrams can resume following turns without another socket await
/// but every datagram still gets a pump between inputs
/// relay input is staged for the next pump so it reuses the same bounded relay
/// drain path as already queued relay packets
fn apply_ingress_for_next_turn(
    context: &mut PacketLoopApplyContext<'_>,
    next_input: NextLoopInput,
    staged_relay_packet: &mut Option<ForwardedPacket>,
) {
    match next_input {
        NextLoopInput::ReadyNow => {}
        NextLoopInput::Control(command) => {
            handle_control_input_and_clear_routing_cache(
                context.packet_loop_state,
                context.bitrate_registry,
                context.snapshot_state,
                context.config,
                command,
                context.routing_state,
            );
        }
        NextLoopInput::RelayPacket => {
            *staged_relay_packet = context.inputs.take_woken_relay_packet();
        }
        NextLoopInput::Datagram {
            source_addr,
            candidate_addr,
            received_size,
        } => {
            route_received_datagram(context, source_addr, candidate_addr, received_size);
        }
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
        context.routing_state,
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
fn handle_control_input_and_clear_routing_cache(
    packet_loop_state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    command: PacketLoopControlInput,
    routing_state: &mut PacketLoopRoutingState,
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
            metrics: &config.metrics,
        },
    );
    routing_state.clear_on_topology_change();
}

/// runs the synchronous work for one packet-loop turn
///
/// this function is intentionally non-async because it holds mutable worker
/// state
/// it returns only the cloned socket and next deadline needed after that borrow
/// ends
fn snapshot_and_pump(
    state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    turn: &mut PacketLoopTurnContext,
) -> Option<WaitPhaseSnapshot> {
    let turn_started_at = Instant::now();
    turn.buffers.clear();
    let (socket, candidate_addr) = {
        // no socket means no RTC session has reached transport bootstrap yet
        // the worker should wait only for control or relay input
        let shared_socket = state.shared_socket.as_ref()?;
        (
            Arc::clone(&shared_socket.socket),
            shared_socket.candidate_addr,
        )
    };
    if let Some(packet) = turn.staged_relay_packet.take() {
        // the packet that woke the wait phase enters the same batch as packets
        // drained from the relay mailbox below
        turn.buffers.pending_packets.push(packet);
    }
    let now = Instant::now();
    let session_drain_context = SessionDrainContext {
        snapshot_state,
        diagnostics: &config.diagnostics,
        metrics: &config.metrics,
        source_policy_signal: &config.source_policy_signal,
        socket: &socket,
    };
    drain_ready_sessions(state, &session_drain_context, &mut turn.buffers, now);
    drain_relay_packets(
        relay_rx,
        &mut turn.buffers.pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
        &config.rtc_metrics,
    );
    state.flush_pending_remote_source_packet_gates();
    drain_due_rid_keyframe_refreshes(state, &*config.rtc_metrics, now);
    flush_pending_keyframe_requests(state, &*config.rtc_metrics, &mut turn.buffers);
    // packet observations must run before fanout planning because layer gates
    // and first-ingress keyframes depend on facts learned from this batch
    record_incoming_stats(
        state,
        &config.source_policy_signal,
        &*config.rtc_metrics,
        &config.rtp_metrics,
        &mut turn.buffers,
    );
    // sink routes are refreshed once per turn so recording lookups do not take
    // the shared registry lock per packet
    turn.packet_sink_cache
        .refresh_from(&config.packet_sink_registry);
    // planning is separated from flushing so all destinations for the batch are
    // known before any send mutates local session state
    for (packet_idx, packet) in turn.buffers.pending_packets.iter_mut().enumerate() {
        populate_forward_routes_for_packet(
            state,
            &turn.packet_sink_cache,
            &*config.rtc_metrics,
            packet_idx,
            packet,
            &mut turn.buffers.forwards,
        );
    }
    // flushing executes local RTC sends, relay sends and packet-sink fanout
    // planned destination order preserves payload reuse opportunities
    flush_forward_routes(
        state,
        &config.metrics,
        &config.rtp_metrics,
        &config.rtc_metrics,
        &mut turn.buffers,
    );
    Some(WaitPhaseSnapshot {
        socket,
        candidate_addr,
        next_timeout: next_timeout_deadline(state),
        turn_started_at,
    })
}

/// updates lag through the coalescing publisher
///
/// this avoids writing the shared atomic snapshot on every loop turn
fn record_packet_loop_lag(
    snapshot: &PacketLoopLagSnapshot,
    publisher: &mut PacketLoopLagPublisher,
    turn_started_at: Instant,
) {
    publisher.observe(snapshot, turn_started_at, Instant::now());
}

/// returns the next time the loop must wake without external input
///
/// dirty sessions are always due immediately because a previous input or local
/// send has queued more `str0m` output
/// otherwise the deadline is the earlier of the next `str0m` timeout and the
/// next delayed selected-RID keyframe refresh
pub(super) fn next_timeout_deadline(state: &mut PacketLoopState) -> Option<Instant> {
    if state.has_dirty_sessions() {
        return Some(Instant::now());
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

/// waits for the next event that should resume the worker loop
///
/// shutdown and control input are biased ahead of socket receive
/// when no socket has been opened yet, only mailbox input or shutdown can wake
/// the worker
async fn wait_for_next_loop_input(
    snapshot: Option<WaitPhaseSnapshot>,
    inputs: &mut PacketLoopInputReceivers,
    receive_buffer: &mut [u8],
    ready_now_budget: &mut usize,
    udp_burst_budget: &mut usize,
) -> Option<NextLoopInput> {
    let Some(info) = snapshot else {
        // without a socket there is no media path to poll
        // the worker waits for lifecycle input that can create a session
        return inputs.recv_control_or_relay().await.map(mailbox_to_input);
    };
    if let Some(next_timeout) = info.next_timeout
        && next_timeout <= Instant::now()
        && *ready_now_budget > 0
    {
        // ready-now keeps due internal work moving but the budget prevents an
        // expired deadline from starving mailbox or socket input forever
        *ready_now_budget = (*ready_now_budget).saturating_sub(1);
        return Some(NextLoopInput::ReadyNow);
    }
    // after one awaited datagram wakes the loop, drain a bounded number of
    // queued datagrams with `try_recv_from` so socket bursts do not pay one
    // await per packet
    if let Some(next_input) = try_recv_queued_datagram(&info, receive_buffer, udp_burst_budget) {
        return Some(next_input);
    }
    // mailbox input is biased so shutdown and lifecycle commands stay
    // responsive even while media traffic is heavy
    tokio::select! {
        biased;
        next_input = inputs.recv_control_or_relay() => next_input.map(mailbox_to_input),
        next_input = wait_for_socket_input(&info, receive_buffer, ready_now_budget, udp_burst_budget) => Some(next_input),
    }
}

/// tries to consume one already queued UDP datagram without awaiting
///
/// the burst budget allows short receive bursts while still forcing the worker
/// back to the biased wait after a bounded number of datagrams
fn try_recv_queued_datagram(
    info: &WaitPhaseSnapshot,
    receive_buffer: &mut [u8],
    udp_burst_budget: &mut usize,
) -> Option<NextLoopInput> {
    if *udp_burst_budget == 0 {
        return None;
    }
    match info.socket.try_recv_from(receive_buffer) {
        Ok((received_size, source_addr)) => {
            // each datagram gets its own following pump turn, so only the input
            // budget is decremented here
            *udp_burst_budget = (*udp_burst_budget).saturating_sub(1);
            Some(NextLoopInput::Datagram {
                source_addr,
                candidate_addr: info.candidate_addr,
                received_size,
            })
        }
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            // no queued datagram means the next awaited receive starts a fresh
            // burst budget
            *udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN;
            None
        }
        Err(_error) => {
            // receive errors should not end the worker
            // an immediate turn lets timeouts and command handling continue
            warn!("rtc packet loop failed to receive datagram");
            Some(NextLoopInput::ReadyNow)
        }
    }
}

fn mailbox_to_input(input: PacketLoopMailboxInput) -> NextLoopInput {
    match input {
        PacketLoopMailboxInput::Control(command) => NextLoopInput::Control(command),
        PacketLoopMailboxInput::Relay => NextLoopInput::RelayPacket,
    }
}

/// waits for either socket input or the next internal timeout
///
/// the socket receive future borrows only the fixed receive buffer and cloned
/// socket handle from the wait snapshot
async fn wait_for_socket_input(
    info: &WaitPhaseSnapshot,
    receive_buffer: &mut [u8],
    ready_now_budget: &mut usize,
    udp_burst_budget: &mut usize,
) -> NextLoopInput {
    let receive = info.socket.recv_from(receive_buffer);
    let result = if let Some(next_timeout) = info.next_timeout {
        // an exhausted ready-now budget converts an already-due deadline into a
        // minimum socket wait through `socket_wait_duration`
        // that gives the biased `select!` a real executor yield before the
        // budget is replenished
        match timeout(socket_wait_duration(next_timeout), receive).await {
            Ok(result) => result,
            Err(_elapsed) => {
                *ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
                return NextLoopInput::ReadyNow;
            }
        }
    } else {
        receive.await
    };
    // successful receives reset the burst budget around this first datagram
    // so follow-up queued datagrams can be tried without awaiting again
    handle_socket_receive_result(result, info.candidate_addr, udp_burst_budget)
}

/// keeps socket receive errors recoverable
///
/// a receive error becomes a ready-now wake so the worker can continue handling
/// control input and future timeouts
fn handle_socket_receive_result(
    result: Result<(usize, SocketAddr), IoError>,
    candidate_addr: SocketAddr,
    udp_burst_budget: &mut usize,
) -> NextLoopInput {
    match result {
        Ok((received_size, source_addr)) => {
            // one datagram is already consumed from the new burst
            *udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN.saturating_sub(1);
            NextLoopInput::Datagram {
                source_addr,
                candidate_addr,
                received_size,
            }
        }
        Err(_error) => {
            warn!("rtc packet loop failed to receive datagram");
            NextLoopInput::ReadyNow
        }
    }
}

/// prevents an expired deadline from becoming a spin loop
///
/// a deadline that is already due becomes a one millisecond timeout
/// this gives Tokio a scheduling point before the next ready-now turn
fn socket_wait_duration(next_timeout: Instant) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(Instant::now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
