//! Packet-loop worker driver.
//!
//! This module contain the async task that ties the RTC worker together. It is the
//! only packet-loop file that awaits socket I/O or worker-channel input. The
//! rest of the packet-loop modules are synchronous helpers that run while the
//! worker owns mutable access to `PacketLoopState`.
//!
//! The driver preserves the worker ordering contract:
//!
//! - drain already queued commands before touching media
//! - pump dirty or timed-out sessions and bounded relay packets
//! - flush all staged UDP transmits outside any shared-state lock
//! - wait for the next shutdown, command, relay, timeout or UDP datagram event
//! - try to route one received datagram into its owning `str0m::Rtc`
//!
//! Shared observable state is updated through narrow side channels. The packet
//! loop owns authoritative media state, while snapshots, metrics, diagnostics,
//! packet sinks, relay target state and source-policy signals are outputs or
//! configuration dependencies.

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

/// Immutable configuration and shared side channels for one packet-loop worker.
///
/// The config is built by `RtcTransportShard` when the worker is booted. Values
/// copied into session creation are immutable shard settings. `Arc` fields are
/// shared services that the packet loop may update or query without exposing
/// direct access to `PacketLoopState`.
pub(in crate::runtime::rtc_engine) struct PacketLoopConfig {
    /// Public ICE-lite address advertised by sessions created on this shard.
    pub public_ip: IpAddr,
    /// Maximum inbound bitrate applied when new RTC engine sessions are created.
    pub max_bitrate_in: Bitrate,
    /// Maximum outbound bitrate applied when new RTC engine sessions are created.
    pub max_bitrate_out: Bitrate,
    /// Video bitrate policy projected into session and route-control decisions.
    pub video_bitrate_limits: VideoBitrateLimits,
    /// UDP port range used when the worker opens or reuses its shard socket.
    pub rtc_port_range: RtcPortRange,
    /// Codec feature flags used while constructing session offers.
    pub codec_flags: MediaCodecFlags,
    /// Ordered codec preferences used while constructing session offers.
    pub codec_preferences: CodecPreferences,
    /// First transport media id allocated by this worker.
    ///
    /// Media ids are worker-local counters once the loop is running, but the
    /// values must be unique across workers because cross-worker relay state is
    /// keyed by the producing media id. The shard set assigns disjoint ranges
    /// before boot so per-packet routing does not need to carry a wider key.
    pub media_id_base: u64,
    /// Cold-path diagnostics sink for transport health changes.
    pub diagnostics: Arc<DiagnosticsStore>,
    /// Room-scoped packet sinks such as recording taps.
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    /// Wakeup mechanism for room-owned source policy recomputation.
    pub source_policy_signal: Arc<SourcePolicySignal>,
    /// Central metrics catalog updated by packet-loop observations.
    pub metrics: Arc<RuntimeMetrics>,
    /// Worker-local RTP metric recorder used by packet forwarding.
    pub rtp_metrics: Arc<RtpMetricsRecorder>,
    /// Worker-local RTC packet-loop metric recorder.
    pub rtc_metrics: Arc<RtcMetricsRecorder>,
    /// Shared atomic packet-loop lag snapshot.
    pub packet_loop_lag: Arc<PacketLoopLagSnapshot>,
}

/// Socket snapshot used for the wait phase of one packet-loop turn.
///
/// The `Arc<UdpSocket>` is cloned before the loop awaits so no borrow of
/// `PacketLoopState` crosses an await point. `next_timeout` is computed from
/// dirty sessions, `str0m` timeouts and delayed selected-RID keyframe refreshes.
struct SnapshotInfo {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<Instant>,
    turn_started_at: Instant,
}

/// External input that resumes the worker after the pump phase.
///
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

struct PacketLoopTurnContext {
    buffers: PacketLoopBuffers,
    packet_sink_cache: PacketSinkRouteCache,
    staged_relay_packet: Option<ForwardedPacket>,
    lag_publisher: PacketLoopLagPublisher,
    ready_now_budget: usize,
    udp_burst_budget: usize,
}

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

/// Run the shard-local media packet loop until shutdown or worker-channel close.
///
/// # Concurrency
///
/// This task owns `PacketLoopState`, routing hints, the UDP receive buffer and
/// `PacketLoopBuffers`. Other tasks communicate with it through channels,
/// shared read-side snapshots and cancellation. No `MutexGuard` is held across
/// socket sends or receives.
///
/// # Hot-path behavior
///
/// The loop batches work in turns. A turn may produce many transmits and
/// forwards, but it waits for only one next external input before looping.
/// Relay draining is bounded so a relay burst cannot starve commands or socket
/// ingress indefinitely.
pub(in crate::runtime::rtc_engine) async fn run_packet_loop(
    config: PacketLoopConfig,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut inputs: PacketLoopInputReceivers,
) {
    let mut packet_loop_state = PacketLoopState {
        next_media_id: config.media_id_base,
        ..PacketLoopState::default()
    };
    let mut routing_state = PacketLoopRoutingState::new();
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    let mut turn = PacketLoopTurnContext::new(Instant::now());

    loop {
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

        if let Some(info) = snapshot.as_ref() {
            flush_staged_transmits(&info.socket, &turn.buffers).await;
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
            return;
        };
        if !matches!(next_input, NextLoopInput::ReadyNow) {
            turn.reset_ready_now_budget();
        }

        let mut apply_context = PacketLoopApplyContext {
            packet_loop_state: &mut packet_loop_state,
            bitrate_registry: &bitrate_registry,
            snapshot_state: &snapshot_state,
            config: &config,
            routing_state: &mut routing_state,
            inputs: &mut inputs,
            receive_buffer: &mut receive_buffer,
        };
        apply_ingress_for_next_turn(
            &mut apply_context,
            next_input,
            &mut turn.staged_relay_packet,
        );
    }
}

async fn flush_staged_transmits(socket: &UdpSocket, buffers: &PacketLoopBuffers) {
    for pending_transmit in buffers.pending_transmits() {
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

fn drain_queued_control_inputs(
    packet_loop_state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    inputs: &mut PacketLoopInputReceivers,
    routing_state: &mut PacketLoopRoutingState,
) -> bool {
    for _ in 0..MAX_CONTROL_INPUTS_PER_TURN {
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

/// Apply the event that woke the worker after the pump phase.
///
/// Control inputs mutate authoritative worker state and conservatively
/// invalidate ingress routing hints. Datagram inputs are routed into the owning
/// `str0m::Rtc`. Queued UDP datagrams can resume following turns without
/// another socket await, but every datagram still gets a pump between inputs.
/// Relay input is staged for the next pump so it reuses the same bounded relay
/// drain path as already queued relay packets.
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

fn route_received_datagram(
    context: &mut PacketLoopApplyContext<'_>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    received_size: usize,
) {
    let Some(packet) = context.receive_buffer.get(..received_size) else {
        return;
    };
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

/// Execute one control input against authoritative worker state.
///
/// The input handler owns all control-plane mutation for the RTC engine. The
/// packet-loop driver only supplies the shard context and then clears cached
/// ingress routing state. This is conservative because control input can change
/// which session owns a source tuple or ICE username fragment.
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

/// Drain synchronous worker work and return the socket state for async waiting.
///
/// This is the center of one packet-loop turn. It clears reusable buffers,
/// drains session outputs, drains bounded relay input, flushes route-control
/// feedback, records packet observations, plans fanout and executes forwarding.
/// The returned snapshot contains only the socket handle and next deadline
/// needed after the mutable borrow of worker state ends. If the worker has not
/// opened a shared socket yet, the function clears staged buffers and returns
/// without polling media.
fn snapshot_and_pump(
    state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    turn: &mut PacketLoopTurnContext,
) -> Option<SnapshotInfo> {
    let turn_started_at = Instant::now();
    turn.buffers.clear();
    let (socket, candidate_addr) = {
        let shared_socket = state.shared_socket.as_ref()?;
        (
            Arc::clone(&shared_socket.socket),
            shared_socket.candidate_addr,
        )
    };
    if let Some(packet) = turn.staged_relay_packet.take() {
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
    );
    drain_due_rid_keyframe_refreshes(state, &*config.rtc_metrics, now);
    flush_pending_keyframe_requests(state, &*config.rtc_metrics, &mut turn.buffers);
    record_incoming_stats(
        state,
        &config.source_policy_signal,
        &*config.rtc_metrics,
        &config.rtp_metrics,
        &mut turn.buffers,
    );
    turn.packet_sink_cache
        .refresh_from(&config.packet_sink_registry);
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
    flush_forward_routes(
        state,
        &config.metrics,
        &config.rtp_metrics,
        &mut turn.buffers,
    );
    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: next_timeout_deadline(state),
        turn_started_at,
    })
}

fn record_packet_loop_lag(
    snapshot: &PacketLoopLagSnapshot,
    publisher: &mut PacketLoopLagPublisher,
    turn_started_at: Instant,
) {
    publisher.observe(snapshot, turn_started_at, Instant::now());
}

/// Return the next time the loop must wake without external input.
///
/// Dirty sessions are always due immediately because a previous input or local
/// send has queued more `str0m` output. Otherwise the deadline is the earlier of
/// the next `str0m` timeout and the next delayed selected-RID keyframe refresh.
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

/// Wait for the next event that should resume the worker loop.
///
/// Shutdown and control input are biased ahead of socket receive. When no
/// socket has been opened yet, only shutdown and control input can wake the
/// worker.
async fn wait_for_next_loop_input(
    snapshot: Option<SnapshotInfo>,
    inputs: &mut PacketLoopInputReceivers,
    receive_buffer: &mut [u8],
    ready_now_budget: &mut usize,
    udp_burst_budget: &mut usize,
) -> Option<NextLoopInput> {
    let Some(info) = snapshot else {
        return inputs.recv_control_or_relay().await.map(mailbox_to_input);
    };
    if let Some(next_timeout) = info.next_timeout
        && next_timeout <= Instant::now()
        && *ready_now_budget > 0
    {
        *ready_now_budget = (*ready_now_budget).saturating_sub(1);
        return Some(NextLoopInput::ReadyNow);
    }
    if let Some(next_input) = try_recv_queued_datagram(&info, receive_buffer, udp_burst_budget) {
        return Some(next_input);
    }
    tokio::select! {
        biased;
        next_input = inputs.recv_control_or_relay() => next_input.map(mailbox_to_input),
        next_input = wait_for_socket_input(&info, receive_buffer, ready_now_budget, udp_burst_budget) => Some(next_input),
    }
}

fn try_recv_queued_datagram(
    info: &SnapshotInfo,
    receive_buffer: &mut [u8],
    udp_burst_budget: &mut usize,
) -> Option<NextLoopInput> {
    if *udp_burst_budget == 0 {
        return None;
    }
    match info.socket.try_recv_from(receive_buffer) {
        Ok((received_size, source_addr)) => {
            *udp_burst_budget = (*udp_burst_budget).saturating_sub(1);
            Some(NextLoopInput::Datagram {
                source_addr,
                candidate_addr: info.candidate_addr,
                received_size,
            })
        }
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            *udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN;
            None
        }
        Err(_error) => {
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

async fn wait_for_socket_input(
    info: &SnapshotInfo,
    receive_buffer: &mut [u8],
    ready_now_budget: &mut usize,
    udp_burst_budget: &mut usize,
) -> NextLoopInput {
    let receive = info.socket.recv_from(receive_buffer);
    let result = if let Some(next_timeout) = info.next_timeout {
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
    handle_socket_receive_result(result, info.candidate_addr, udp_burst_budget)
}

fn handle_socket_receive_result(
    result: Result<(usize, SocketAddr), IoError>,
    candidate_addr: SocketAddr,
    udp_burst_budget: &mut usize,
) -> NextLoopInput {
    match result {
        Ok((received_size, source_addr)) => {
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

/// Convert a deadline into the socket receive timeout used by `tokio::time`.
///
/// A deadline that is already due becomes a one millisecond timeout. This yields
/// back to Tokio instead of spinning if the loop reaches an expired deadline.
fn socket_wait_duration(next_timeout: Instant) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(Instant::now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
