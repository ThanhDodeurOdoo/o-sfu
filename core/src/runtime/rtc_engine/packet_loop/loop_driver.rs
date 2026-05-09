//! Packet-loop worker driver.
//!
//! This module contain the async task that ties the RTC worker together. It is the
//! only packet-loop file that awaits socket I/O or worker-channel input. The
//! rest of the packet-loop modules are synchronous helpers that run while the
//! worker owns mutable access to `RtcBootstrapState`.
//!
//! The driver preserves the worker ordering contract:
//!
//! - drain already queued commands before touching media
//! - poll dirty or timed-out host sessions and pump bounded relay packets
//! - flush all staged UDP transmits outside any shared-state lock
//! - wait for the next shutdown, command, relay, timeout or UDP datagram event
//! - ask the host session adapter to route one received datagram into its owner
//!
//! Shared observable state is updated through narrow side channels. The packet
//! loop owns authoritative media state, while snapshots, metrics, diagnostics,
//! packet sinks, relay target state and source-policy signals are outputs or
//! configuration dependencies.

use std::{
    io::Error as IoError,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tracing::warn;

use super::{
    super::{
        bitrate::RtcBitrateState,
        forwarded_packet::ForwardedPacket,
        routing_miss::PacketLoopRoutingState,
        state::{RtcBootstrapState, RtcSnapshotState},
        worker::WorkerCommandContext,
    },
    host_clock::PacketLoopClock,
    host_effects::{
        PacketLoopHostEffectContext, execute_packet_loop_effects, flush_packet_loop_forwards,
    },
    ingress_routing::{DatagramRouteInput, route_packet_to_matching_session},
    input::{PacketLoopControlInput, PacketLoopInputReceivers, PacketLoopWakeInput},
    machine::{
        effect::PacketLoopEffects,
        scratch::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopScratch, RECEIVE_BUFFER_LEN},
        state::PacketLoopState,
        turn::{PacketLoopTurn, PacketLoopTurnInput},
    },
    route_snapshot::PacketLoopRouteSnapshot,
    session_drain::{DrainedSessionOutput, SessionPollContext, drain_ready_session_outputs},
    time::PacketLoopTime,
};
use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore,
        media_transport::{SourcePolicySignal, TransportSessionKey},
        metrics::{RtpMetricsRecorder, RuntimeMetrics},
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

const PACKET_LOOP_LAG_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(50);

/// Immutable configuration and shared side channels for one packet-loop worker.
///
/// The config is built by `RtcTransportShard` when the worker is booted. Values
/// copied into session creation are immutable shard settings. `Arc` fields are
/// shared services that the packet loop may update or query without exposing
/// direct access to `RtcBootstrapState`.
pub(in crate::runtime::rtc_engine) struct PacketLoopConfig {
    /// Public ICE-lite address advertised by sessions created on this shard.
    pub public_ip: IpAddr,
    /// Maximum inbound bitrate applied when new RTC engine sessions are created.
    pub max_bitrate_in_bps: u64,
    /// Maximum outbound bitrate applied when new RTC engine sessions are created.
    pub max_bitrate_out_bps: u64,
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
}

/// Socket snapshot used for the wait phase of one packet-loop turn.
///
/// The `Arc<UdpSocket>` is cloned before the loop awaits so no borrow of
/// `RtcBootstrapState` crosses an await point. `next_timeout` is computed from
/// dirty sessions, `str0m` timeouts and delayed selected-RID keyframe refreshes.
struct SnapshotInfo {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<PacketLoopTime>,
    clock: PacketLoopClock,
}

struct PacketLoopRuntimeContext<'a> {
    bitrate_state: &'a Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    config: &'a PacketLoopConfig,
}

impl<'a> PacketLoopRuntimeContext<'a> {
    fn host_effect_context(&self) -> PacketLoopHostEffectContext<'a> {
        PacketLoopHostEffectContext {
            snapshot_state: self.snapshot_state,
            diagnostics: &self.config.diagnostics,
            metrics: &self.config.metrics,
            source_policy_signal: &self.config.source_policy_signal,
            rtp_metrics: &self.config.rtp_metrics,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct PacketLoopLagReporter {
    last_snapshot_at: Option<Instant>,
    pending_max_lag_ms: u64,
}

impl PacketLoopLagReporter {
    pub(super) fn record(&mut self, observed_at: Instant, lag_ms: u64) -> Option<u64> {
        self.pending_max_lag_ms = self.pending_max_lag_ms.max(lag_ms);
        if let Some(last_snapshot_at) = self.last_snapshot_at
            && observed_at.saturating_duration_since(last_snapshot_at)
                < PACKET_LOOP_LAG_SNAPSHOT_INTERVAL
        {
            return None;
        }
        self.last_snapshot_at = Some(observed_at);
        let lag_ms = self.pending_max_lag_ms;
        self.pending_max_lag_ms = 0;
        Some(lag_ms)
    }
}

struct PacketLoopPumpWorkspace<'a> {
    relay_rx: &'a mut mpsc::Receiver<ForwardedPacket>,
    scratch: &'a mut PacketLoopScratch,
    effects: &'a mut PacketLoopEffects,
    route_snapshot: &'a mut PacketLoopRouteSnapshot,
    session_output_batch: &'a mut Vec<DrainedSessionOutput>,
    ready_session_batch: &'a mut Vec<TransportSessionKey>,
    relay_packet_batch: &'a mut Vec<ForwardedPacket>,
    pending_relay_packet: &'a mut Option<ForwardedPacket>,
    lag_reporter: &'a mut PacketLoopLagReporter,
}

/// External input that resumes the worker after the pump phase.
///
enum NextLoopInput {
    Control(PacketLoopControlInput),
    Relay,
    Timeout,
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
}

/// Run the shard-local media packet loop until shutdown or worker-channel close.
///
/// # Concurrency
///
/// This task owns `RtcBootstrapState`, routing hints, the UDP receive buffer and
/// `PacketLoopScratch`. Other tasks communicate with it through channels,
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
    bitrate_state: Arc<Mutex<RtcBitrateState>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut inputs: PacketLoopInputReceivers,
) {
    let mut bootstrap_state = RtcBootstrapState::default();
    bootstrap_state.packet_loop.next_media_id = config.media_id_base;
    let mut routing_state = PacketLoopRoutingState::new();
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    let mut scratch = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();
    let mut route_snapshot = PacketLoopRouteSnapshot::default();
    let mut session_output_batch = Vec::with_capacity(64);
    let mut ready_session_batch = Vec::with_capacity(64);
    let mut relay_packet_batch = Vec::with_capacity(MAX_RELAY_PACKETS_PER_ITERATION);
    let mut pending_relay_packet = None;
    let mut lag_reporter = PacketLoopLagReporter::default();
    let clock = PacketLoopClock::new(Instant::now());
    let runtime_context = PacketLoopRuntimeContext {
        bitrate_state: &bitrate_state,
        snapshot_state: &snapshot_state,
        config: &config,
    };

    loop {
        while let Some(input) = inputs.try_recv_control() {
            handle_control_input_and_clear_routing_cache(
                &mut bootstrap_state,
                &runtime_context,
                input,
                &mut routing_state,
                clock,
            );
        }
        if inputs.control_receiver_closed() {
            return;
        }

        let snapshot = snapshot_and_pump(
            &mut bootstrap_state,
            &runtime_context,
            PacketLoopPumpWorkspace {
                relay_rx: inputs.relay_rx(),
                scratch: &mut scratch,
                effects: &mut effects,
                route_snapshot: &mut route_snapshot,
                session_output_batch: &mut session_output_batch,
                ready_session_batch: &mut ready_session_batch,
                relay_packet_batch: &mut relay_packet_batch,
                pending_relay_packet: &mut pending_relay_packet,
                lag_reporter: &mut lag_reporter,
            },
            clock,
        );

        if let Some(info) = snapshot.as_ref() {
            for pending_transmit in scratch.pending_transmits() {
                if info
                    .socket
                    .send_to(pending_transmit.contents(), pending_transmit.destination())
                    .await
                    .is_err()
                {
                    warn!(
                        destination = %pending_transmit.destination(),
                        "failed to send packet-loop transport datagram"
                    );
                }
            }
        }

        let Some(next_input) = wait_for_next_loop_input(
            snapshot,
            &mut inputs,
            &mut receive_buffer,
            &mut pending_relay_packet,
        )
        .await
        else {
            return;
        };

        if let NextLoopInput::Relay = next_input {
            continue;
        }

        effects.clear();
        apply_ingress_for_next_turn(
            &mut bootstrap_state,
            &runtime_context,
            next_input,
            &receive_buffer,
            &mut routing_state,
            &mut effects,
            clock,
        );
        let host_effect_context = runtime_context.host_effect_context();
        execute_packet_loop_effects(
            &mut bootstrap_state,
            &scratch,
            &host_effect_context,
            &effects,
        );
    }
}

/// Apply the event that woke the worker after the pump phase.
///
/// Control inputs mutate authoritative worker state and conservatively
/// invalidate ingress routing hints. Datagram inputs are routed into the owning
/// host session if the receive buffer contains a packet. Relay and timeout
/// input only cause the next turn to poll ready sessions and relay packets.
fn apply_ingress_for_next_turn(
    bootstrap_state: &mut RtcBootstrapState,
    runtime_context: &PacketLoopRuntimeContext<'_>,
    next_input: NextLoopInput,
    receive_buffer: &[u8],
    routing_state: &mut PacketLoopRoutingState,
    effects: &mut PacketLoopEffects,
    clock: PacketLoopClock,
) {
    match next_input {
        NextLoopInput::Control(command) => {
            handle_control_input_and_clear_routing_cache(
                bootstrap_state,
                runtime_context,
                command,
                routing_state,
                clock,
            );
        }
        NextLoopInput::Relay | NextLoopInput::Timeout => {}
        NextLoopInput::Datagram {
            source_addr,
            candidate_addr,
            received_size,
        } => {
            let Some(packet) = receive_buffer.get(..received_size) else {
                return;
            };
            route_packet_to_matching_session(
                bootstrap_state,
                routing_state,
                effects,
                DatagramRouteInput {
                    source_addr,
                    candidate_addr,
                    packet,
                    received_at: Instant::now(),
                    packet_time: clock.now(),
                },
            );
        }
    }
}

/// Execute one control input against authoritative worker state.
///
/// The input handler owns all control-plane mutation for the RTC engine. The
/// packet-loop driver only supplies the shard context and then clears cached
/// ingress routing state. This is conservative because control input can change
/// which session owns a source tuple or ICE username fragment.
fn handle_control_input_and_clear_routing_cache(
    bootstrap_state: &mut RtcBootstrapState,
    runtime_context: &PacketLoopRuntimeContext<'_>,
    command: PacketLoopControlInput,
    routing_state: &mut PacketLoopRoutingState,
    clock: PacketLoopClock,
) {
    let now = Instant::now();
    command.dispatch(
        bootstrap_state,
        &WorkerCommandContext {
            bitrate_state: runtime_context.bitrate_state,
            snapshot_state: runtime_context.snapshot_state,
            packet_now: clock.to_packet_time(now),
            public_ip: runtime_context.config.public_ip,
            max_bitrate_in_bps: runtime_context.config.max_bitrate_in_bps,
            max_bitrate_out_bps: runtime_context.config.max_bitrate_out_bps,
            video_bitrate_limits: runtime_context.config.video_bitrate_limits,
            rtc_port_range: runtime_context.config.rtc_port_range,
            codec_flags: runtime_context.config.codec_flags,
            codec_preferences: runtime_context.config.codec_preferences,
            metrics: &runtime_context.config.metrics,
        },
    );
    routing_state.clear_on_topology_change();
}

/// Drain synchronous worker work and return the socket state for async waiting.
///
/// Host wrapper around one packet-loop machine turn.
///
/// This keeps socket ownership, relay-channel draining, packet-sink snapshot
/// refresh, effect execution, lag measurement and wake calculation outside the
/// machine turn. The turn itself only mutates worker state, scratch and ordered
/// effects.
///
/// The returned snapshot contains only the socket handle and next deadline
/// needed after the mutable borrow of worker state ends. If the worker has not
/// opened a shared socket yet, the function clears staged buffers and returns
/// without polling media.
fn snapshot_and_pump(
    state: &mut RtcBootstrapState,
    runtime_context: &PacketLoopRuntimeContext<'_>,
    workspace: PacketLoopPumpWorkspace<'_>,
    clock: PacketLoopClock,
) -> Option<SnapshotInfo> {
    let PacketLoopPumpWorkspace {
        relay_rx,
        scratch,
        effects,
        route_snapshot,
        session_output_batch,
        ready_session_batch,
        relay_packet_batch,
        pending_relay_packet,
        lag_reporter,
    } = workspace;
    let turn_started_at = Instant::now();
    scratch.clear();
    effects.clear();
    session_output_batch.clear();
    relay_packet_batch.clear();
    let (socket, candidate_addr) = {
        let shared_socket = state.shared_socket.as_ref()?;
        (
            Arc::clone(&shared_socket.socket),
            shared_socket.candidate_addr,
        )
    };
    let host_now = Instant::now();
    let packet_now = clock.to_packet_time(host_now);
    drain_ready_session_outputs(
        state,
        session_output_batch,
        ready_session_batch,
        &SessionPollContext {
            host_now,
            packet_now,
            clock,
        },
    );
    drain_relay_packets_into_batch(
        relay_rx,
        relay_packet_batch,
        pending_relay_packet,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );
    route_snapshot.refresh_from(state, &runtime_context.config.packet_sink_registry);
    PacketLoopTurn::step(
        &mut state.packet_loop,
        scratch,
        effects,
        PacketLoopTurnInput::new(
            packet_now,
            session_output_batch,
            relay_packet_batch,
            route_snapshot,
        ),
    );
    let host_effect_context = runtime_context.host_effect_context();
    execute_packet_loop_effects(state, &*scratch, &host_effect_context, effects);
    flush_packet_loop_forwards(state, scratch, route_snapshot, &host_effect_context);
    record_packet_loop_lag(
        runtime_context.snapshot_state,
        turn_started_at,
        lag_reporter,
    );
    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: next_timeout_deadline(&mut state.packet_loop, packet_now),
        clock,
    })
}

pub(super) fn drain_relay_packets_into_batch(
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    relay_packet_batch: &mut Vec<ForwardedPacket>,
    pending_relay_packet: &mut Option<ForwardedPacket>,
    max_packets: usize,
) -> usize {
    relay_packet_batch.clear();
    if relay_packet_batch.len() < max_packets
        && let Some(packet) = pending_relay_packet.take()
    {
        relay_packet_batch.push(packet);
    }
    let mut drained_packets = 0;
    while relay_packet_batch.len() < max_packets {
        match relay_rx.try_recv() {
            Ok(packet) => {
                relay_packet_batch.push(packet);
                drained_packets += 1;
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
    drained_packets
}

fn record_packet_loop_lag(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    turn_started_at: Instant,
    reporter: &mut PacketLoopLagReporter,
) {
    let observed_at = Instant::now();
    let lag_ms = u64::try_from(
        observed_at
            .saturating_duration_since(turn_started_at)
            .as_millis(),
    )
    .map_or(u64::MAX, |value| value);
    let Some(lag_ms) = reporter.record(observed_at, lag_ms) else {
        return;
    };
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.set_packet_loop_lag_ms(lag_ms, observed_at);
    }
}

/// Return the next time the loop must wake without external input.
///
/// Dirty sessions are always due immediately because a previous input or local
/// send has queued more `str0m` output. Otherwise the deadline is the earlier of
/// the next `str0m` timeout and the next delayed selected-RID keyframe refresh.
pub(super) fn next_timeout_deadline(
    state: &mut PacketLoopState,
    now: PacketLoopTime,
) -> Option<PacketLoopTime> {
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

/// Wait for the next event that should resume the worker loop.
///
/// Socket input stays ahead of relay input once queued control has been
/// drained. Relay still wakes an otherwise idle worker, but a relay burst must
/// not starve browser-originated RTCP or ICE traffic.
async fn wait_for_next_loop_input(
    snapshot: Option<SnapshotInfo>,
    inputs: &mut PacketLoopInputReceivers,
    receive_buffer: &mut [u8],
    pending_relay_packet: &mut Option<ForwardedPacket>,
) -> Option<NextLoopInput> {
    if let Some(input) = inputs.try_recv_control() {
        return Some(NextLoopInput::Control(input));
    }
    if inputs.control_receiver_closed() {
        return None;
    }
    let Some(info) = snapshot else {
        return inputs.recv_control().await.map(NextLoopInput::Control);
    };
    tokio::select! {
        biased;
        next_input = wait_for_socket_input(&info, receive_buffer) => Some(next_input),
        next_input = inputs.recv_control_or_relay(pending_relay_packet) => next_input.map(next_loop_input_from_wake),
    }
}

fn next_loop_input_from_wake(input: PacketLoopWakeInput) -> NextLoopInput {
    match input {
        PacketLoopWakeInput::Control(input) => NextLoopInput::Control(input),
        PacketLoopWakeInput::Relay => NextLoopInput::Relay,
    }
}

async fn wait_for_socket_input(info: &SnapshotInfo, receive_buffer: &mut [u8]) -> NextLoopInput {
    let receive = info.socket.recv_from(receive_buffer);
    let result = if let Some(next_timeout) = info.next_timeout {
        match timeout(socket_wait_duration(info.clock, next_timeout), receive).await {
            Ok(result) => result,
            Err(_elapsed) => return NextLoopInput::Timeout,
        }
    } else {
        receive.await
    };
    handle_socket_receive_result(result, info.candidate_addr)
}

fn handle_socket_receive_result(
    result: Result<(usize, SocketAddr), IoError>,
    candidate_addr: SocketAddr,
) -> NextLoopInput {
    match result {
        Ok((received_size, source_addr)) => NextLoopInput::Datagram {
            source_addr,
            candidate_addr,
            received_size,
        },
        Err(_error) => {
            warn!("rtc packet loop failed to receive datagram");
            NextLoopInput::Timeout
        }
    }
}

/// Convert a deadline into the socket receive timeout used by `tokio::time`.
///
/// A deadline that is already due becomes a one millisecond timeout. This yields
/// back to Tokio instead of spinning if the loop reaches an expired deadline.
fn socket_wait_duration(clock: PacketLoopClock, next_timeout: PacketLoopTime) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(clock.now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
