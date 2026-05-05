//! Packet-loop worker driver.
//!
//! This module owns the async task that ties the RTC worker together. It is the
//! only packet-loop file that awaits socket I/O or worker-channel input. The
//! rest of the packet-loop modules are synchronous helpers that run while the
//! worker owns mutable access to `RtcBootstrapState`.
//!
//! The driver preserves the worker ordering contract:
//!
//! - drain already queued commands before touching media
//! - pump dirty or timed-out sessions and bounded relay packets
//! - flush all staged UDP transmits outside any shared-state lock
//! - wait for the next shutdown, command, timeout or UDP datagram event
//! - try to route one received datagram into its owning `str0m::Rtc`
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
        forwarding_planner::populate_forward_routes,
        routing_miss::PacketLoopRoutingState,
        state::{RtcBootstrapState, RtcSnapshotState},
        worker::{WorkerCommandContext, drain_due_rid_keyframe_refreshes},
    },
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers, RECEIVE_BUFFER_LEN},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::route_packet_to_matching_session,
    input::{PacketLoopControlInput, PacketLoopInputReceivers},
    keyframe_requests::flush_pending_keyframe_requests,
    session_drain::drain_ready_sessions,
};
use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore, media_transport::SourcePolicySignal,
        metrics::RuntimeMetrics, packet_sink_registry::RoomPacketSinkRegistry,
    },
};

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
}

/// Socket snapshot used for the wait phase of one packet-loop turn.
///
/// The `Arc<UdpSocket>` is cloned before the loop awaits so no borrow of
/// `RtcBootstrapState` crosses an await point. `next_timeout` is computed from
/// dirty sessions, `str0m` timeouts and delayed selected-RID keyframe refreshes.
struct SnapshotInfo {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<Instant>,
}

/// External input that resumes the worker after the pump phase.
///
/// A timeout wake is represented as `Datagram { received_size: 0, .. }`. That
/// keeps the driver on one input path while letting `next_timeout_deadline`
/// wake ready sessions without fabricating a command.
enum NextLoopInput {
    Control(PacketLoopControlInput),
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
}

/// Run the shard-local media packet loop until shutdown or worker-channel close.
///
/// # Concurrency model
///
/// This task owns `RtcBootstrapState`, routing hints, the UDP receive buffer and
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
    bitrate_state: Arc<Mutex<RtcBitrateState>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut inputs: PacketLoopInputReceivers,
) {
    let mut bootstrap_state = RtcBootstrapState {
        next_media_id: config.media_id_base,
        ..RtcBootstrapState::default()
    };
    let mut routing_state = PacketLoopRoutingState::new();
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    let mut buffers = PacketLoopBuffers::new();

    loop {
        drain_pending_control_inputs(
            &mut bootstrap_state,
            &bitrate_state,
            &snapshot_state,
            &config,
            &mut inputs,
            &mut routing_state,
        );

        let snapshot = snapshot_and_pump(
            &mut bootstrap_state,
            &snapshot_state,
            &config,
            inputs.relay_rx(),
            &mut buffers,
        );

        if let Some(info) = snapshot.as_ref() {
            for pending_transmit in buffers.pending_transmits() {
                if info
                    .socket
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

        let Some(next_input) =
            wait_for_next_loop_input(snapshot, &mut inputs, &mut receive_buffer).await
        else {
            return;
        };

        let _ = handle_loop_input(
            &mut bootstrap_state,
            &bitrate_state,
            &snapshot_state,
            &config,
            next_input,
            &receive_buffer,
            &mut routing_state,
        );
    }
}

fn drain_pending_control_inputs(
    bootstrap_state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    inputs: &mut PacketLoopInputReceivers,
    routing_state: &mut PacketLoopRoutingState,
) {
    while let Some(input) = inputs.try_recv_control() {
        handle_control_input_and_clear_routing_cache(
            bootstrap_state,
            bitrate_state,
            snapshot_state,
            config,
            input,
            routing_state,
        );
    }
}

/// Apply the event that woke the worker after the pump phase.
///
/// Control inputs mutate authoritative worker state and conservatively
/// invalidate ingress routing hints. Datagram inputs are routed into the owning
/// `str0m::Rtc` if the receive buffer contains a non-empty packet. A
/// zero-length datagram input is the driver's internal timeout wake and only
/// causes the next turn to poll ready sessions.
fn handle_loop_input(
    bootstrap_state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    next_input: NextLoopInput,
    receive_buffer: &[u8],
    routing_state: &mut PacketLoopRoutingState,
) -> bool {
    match next_input {
        NextLoopInput::Control(input) => {
            handle_control_input_and_clear_routing_cache(
                bootstrap_state,
                bitrate_state,
                snapshot_state,
                config,
                input,
                routing_state,
            );
            true
        }
        NextLoopInput::Datagram {
            source_addr,
            candidate_addr,
            received_size,
        } => {
            if received_size == 0 {
                return false;
            }
            let Some(packet) = receive_buffer.get(..received_size) else {
                return false;
            };
            route_packet_to_matching_session(
                bootstrap_state,
                snapshot_state,
                routing_state,
                &config.metrics,
                source_addr,
                candidate_addr,
                packet,
            );
            true
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
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    input: PacketLoopControlInput,
    routing_state: &mut PacketLoopRoutingState,
) {
    input.dispatch(
        bootstrap_state,
        &WorkerCommandContext {
            bitrate_state,
            snapshot_state,
            public_ip: config.public_ip,
            max_bitrate_in_bps: config.max_bitrate_in_bps,
            max_bitrate_out_bps: config.max_bitrate_out_bps,
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
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    relay_rx: &mut mpsc::Receiver<super::super::forwarded_packet::ForwardedPacket>,
    buffers: &mut PacketLoopBuffers,
) -> Option<SnapshotInfo> {
    buffers.clear();
    let (socket, candidate_addr) = {
        let shared_socket = state.shared_socket.as_ref()?;
        (
            Arc::clone(&shared_socket.socket),
            shared_socket.candidate_addr,
        )
    };
    let now = Instant::now();
    drain_ready_sessions(
        state,
        snapshot_state,
        &config.diagnostics,
        &config.metrics,
        &config.source_policy_signal,
        buffers,
        now,
    );
    drain_relay_packets(
        relay_rx,
        &mut buffers.pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );
    drain_due_rid_keyframe_refreshes(state, &config.metrics, now);
    flush_pending_keyframe_requests(state, &config.metrics, buffers);
    record_incoming_stats(
        state,
        &config.source_policy_signal,
        &config.metrics,
        buffers,
    );
    populate_forward_routes(
        state,
        &config.packet_sink_registry,
        &config.metrics,
        &mut buffers.pending_packets,
        &mut buffers.forwards,
    );
    flush_forward_routes(state, &config.metrics, buffers);
    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: next_timeout_deadline(state),
    })
}

/// Return the next time the loop must wake without external input.
///
/// Dirty sessions are always due immediately because a previous input or local
/// send has queued more `str0m` output. Otherwise the deadline is the earlier of
/// the next `str0m` timeout and the next delayed selected-RID keyframe refresh.
pub(super) fn next_timeout_deadline(state: &mut RtcBootstrapState) -> Option<Instant> {
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
) -> Option<NextLoopInput> {
    let Some(info) = snapshot else {
        return inputs.recv_control().await.map(NextLoopInput::Control);
    };
    tokio::select! {
        biased;
        next_input = inputs.recv_control() => next_input.map(NextLoopInput::Control),
        next_input = wait_for_socket_input(&info, receive_buffer) => Some(next_input),
    }
}

async fn wait_for_socket_input(info: &SnapshotInfo, receive_buffer: &mut [u8]) -> NextLoopInput {
    let receive = info.socket.recv_from(receive_buffer);
    let result = if let Some(next_timeout) = info.next_timeout {
        match timeout(socket_wait_duration(next_timeout), receive).await {
            Ok(result) => result,
            Err(_elapsed) => return empty_datagram_loop_input(info.candidate_addr),
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
            empty_datagram_loop_input(candidate_addr)
        }
    }
}

fn empty_datagram_loop_input(candidate_addr: SocketAddr) -> NextLoopInput {
    NextLoopInput::Datagram {
        source_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
        candidate_addr,
        received_size: 0,
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
