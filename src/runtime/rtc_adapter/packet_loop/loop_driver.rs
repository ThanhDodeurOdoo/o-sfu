use std::{
    io::Error as IoError,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[cfg(test)]
use super::super::commands::debug::DebugRtcWorkerCommand;
#[cfg(test)]
use super::super::worker::handle_debug_worker_command;
use super::super::{
    commands::RtcWorkerCommand,
    forwarding_planner::populate_forward_routes,
    relay_registry::RelayRegistry,
    routing_miss::PacketLoopRoutingState,
    state::{RtcBitrateState, RtcBootstrapState, RtcSnapshotState},
    worker::{WorkerCommandContext, handle_worker_command},
};
use super::{
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers, RECEIVE_BUFFER_LEN},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::route_packet_to_matching_session,
    keyframe_requests::flush_pending_keyframe_requests,
    session_drain::drain_ready_sessions,
};
use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::{metrics::RuntimeMetrics, recording::MediaTap};

pub(crate) struct PacketLoopConfig {
    pub(crate) public_ip: IpAddr,
    pub(crate) rtc_port_range: RtcPortRange,
    pub(crate) codec_flags: MediaCodecFlags,
    pub(crate) media_tap: Arc<MediaTap>,
    pub(crate) relay_registry: Arc<RelayRegistry>,
    pub(crate) metrics: Arc<RuntimeMetrics>,
}

struct SnapshotInfo {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<Instant>,
}

enum NextLoopInput {
    Command(RtcWorkerCommand),
    #[cfg(test)]
    DebugCommand(DebugRtcWorkerCommand),
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
}

/// The main entry point for the media packet processing loop.
///
/// This function runs indefinitely (until the `shutdown_token` is cancelled),
/// orchestrating the high-frequency tasks of the RTC adapter:
///
/// 1. **Command Processing**: Drains control commands that modify the topology or state.
/// 2. **Media Pumping**: Drains media from all active sessions and relay channels.
/// 3. **Packet Transmission**: Flushes all pending transmissions (media, keyframe requests)
///    to the underlying UDP socket.
/// 4. **Socket Reception**: Waits for incoming UDP datagrams and routes them to the
///    appropriate session.
///
/// This loop is "biased" towards control commands and shutdown to ensure the
/// SFU remains responsive to management even under heavy media load.
pub(crate) async fn run_packet_loop(
    config: PacketLoopConfig,
    bitrate_state: Arc<Mutex<RtcBitrateState>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut command_rx: mpsc::Receiver<RtcWorkerCommand>,
    #[cfg(test)] mut debug_rx: mpsc::Receiver<DebugRtcWorkerCommand>,
    mut relay_rx: mpsc::Receiver<super::super::forwarded_packet::ForwardedPacket>,
    shutdown_token: CancellationToken,
) {
    let mut bootstrap_state = RtcBootstrapState::default();
    let mut routing_state = PacketLoopRoutingState::new();
    let mut receive_buffer = vec![0_u8; RECEIVE_BUFFER_LEN];
    let mut buffers = PacketLoopBuffers::new();

    loop {
        drain_pending_worker_commands(
            &mut bootstrap_state,
            &bitrate_state,
            &snapshot_state,
            &config,
            &mut command_rx,
            #[cfg(test)]
            &mut debug_rx,
            &mut routing_state,
        );

        let snapshot = snapshot_and_pump(
            &mut bootstrap_state,
            &bitrate_state,
            &snapshot_state,
            &config,
            &mut relay_rx,
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

        let Some(next_input) = wait_for_next_loop_input(
            snapshot,
            &mut command_rx,
            #[cfg(test)]
            &mut debug_rx,
            &mut receive_buffer,
            &shutdown_token,
        )
        .await
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

fn drain_pending_worker_commands(
    bootstrap_state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    #[cfg(test)] debug_rx: &mut mpsc::Receiver<DebugRtcWorkerCommand>,
    routing_state: &mut PacketLoopRoutingState,
) {
    while let Ok(command) = command_rx.try_recv() {
        handle_worker_command_and_clear_routing_cache(
            bootstrap_state,
            bitrate_state,
            snapshot_state,
            config,
            command,
            routing_state,
        );
    }
    #[cfg(test)]
    while let Ok(command) = debug_rx.try_recv() {
        handle_debug_worker_command_and_clear_routing_cache(
            bootstrap_state,
            bitrate_state,
            snapshot_state,
            config,
            command,
            routing_state,
        );
    }
}

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
        NextLoopInput::Command(command) => {
            handle_worker_command_and_clear_routing_cache(
                bootstrap_state,
                bitrate_state,
                snapshot_state,
                config,
                command,
                routing_state,
            );
            true
        }
        #[cfg(test)]
        NextLoopInput::DebugCommand(command) => {
            handle_debug_worker_command_and_clear_routing_cache(
                bootstrap_state,
                bitrate_state,
                snapshot_state,
                config,
                command,
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

fn handle_worker_command_and_clear_routing_cache(
    bootstrap_state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    command: RtcWorkerCommand,
    routing_state: &mut PacketLoopRoutingState,
) {
    handle_worker_command(
        bootstrap_state,
        &WorkerCommandContext {
            bitrate_state,
            snapshot_state,
            relay_registry: &config.relay_registry,
            public_ip: config.public_ip,
            rtc_port_range: config.rtc_port_range,
            codec_flags: config.codec_flags,
            metrics: &config.metrics,
        },
        command,
    );
    routing_state.clear_on_topology_change();
}

#[cfg(test)]
fn handle_debug_worker_command_and_clear_routing_cache(
    bootstrap_state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    command: DebugRtcWorkerCommand,
    routing_state: &mut PacketLoopRoutingState,
) {
    handle_debug_worker_command(
        bootstrap_state,
        &WorkerCommandContext {
            bitrate_state,
            snapshot_state,
            relay_registry: &config.relay_registry,
            public_ip: config.public_ip,
            rtc_port_range: config.rtc_port_range,
            codec_flags: config.codec_flags,
            metrics: &config.metrics,
        },
        command,
    );
    routing_state.clear_on_topology_change();
}

fn snapshot_and_pump(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
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
    drain_ready_sessions(state, snapshot_state, &config.metrics, buffers, now);
    drain_relay_packets(
        relay_rx,
        &mut buffers.pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );
    flush_pending_keyframe_requests(state, &config.metrics, buffers);
    record_incoming_stats(state, bitrate_state, &config.metrics, buffers);
    populate_forward_routes(
        state,
        &config.media_tap,
        &config.relay_registry,
        &config.metrics,
        &mut buffers.pending_packets,
        &mut buffers.forwards,
    );
    flush_forward_routes(state, &config.metrics, buffers);
    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: state.next_timeout_deadline(),
    })
}

#[cfg(not(test))]
async fn wait_for_next_loop_input(
    snapshot: Option<SnapshotInfo>,
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    receive_buffer: &mut [u8],
    shutdown_token: &CancellationToken,
) -> Option<NextLoopInput> {
    let Some(info) = snapshot else {
        return tokio::select! {
            biased;
            () = shutdown_token.cancelled() => None,
            maybe_command = command_rx.recv() => maybe_command.map(NextLoopInput::Command),
        };
    };
    if let Some(next_timeout) = info.next_timeout {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => None,
            maybe_command = command_rx.recv() => {
                maybe_command.map(NextLoopInput::Command)
            }
            result = timeout(
                socket_wait_duration(next_timeout),
                info.socket.recv_from(receive_buffer),
            ) => {
                Some(match result {
                    Ok(result) => handle_socket_receive_result(result, info.candidate_addr),
                    Err(_elapsed) => NextLoopInput::Datagram {
                        source_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                        candidate_addr: info.candidate_addr,
                        received_size: 0,
                    },
                })
            }
        }
    } else {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => None,
            maybe_command = command_rx.recv() => {
                maybe_command.map(NextLoopInput::Command)
            }
            result = info.socket.recv_from(receive_buffer) => {
                Some(handle_socket_receive_result(result, info.candidate_addr))
            }
        }
    }
}

#[cfg(test)]
async fn wait_for_next_loop_input(
    snapshot: Option<SnapshotInfo>,
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    debug_rx: &mut mpsc::Receiver<DebugRtcWorkerCommand>,
    receive_buffer: &mut [u8],
    shutdown_token: &CancellationToken,
) -> Option<NextLoopInput> {
    let Some(info) = snapshot else {
        return tokio::select! {
            biased;
            () = shutdown_token.cancelled() => None,
            maybe_command = command_rx.recv() => maybe_command.map(NextLoopInput::Command),
            maybe_debug_command = debug_rx.recv() => maybe_debug_command.map(NextLoopInput::DebugCommand),
        };
    };
    if let Some(next_timeout) = info.next_timeout {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => None,
            maybe_command = command_rx.recv() => {
                maybe_command.map(NextLoopInput::Command)
            }
            maybe_debug_command = debug_rx.recv() => {
                maybe_debug_command.map(NextLoopInput::DebugCommand)
            }
            result = timeout(
                socket_wait_duration(next_timeout),
                info.socket.recv_from(receive_buffer),
            ) => {
                Some(match result {
                    Ok(result) => handle_socket_receive_result(result, info.candidate_addr),
                    Err(_elapsed) => NextLoopInput::Datagram {
                        source_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                        candidate_addr: info.candidate_addr,
                        received_size: 0,
                    },
                })
            }
        }
    } else {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => None,
            maybe_command = command_rx.recv() => {
                maybe_command.map(NextLoopInput::Command)
            }
            maybe_debug_command = debug_rx.recv() => {
                maybe_debug_command.map(NextLoopInput::DebugCommand)
            }
            result = info.socket.recv_from(receive_buffer) => {
                Some(handle_socket_receive_result(result, info.candidate_addr))
            }
        }
    }
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
            NextLoopInput::Datagram {
                source_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                candidate_addr,
                received_size: 0,
            }
        }
    }
}

fn socket_wait_duration(next_timeout: Instant) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(Instant::now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
