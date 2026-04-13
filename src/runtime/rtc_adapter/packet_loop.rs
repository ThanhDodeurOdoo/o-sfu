use std::{
    io::Error as IoError,
    mem::take,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use str0m::IceConnectionState;
use str0m::media::{MediaData, Mid};
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output};
use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use super::{
    commands::RtcWorkerCommand,
    state::{RtcBootstrapState, RtcSessionState, RtcSnapshotState, TransportSessionHealth},
    worker::handle_worker_command,
};
use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::TransportSessionKey;

const RECEIVE_BUFFER_LEN: usize = 2000;

#[derive(Debug)]
struct PendingTransmit {
    destination: SocketAddr,
    contents: Vec<u8>,
}

impl PendingTransmit {
    fn empty() -> Self {
        Self {
            destination: SocketAddr::from(([0, 0, 0, 0], 0)),
            contents: Vec::new(),
        }
    }

    fn overwrite(&mut self, destination: SocketAddr, contents: &[u8]) {
        self.destination = destination;
        self.contents.clear();
        self.contents.extend_from_slice(contents);
    }
}

/// Reusable buffers for the packet loop, allocated once and cleared per iteration
/// to avoids steady-state heap allocations
struct PacketLoopBuffers {
    pending_transmits: Vec<PendingTransmit>,
    pending_transmit_count: usize,
    pending_media: Vec<(TransportSessionKey, MediaData)>,
    forwards: Vec<(usize, TransportSessionKey, Mid)>,
}

impl PacketLoopBuffers {
    fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_transmit_count: 0,
            pending_media: Vec::with_capacity(32),
            forwards: Vec::with_capacity(64),
        }
    }

    fn clear(&mut self) {
        self.pending_transmit_count = 0;
        self.pending_media.clear();
        self.forwards.clear();
    }

    fn push_pending_transmit(&mut self, destination: SocketAddr, contents: &[u8]) {
        if let Some(slot) = self.pending_transmits.get_mut(self.pending_transmit_count) {
            slot.overwrite(destination, contents);
        } else {
            let mut slot = PendingTransmit::empty();
            slot.overwrite(destination, contents);
            self.pending_transmits.push(slot);
        }
        self.pending_transmit_count = self.pending_transmit_count.saturating_add(1);
    }

    fn pending_transmits(&self) -> impl Iterator<Item = &PendingTransmit> {
        self.pending_transmits
            .iter()
            .take(self.pending_transmit_count)
    }
}

struct SnapshotInfo {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<Instant>,
}

enum NextLoopInput {
    Command(RtcWorkerCommand),
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
}

pub(super) async fn run_packet_loop(
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    media_tap: Arc<MediaTap>,
    mut command_rx: mpsc::Receiver<RtcWorkerCommand>,
    shutdown_token: CancellationToken,
) {
    let mut bootstrap_state = RtcBootstrapState::default();
    let mut receive_buffer = vec![0_u8; RECEIVE_BUFFER_LEN];
    let mut buffers = PacketLoopBuffers::new();
    loop {
        while let Ok(command) = command_rx.try_recv() {
            handle_worker_command(
                &mut bootstrap_state,
                &snapshot_state,
                public_ip,
                rtc_port_range,
                codec_flags,
                command,
            );
        }
        let snapshot = snapshot_and_pump(
            &mut bootstrap_state,
            &snapshot_state,
            &media_tap,
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
            &mut receive_buffer,
            &shutdown_token,
        )
        .await
        else {
            return;
        };
        match next_input {
            NextLoopInput::Command(command) => {
                handle_worker_command(
                    &mut bootstrap_state,
                    &snapshot_state,
                    public_ip,
                    rtc_port_range,
                    codec_flags,
                    command,
                );
            }
            NextLoopInput::Datagram {
                source_addr,
                candidate_addr,
                received_size,
            } => {
                if received_size == 0 {
                    continue;
                }
                let Some(packet) = receive_buffer.get(..received_size) else {
                    continue;
                };
                route_incoming_packet(
                    &mut bootstrap_state,
                    &snapshot_state,
                    source_addr,
                    candidate_addr,
                    packet,
                );
            }
        }
    }
}

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

fn record_incoming_stats(
    state: &RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    media_tap: &MediaTap,
    buffers: &PacketLoopBuffers,
    now: Instant,
) {
    let Ok(mut snapshot) = snapshot_state.lock() else {
        return;
    };
    for (source_session, media) in &buffers.pending_media {
        if let Some(transport_media_id) =
            state.transport_media_id_for_source(source_session, media.mid)
        {
            snapshot.record_incoming_media(
                source_session,
                transport_media_id,
                now,
                media.data.len(),
            );
            media_tap.write_frame(source_session, transport_media_id, now, &media.data);
        }
    }
}

fn snapshot_and_pump(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    media_tap: &MediaTap,
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
    let ready_sessions = state.take_ready_sessions(now);
    for session_id in ready_sessions {
        let session_timeout = {
            let Some(session_state) = state.sessions.get_mut(&session_id) else {
                continue;
            };
            drain_single_session(&session_id, session_state, snapshot_state, buffers)
        };
        state.update_session_timeout(&session_id, session_timeout);
    }

    // Two-pass: collect matching routes, then write to destination sessions
    let media_stats_now = Instant::now();
    record_incoming_stats(state, snapshot_state, media_tap, buffers, media_stats_now);
    for (media_idx, (source_session, media)) in buffers.pending_media.iter().enumerate() {
        if let Some(route_entry) = state
            .media_route_index
            .get(&(source_session.clone(), media.mid))
        {
            if !route_entry.source_active {
                continue;
            }
            for dest in &route_entry.destinations {
                if !dest.active {
                    continue;
                }
                buffers
                    .forwards
                    .push((media_idx, dest.dest_session.clone(), dest.dest_mid));
            }
        }
    }
    for forward_idx in 0..buffers.forwards.len() {
        let Some((media_idx, dest_session, dest_mid)) =
            buffers
                .forwards
                .get(forward_idx)
                .map(|(media_idx, dest_session, dest_mid)| {
                    (*media_idx, dest_session.clone(), *dest_mid)
                })
        else {
            continue;
        };
        let is_last_destination = buffers
            .forwards
            .get(forward_idx + 1)
            .is_none_or(|(next_media_idx, _, _)| *next_media_idx != media_idx);
        let Some((_source_session, media)) = buffers.pending_media.get_mut(media_idx) else {
            continue;
        };
        let Some(dest_session_state) = state.sessions.get_mut(&dest_session) else {
            continue;
        };
        let Some(writer) = dest_session_state.rtc.writer(dest_mid) else {
            continue;
        };
        let Some(pt) = writer.match_params(media.params) else {
            continue;
        };
        let mut data_writer = writer;
        if let Some(rid) = media.rid {
            data_writer = data_writer.rid(rid);
        }
        if media.audio_start_of_talk_spurt {
            data_writer = data_writer.start_of_talkspurt(true);
        }
        if let Err(error) = data_writer.write(
            pt,
            media.network_time,
            media.time,
            take_write_payload(&mut media.data, is_last_destination),
        ) {
            warn!(
                ?dest_session,
                ?error,
                "failed to write media to destination session"
            );
        }
    }

    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: state.next_timeout_deadline(),
    })
}

/// Drain all ready outputs from a single session's `Rtc` instance.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    buffers: &mut PacketLoopBuffers,
) -> Option<Instant> {
    loop {
        let now = Instant::now();
        match session_state.rtc.poll_output() {
            Ok(Output::Transmit(transmit)) => {
                buffers.push_pending_transmit(transmit.destination, &transmit.contents);
            }
            Ok(Output::Event(Event::MediaData(data))) => {
                buffers.pending_media.push((session_key.clone(), data));
            }
            Ok(Output::Event(event)) => {
                observe_rtc_event(snapshot_state, session_key, &event);
                log_rtc_event(session_key, &event);
            }
            Ok(Output::Timeout(timeout_at)) => {
                if timeout_at <= now {
                    if session_state.rtc.handle_input(Input::Timeout(now)).is_err() {
                        warn!(
                            session_id = ?session_key.session_id(),
                            channel_runtime_id = session_key.channel_runtime_id(),
                            "failed to apply immediate rtc packet-loop timeout input"
                        );
                        return None;
                    }
                    continue;
                }
                return Some(timeout_at);
            }
            Err(error) => {
                warn!(
                    session_id = ?session_key.session_id(),
                    channel_runtime_id = session_key.channel_runtime_id(),
                    ?error,
                    "rtc packet loop failed while polling output"
                );
                return None;
            }
        }
    }
}

pub(super) fn take_write_payload(data: &mut Vec<u8>, is_last_destination: bool) -> Vec<u8> {
    if is_last_destination {
        take(data)
    } else {
        data.clone()
    }
}

fn log_rtc_event(session_key: &TransportSessionKey, event: &Event) {
    match event {
        Event::IceConnectionStateChange(state) => {
            debug!(
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                ?state,
                "rtc ICE connection state transition"
            );
        }
        Event::Connected => {
            debug!(
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                "rtc DTLS transport reached connected state"
            );
        }
        _ => {
            trace!(
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                ?event,
                "rtc packet loop event"
            );
        }
    }
}

fn observe_rtc_event(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    event: &Event,
) {
    let Some(health) = transport_health_from_event(event) else {
        return;
    };
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    snapshot_state.set_transport_health(session_key, health);
}

pub(super) fn transport_health_from_event(event: &Event) -> Option<TransportSessionHealth> {
    match event {
        Event::Connected => Some(TransportSessionHealth::Connected),
        Event::IceConnectionStateChange(state) => transport_health_from_ice_state(*state),
        _ => None,
    }
}

fn transport_health_from_ice_state(state: IceConnectionState) -> Option<TransportSessionHealth> {
    if state.is_connected() {
        Some(TransportSessionHealth::Connected)
    } else if state.is_disconnected() {
        Some(TransportSessionHealth::Disconnected)
    } else {
        None
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

fn route_incoming_packet(
    bootstrap_state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    route_packet_to_matching_session(
        bootstrap_state,
        snapshot_state,
        source_addr,
        candidate_addr,
        packet,
    );
}

fn route_packet_to_matching_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    if route_packet_with_cached_session(state, snapshot_state, source_addr, candidate_addr, packet)
    {
        return;
    }
    route_packet_by_scan(state, snapshot_state, source_addr, candidate_addr, packet);
}

fn route_packet_with_cached_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) -> bool {
    let Some(session_key) = state
        .remote_addr_demux
        .session_key_for_remote_addr(source_addr)
        .cloned()
    else {
        return false;
    };
    let Some(session_state) = state.sessions.get_mut(&session_key) else {
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return false;
    };
    let Ok(receive) = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet) else {
        log_malformed_datagram(source_addr);
        return true;
    };
    let now = Instant::now();
    let input = Input::Receive(now, receive);
    let accepts_input = session_state.rtc.accepts(&input);
    if !accepts_input {
        let _ = session_state;
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return false;
    }
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        warn!(
            session_id = ?session_key.session_id(),
            channel_runtime_id = session_key.channel_runtime_id(),
            "failed to feed indexed UDP datagram into rtc session state"
        );
    } else {
        state.mark_session_dirty(&session_key);
    }
    state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &session_key);
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key);
    }
    true
}

fn matching_session_key_for_packet(
    state: &RtcBootstrapState,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    now: Instant,
) -> Result<Option<TransportSessionKey>, ()> {
    for (session_key, session_state) in &state.sessions {
        let Some(input) = receive_input(now, source_addr, candidate_addr, packet) else {
            return Err(());
        };
        if session_state.rtc.accepts(&input) {
            return Ok(Some(session_key.clone()));
        }
    }
    Ok(None)
}

fn receive_input(
    now: Instant,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) -> Option<Input<'_>> {
    let receive = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet).ok()?;
    Some(Input::Receive(now, receive))
}

fn log_malformed_datagram(source_addr: SocketAddr) {
    trace!(
        source = %source_addr,
        "ignoring malformed UDP datagram in rtc packet loop"
    );
}

fn route_packet_by_scan(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    let now = Instant::now();
    let session_key =
        match matching_session_key_for_packet(state, source_addr, candidate_addr, packet, now) {
            Ok(Some(session_key)) => session_key,
            Ok(None) => {
                trace!(
                    source = %source_addr,
                    "dropping UDP datagram because no rtc session accepted it"
                );
                return;
            }
            Err(()) => {
                log_malformed_datagram(source_addr);
                return;
            }
        };
    let Some(session_state) = state.sessions.get_mut(&session_key) else {
        return;
    };
    let Some(input) = receive_input(now, source_addr, candidate_addr, packet) else {
        log_malformed_datagram(source_addr);
        return;
    };
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        warn!(
            session_id = ?session_key.session_id(),
            channel_runtime_id = session_key.channel_runtime_id(),
            "failed to feed incoming UDP datagram into rtc session state"
        );
    } else {
        state.mark_session_dirty(&session_key);
    }
    state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &session_key);
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key);
    }
}
