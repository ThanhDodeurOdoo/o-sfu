use std::{
    collections::{HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
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
use crate::runtime::metrics::{
    RtcDatagramDropReason, RtcDatagramRoutePath, RuntimeMetrics, TransportIceState,
};
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::TransportSessionKey;

const RECEIVE_BUFFER_LEN: usize = 2000;
const RECENT_MISS_CACHE_LIMIT: usize = 256;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PacketLoopRoutingMissKey {
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet_len: usize,
    packet_hash: u64,
}

impl PacketLoopRoutingMissKey {
    fn new(source_addr: SocketAddr, candidate_addr: SocketAddr, packet: &[u8]) -> Self {
        let mut hasher = DefaultHasher::new();
        packet.hash(&mut hasher);
        Self {
            source_addr,
            candidate_addr,
            packet_len: packet.len(),
            packet_hash: hasher.finish(),
        }
    }
}

#[derive(Default)]
struct PacketLoopRoutingMissCache {
    entries: VecDeque<PacketLoopRoutingMissKey>,
    entry_set: HashSet<PacketLoopRoutingMissKey>,
}

impl PacketLoopRoutingMissCache {
    fn clear(&mut self) {
        self.entries.clear();
        self.entry_set.clear();
    }

    fn contains(&self, key: PacketLoopRoutingMissKey) -> bool {
        self.entry_set.contains(&key)
    }

    fn record(&mut self, key: PacketLoopRoutingMissKey) {
        if !self.entry_set.insert(key) {
            return;
        }
        self.entries.push_back(key);
        while self.entries.len() > RECENT_MISS_CACHE_LIMIT {
            let Some(evicted_key) = self.entries.pop_front() else {
                break;
            };
            self.entry_set.remove(&evicted_key);
        }
    }

    fn forget(&mut self, key: PacketLoopRoutingMissKey) {
        if !self.entry_set.remove(&key) {
            return;
        }
        if let Some(position) = self.entries.iter().position(|candidate| *candidate == key) {
            let _ = self.entries.remove(position);
        }
    }
}

struct PacketLoopRoutingState {
    miss_cache: PacketLoopRoutingMissCache,
    #[cfg(test)]
    scan_attempts: usize,
}

impl PacketLoopRoutingState {
    fn new() -> Self {
        Self {
            miss_cache: PacketLoopRoutingMissCache::default(),
            #[cfg(test)]
            scan_attempts: 0,
        }
    }

    fn clear_on_topology_change(&mut self) {
        self.miss_cache.clear();
    }

    fn should_skip_scan(&self, miss_key: PacketLoopRoutingMissKey) -> bool {
        self.miss_cache.contains(miss_key)
    }

    fn record_miss(&mut self, miss_key: PacketLoopRoutingMissKey) {
        self.miss_cache.record(miss_key);
    }

    fn forget_miss(&mut self, miss_key: PacketLoopRoutingMissKey) {
        self.miss_cache.forget(miss_key);
    }

    #[cfg(test)]
    fn scan_attempts(&self) -> usize {
        self.scan_attempts
    }
}

enum NextLoopInput {
    Command(RtcWorkerCommand),
    Datagram {
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_size: usize,
    },
}

enum CachedRouteOutcome {
    Routed,
    NotMatched,
    Malformed,
}

enum PacketScanOutcome {
    Matched {
        session_key: TransportSessionKey,
        examined_sessions: usize,
    },
    NoMatch {
        examined_sessions: usize,
    },
    Malformed,
}

pub(super) struct PacketLoopConfig {
    pub(super) public_ip: IpAddr,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) media_tap: Arc<MediaTap>,
    pub(super) metrics: Arc<RuntimeMetrics>,
}

pub(super) async fn run_packet_loop(
    config: PacketLoopConfig,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut command_rx: mpsc::Receiver<RtcWorkerCommand>,
    shutdown_token: CancellationToken,
) {
    let mut bootstrap_state = RtcBootstrapState::default();
    let mut routing_state = PacketLoopRoutingState::new();
    let mut receive_buffer = vec![0_u8; RECEIVE_BUFFER_LEN];
    let mut buffers = PacketLoopBuffers::new();
    loop {
        while let Ok(command) = command_rx.try_recv() {
            handle_worker_command_and_clear_routing_cache(
                &mut bootstrap_state,
                &snapshot_state,
                &config,
                command,
                &mut routing_state,
            );
        }
        let snapshot =
            snapshot_and_pump(&mut bootstrap_state, &snapshot_state, &config, &mut buffers);
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
                handle_worker_command_and_clear_routing_cache(
                    &mut bootstrap_state,
                    &snapshot_state,
                    &config,
                    command,
                    &mut routing_state,
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
                    &mut routing_state,
                    &config.metrics,
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
    metrics: &RuntimeMetrics,
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
            metrics.record_rtp_ingress(media.data.len());
            media_tap.write_frame(source_session, transport_media_id, now, &media.data);
        }
    }
}

fn handle_worker_command_and_clear_routing_cache(
    bootstrap_state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    command: RtcWorkerCommand,
    routing_state: &mut PacketLoopRoutingState,
) {
    handle_worker_command(
        bootstrap_state,
        snapshot_state,
        config.public_ip,
        config.rtc_port_range,
        config.codec_flags,
        &config.metrics,
        command,
    );
    routing_state.clear_on_topology_change();
}

fn snapshot_and_pump(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
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
            drain_single_session(
                &session_id,
                session_state,
                snapshot_state,
                &config.metrics,
                buffers,
            )
        };
        state.update_session_timeout(&session_id, session_timeout);
    }

    // Two-pass: collect matching routes, then write to destination sessions
    let media_stats_now = Instant::now();
    record_incoming_stats(
        state,
        snapshot_state,
        &config.media_tap,
        &config.metrics,
        buffers,
        media_stats_now,
    );
    populate_forward_routes(state, buffers);
    flush_forward_routes(state, &config.metrics, buffers);

    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: state.next_timeout_deadline(),
    })
}

fn populate_forward_routes(state: &RtcBootstrapState, buffers: &mut PacketLoopBuffers) {
    for (media_idx, (source_session, media)) in buffers.pending_media.iter().enumerate() {
        let Some(route_entry) = state
            .media_route_index
            .get(&(source_session.clone(), media.mid))
        else {
            continue;
        };
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

fn flush_forward_routes(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
) {
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
        let payload_len = media.data.len();
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
        } else {
            metrics.record_rtp_egress(payload_len);
        }
    }
}

/// Drain all ready outputs from a single session's `Rtc` instance.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
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
                observe_rtc_event(snapshot_state, metrics, session_key, &event);
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
    metrics: &RuntimeMetrics,
    session_key: &TransportSessionKey,
    event: &Event,
) {
    match event {
        Event::IceConnectionStateChange(state) => {
            metrics.record_transport_ice_state_change(transport_ice_state(*state));
        }
        Event::Connected => {
            metrics.record_transport_dtls_connected();
        }
        _ => {}
    }
    let Some(health) = transport_health_from_event(event) else {
        return;
    };
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    let previous = snapshot_state.set_transport_health(session_key, health);
    metrics.record_transport_health_transition(previous, Some(health));
}

pub(super) fn transport_ice_state(state: IceConnectionState) -> TransportIceState {
    match state {
        IceConnectionState::New => TransportIceState::New,
        IceConnectionState::Checking => TransportIceState::Checking,
        IceConnectionState::Connected => TransportIceState::Connected,
        IceConnectionState::Completed => TransportIceState::Completed,
        IceConnectionState::Disconnected => TransportIceState::Disconnected,
    }
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
    routing_state: &mut PacketLoopRoutingState,
    metrics: &RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    route_packet_to_matching_session(
        bootstrap_state,
        snapshot_state,
        routing_state,
        metrics,
        source_addr,
        candidate_addr,
        packet,
    );
}

fn route_packet_to_matching_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    routing_state: &mut PacketLoopRoutingState,
    metrics: &RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
    match route_packet_with_cached_session(
        state,
        snapshot_state,
        source_addr,
        candidate_addr,
        packet,
    ) {
        CachedRouteOutcome::Routed => {
            metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
            routing_state.forget_miss(miss_key);
            return;
        }
        CachedRouteOutcome::Malformed => {
            metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            return;
        }
        CachedRouteOutcome::NotMatched => {}
    }
    if routing_state.should_skip_scan(miss_key) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
        trace!(
            source = %source_addr,
            "dropping UDP datagram because a recent cache miss already proved no rtc session accepted it"
        );
        return;
    }
    route_packet_by_scan(
        state,
        snapshot_state,
        routing_state,
        metrics,
        source_addr,
        candidate_addr,
        packet,
    );
}

fn route_packet_with_cached_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) -> CachedRouteOutcome {
    let Some(session_key) = state
        .remote_addr_demux
        .session_key_for_remote_addr(source_addr)
        .cloned()
    else {
        return CachedRouteOutcome::NotMatched;
    };
    let Some(session_state) = state.sessions.get_mut(&session_key) else {
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return CachedRouteOutcome::NotMatched;
    };
    let Ok(receive) = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet) else {
        log_malformed_datagram(source_addr);
        return CachedRouteOutcome::Malformed;
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
        return CachedRouteOutcome::NotMatched;
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
    if state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &session_key)
        && let Ok(mut snapshot) = snapshot_state.lock()
    {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key);
    }
    CachedRouteOutcome::Routed
}

fn matching_session_key_for_packet(
    state: &RtcBootstrapState,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    now: Instant,
) -> PacketScanOutcome {
    let Some(input) = receive_input(now, source_addr, candidate_addr, packet) else {
        return PacketScanOutcome::Malformed;
    };
    let mut examined_sessions: usize = 0;
    for (session_key, session_state) in &state.sessions {
        examined_sessions = examined_sessions.saturating_add(1);
        if session_state.rtc.accepts(&input) {
            return PacketScanOutcome::Matched {
                session_key: session_key.clone(),
                examined_sessions,
            };
        }
    }
    PacketScanOutcome::NoMatch { examined_sessions }
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
    routing_state: &mut PacketLoopRoutingState,
    metrics: &RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    #[cfg(test)]
    {
        routing_state.scan_attempts = routing_state.scan_attempts.saturating_add(1);
    }
    let now = Instant::now();
    let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
    let session_key =
        match matching_session_key_for_packet(state, source_addr, candidate_addr, packet, now) {
            PacketScanOutcome::Matched {
                session_key,
                examined_sessions,
            } => {
                metrics.record_rtc_datagram_fallback_scan(examined_sessions);
                session_key
            }
            PacketScanOutcome::NoMatch { examined_sessions } => {
                metrics.record_rtc_datagram_fallback_scan(examined_sessions);
                metrics.record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
                routing_state.record_miss(miss_key);
                trace!(
                    source = %source_addr,
                    "dropping UDP datagram because no rtc session accepted it"
                );
                return;
            }
            PacketScanOutcome::Malformed => {
                metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
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
    if state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &session_key)
        && let Ok(mut snapshot) = snapshot_state.lock()
    {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key);
    }
    metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    routing_state.forget_miss(miss_key);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_rtp_packet(sequence_number: u16, ssrc: u32) -> Vec<u8> {
        let sequence_number = sequence_number.to_be_bytes();
        let ssrc = ssrc.to_be_bytes();
        vec![
            0x80,
            96,
            sequence_number[0],
            sequence_number[1],
            0,
            0,
            0,
            1,
            ssrc[0],
            ssrc[1],
            ssrc[2],
            ssrc[3],
        ]
    }

    #[test]
    fn recent_miss_cache_skips_repeated_scans_for_the_same_source() {
        let mut bootstrap_state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let mut routing_state = PacketLoopRoutingState::new();
        let metrics = RuntimeMetrics::default();
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_001));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_000));
        let packet = valid_rtp_packet(1, 11);

        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &packet,
        );
        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &packet,
        );

        assert_eq!(routing_state.scan_attempts(), 1);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 1);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 1);
        assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 1);
        assert_eq!(snapshot.rtc_datagram_drops_malformed, 0);
    }

    #[test]
    fn recent_miss_cache_clears_on_topology_change() {
        let mut bootstrap_state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let mut routing_state = PacketLoopRoutingState::new();
        let metrics = RuntimeMetrics::default();
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_011));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_010));
        let packet = valid_rtp_packet(2, 22);

        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &packet,
        );
        routing_state.clear_on_topology_change();
        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &packet,
        );

        assert_eq!(routing_state.scan_attempts(), 2);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 2);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 2);
        assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
    }

    #[test]
    fn recent_miss_cache_does_not_skip_different_packets_from_the_same_source() {
        let mut bootstrap_state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let mut routing_state = PacketLoopRoutingState::new();
        let metrics = RuntimeMetrics::default();
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_021));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_020));

        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &valid_rtp_packet(3, 33),
        );
        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &valid_rtp_packet(4, 44),
        );

        assert_eq!(routing_state.scan_attempts(), 2);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 2);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 2);
        assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
    }

    #[test]
    fn malformed_udp_datagram_counts_as_malformed_drop_without_scan_metrics() {
        let mut bootstrap_state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let mut routing_state = PacketLoopRoutingState::new();
        let metrics = RuntimeMetrics::default();
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_031));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_030));

        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &[0x01, 0x02, 0x03],
        );

        let snapshot = metrics.snapshot();
        assert_eq!(routing_state.scan_attempts(), 1);
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 0);
        assert_eq!(snapshot.rtc_datagram_scan_sessions, 0);
        assert_eq!(snapshot.rtc_datagram_drops_malformed, 1);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 0);
    }
}
