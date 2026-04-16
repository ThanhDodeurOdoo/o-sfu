use std::{
    collections::{BTreeMap, VecDeque},
    io::Error as IoError,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use str0m::IceConnectionState;
use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output};
use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use super::{
    commands::RtcWorkerCommand,
    forwarded_packet::ForwardedPacket,
    forwarding_destination::PacketForward,
    forwarding_planner::populate_forward_routes,
    relay_registry::RelayRegistry,
    route_control::{KeyframeRequestDecision, coalesce_keyframe_kind},
    state::{RtcBootstrapState, RtcSessionState, RtcSnapshotState, TransportSessionHealth},
    worker::{WorkerCommandContext, handle_worker_command, request_keyframe_for_source},
};
use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::metrics::{
    RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome, RuntimeMetrics,
    TransportIceState,
};
use crate::runtime::recording::MediaTap;
use crate::runtime::rtc_adapter::media_registry::RegisteredMediaHandle;
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

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
    pending_packets: Vec<ForwardedPacket>,
    pending_keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    forwards: Vec<PacketForward>,
}

impl PacketLoopBuffers {
    fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_transmit_count: 0,
            pending_packets: Vec::with_capacity(32),
            pending_keyframe_requests: Vec::with_capacity(8),
            forwards: Vec::with_capacity(64),
        }
    }

    fn clear(&mut self) {
        self.pending_transmit_count = 0;
        self.pending_packets.clear();
        self.pending_keyframe_requests.clear();
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

#[derive(Debug, Clone, Copy)]
struct PendingKeyframeRequest {
    consumer_mid: Mid,
    consumer_rid: Option<Rid>,
    kind: KeyframeRequestKind,
}

impl PendingKeyframeRequest {
    fn new(request: KeyframeRequest) -> Self {
        Self {
            consumer_mid: request.mid,
            consumer_rid: request.rid,
            kind: request.kind,
        }
    }
}

#[derive(Clone)]
enum ResolvedKeyframeRoute {
    Local {
        source_session_key: TransportSessionKey,
    },
    Remote {
        source_session_key: TransportSessionKey,
        source_control: super::commands::RemoteSourceControl,
    },
}

#[derive(Clone)]
struct CoalescedKeyframeRequest {
    source_transport_media_id: TransportMediaId,
    route: ResolvedKeyframeRoute,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
}

impl CoalescedKeyframeRequest {
    fn new(
        source_transport_media_id: TransportMediaId,
        route: ResolvedKeyframeRoute,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    ) -> Self {
        Self {
            source_transport_media_id,
            route,
            rid,
            kind,
        }
    }

    fn coalesce(&mut self, rid: Option<Rid>, kind: KeyframeRequestKind) {
        self.rid = self.rid.or(rid);
        self.kind = coalesce_keyframe_kind(self.kind, kind);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PacketLoopRoutingMissKey {
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet_len: usize,
    packet_fingerprint: u64,
}

impl PacketLoopRoutingMissKey {
    fn new(source_addr: SocketAddr, candidate_addr: SocketAddr, packet: &[u8]) -> Self {
        Self {
            source_addr,
            candidate_addr,
            packet_len: packet.len(),
            packet_fingerprint: packet_fingerprint(packet),
        }
    }
}

fn packet_fingerprint(packet: &[u8]) -> u64 {
    fn load_u64(bytes: &[u8]) -> u64 {
        let mut buffer = [0_u8; 8];
        for (slot, byte) in buffer.iter_mut().zip(bytes.iter().copied()) {
            *slot = byte;
        }
        u64::from_le_bytes(buffer)
    }

    let len = u64::try_from(packet.len()).map_or(u64::MAX, |len| len);
    let prefix = load_u64(packet);
    let suffix = load_u64(
        packet
            .get(packet.len().saturating_sub(8)..)
            .unwrap_or(packet),
    );
    len.rotate_left(17) ^ prefix.rotate_left(29) ^ suffix.rotate_left(43)
}

#[derive(Debug, Clone)]
struct PacketLoopRoutingMissRecord {
    key: PacketLoopRoutingMissKey,
    packet: Box<[u8]>,
}

#[derive(Default)]
struct PacketLoopRoutingMissCache {
    entries: VecDeque<PacketLoopRoutingMissRecord>,
}

impl PacketLoopRoutingMissCache {
    fn clear(&mut self) {
        self.entries.clear();
    }

    fn contains(&self, key: PacketLoopRoutingMissKey, packet: &[u8]) -> bool {
        self.entries
            .iter()
            .any(|candidate| candidate.key == key && candidate.packet.as_ref() == packet)
    }

    fn record(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        if self.contains(key, packet) {
            return;
        }
        self.entries.push_back(PacketLoopRoutingMissRecord {
            key,
            packet: packet.to_vec().into_boxed_slice(),
        });
        while self.entries.len() > RECENT_MISS_CACHE_LIMIT {
            let Some(_) = self.entries.pop_front() else {
                break;
            };
        }
    }

    fn forget(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        let Some(position) = self
            .entries
            .iter()
            .position(|candidate| candidate.key == key && candidate.packet.as_ref() == packet)
        else {
            return;
        };
        let _ = self.entries.remove(position);
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

    fn should_skip_scan(&self, miss_key: PacketLoopRoutingMissKey, packet: &[u8]) -> bool {
        self.miss_cache.contains(miss_key, packet)
    }

    fn record_miss(&mut self, miss_key: PacketLoopRoutingMissKey, packet: &[u8]) {
        self.miss_cache.record(miss_key, packet);
    }

    fn forget_miss(&mut self, miss_key: PacketLoopRoutingMissKey, packet: &[u8]) {
        self.miss_cache.forget(miss_key, packet);
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
    pub(super) relay_registry: Arc<RelayRegistry>,
    pub(super) metrics: Arc<RuntimeMetrics>,
}

pub(super) async fn run_packet_loop(
    config: PacketLoopConfig,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut command_rx: mpsc::Receiver<RtcWorkerCommand>,
    mut relay_rx: mpsc::UnboundedReceiver<ForwardedPacket>,
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
        let snapshot = snapshot_and_pump(
            &mut bootstrap_state,
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
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
    buffers: &PacketLoopBuffers,
) {
    let Ok(mut snapshot) = snapshot_state.lock() else {
        return;
    };
    for packet in &buffers.pending_packets {
        if let Some(transport_media_id) = packet.resolve_source_transport_media_id(state) {
            state.route_control.observe_audio_activity(
                transport_media_id,
                packet.route_control_voice_activity(),
                packet.route_control_audio_level(),
                packet.received_at(),
            );
            snapshot.record_incoming_media(
                packet.source_session_key(),
                transport_media_id,
                packet.received_at(),
                packet.payload_len(),
            );
            metrics.record_rtp_ingress(packet.payload_len());
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
        &WorkerCommandContext {
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
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: &PacketLoopConfig,
    relay_rx: &mut mpsc::UnboundedReceiver<ForwardedPacket>,
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

    drain_relay_packets(relay_rx, &mut buffers.pending_packets);
    flush_pending_keyframe_requests(state, &config.metrics, buffers);

    // Two-pass: collect matching routes, then write to destination sessions
    record_incoming_stats(state, snapshot_state, &config.metrics, buffers);
    populate_forward_routes(
        state,
        &config.media_tap,
        &config.relay_registry,
        &config.metrics,
        &buffers.pending_packets,
        &mut buffers.forwards,
    );
    flush_forward_routes(state, &config.metrics, buffers);

    Some(SnapshotInfo {
        socket,
        candidate_addr,
        next_timeout: state.next_timeout_deadline(),
    })
}

fn drain_relay_packets(
    relay_rx: &mut mpsc::UnboundedReceiver<ForwardedPacket>,
    pending_packets: &mut Vec<ForwardedPacket>,
) {
    while let Ok(packet) = relay_rx.try_recv() {
        pending_packets.push(packet);
    }
}

fn flush_pending_keyframe_requests(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    let mut coalesced_requests = BTreeMap::new();
    for (consumer_session_key, request) in buffers.pending_keyframe_requests.drain(..) {
        let Some(source_transport_media_id) = state.consumer_source_transport_media_id_for_mid(
            &consumer_session_key,
            request.consumer_mid,
        ) else {
            continue;
        };
        let Some(route) = resolve_keyframe_route(state, source_transport_media_id) else {
            continue;
        };
        coalesced_requests
            .entry(source_transport_media_id)
            .and_modify(|coalesced: &mut CoalescedKeyframeRequest| {
                coalesced.coalesce(request.consumer_rid, request.kind);
            })
            .or_insert_with(|| {
                CoalescedKeyframeRequest::new(
                    source_transport_media_id,
                    route,
                    request.consumer_rid,
                    request.kind,
                )
            });
    }
    let now = Instant::now();
    for coalesced_request in coalesced_requests.into_values() {
        match coalesced_request.route {
            ResolvedKeyframeRoute::Local { source_session_key } => request_keyframe_for_source(
                state,
                metrics,
                &source_session_key,
                coalesced_request.source_transport_media_id,
                coalesced_request.rid,
                coalesced_request.kind,
                now,
            ),
            ResolvedKeyframeRoute::Remote {
                source_session_key,
                source_control,
            } => {
                match state
                    .route_control
                    .decide_keyframe_request(coalesced_request.source_transport_media_id, now)
                {
                    KeyframeRequestDecision::Forward => {
                        source_control.request_keyframe(
                            source_session_key,
                            coalesced_request.source_transport_media_id,
                            coalesced_request.rid,
                            coalesced_request.kind,
                        );
                        metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
                    }
                    KeyframeRequestDecision::Absorb => {
                        metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
                    }
                }
            }
        }
    }
}

fn resolve_keyframe_route(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<ResolvedKeyframeRoute> {
    if let Some(handle) = state
        .mid_registry
        .get(&source_transport_media_id.as_u64())
        .cloned()
        && let RegisteredMediaHandle::Producer { session_key, .. } = handle
    {
        return Some(ResolvedKeyframeRoute::Local {
            source_session_key: session_key,
        });
    }
    state
        .remote_source_registration(source_transport_media_id)
        .cloned()
        .map(|remote_source| ResolvedKeyframeRoute::Remote {
            source_session_key: remote_source.source_session_key().clone(),
            source_control: remote_source.source_control().clone(),
        })
}

fn flush_forward_routes(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
) {
    let (forwards, pending_packets) = (&buffers.forwards, &mut buffers.pending_packets);
    for (forward_idx, forward) in forwards.iter().enumerate() {
        let is_last_destination = forwards
            .get(forward_idx + 1)
            .is_none_or(|next_forward| next_forward.packet_idx() != forward.packet_idx());
        let Some(packet) = pending_packets.get_mut(forward.packet_idx()) else {
            continue;
        };
        let destination = forward.destination();
        match destination.send(state, packet, is_last_destination) {
            Ok(Some(payload_len)) => metrics.record_rtp_egress(payload_len),
            Ok(None) => {}
            Err(error) => {
                warn!(
                    ?destination,
                    ?error,
                    "failed to write media to destination session"
                );
            }
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
            Ok(Output::Event(Event::RtpPacket(packet))) => {
                buffers
                    .pending_packets
                    .push(ForwardedPacket::from_rtp_packet(
                        session_key.clone(),
                        packet,
                    ));
            }
            Ok(Output::Event(Event::KeyframeRequest(request))) => {
                buffers
                    .pending_keyframe_requests
                    .push((session_key.clone(), PendingKeyframeRequest::new(request)));
                trace!(
                    session_id = ?session_key.session_id(),
                    media_worker_id = session_key.media_worker_id(),
                    mid = %request.mid,
                    rid = ?request.rid,
                    kind = ?request.kind,
                    "queued route-level keyframe request from rtc packet-loop event"
                );
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
                            media_worker_id = session_key.media_worker_id(),
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
                    media_worker_id = session_key.media_worker_id(),
                    ?error,
                    "rtc packet loop failed while polling output"
                );
                return None;
            }
        }
    }
}

fn log_rtc_event(session_key: &TransportSessionKey, event: &Event) {
    match event {
        Event::IceConnectionStateChange(state) => {
            debug!(
                session_id = ?session_key.session_id(),
                media_worker_id = session_key.media_worker_id(),
                ?state,
                "rtc ICE connection state transition"
            );
        }
        Event::Connected => {
            debug!(
                session_id = ?session_key.session_id(),
                media_worker_id = session_key.media_worker_id(),
                "rtc DTLS transport reached connected state"
            );
        }
        _ => {
            trace!(
                session_id = ?session_key.session_id(),
                media_worker_id = session_key.media_worker_id(),
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
            routing_state.forget_miss(miss_key, packet);
            return;
        }
        CachedRouteOutcome::Malformed => {
            metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            return;
        }
        CachedRouteOutcome::NotMatched => {}
    }
    if routing_state.should_skip_scan(miss_key, packet) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
        trace!(
            source = %source_addr,
            "dropping UDP datagram because a recent cache miss already proved no rtc session accepted it"
        );
        return;
    }
    let route = PacketRouteContext {
        snapshot_state,
        metrics,
        source_addr,
        candidate_addr,
        packet,
        now: Instant::now(),
    };
    if state.sessions.len() == 1 {
        route_packet_by_single_session(state, routing_state, miss_key, &route);
        return;
    }
    route_packet_by_scan(state, routing_state, miss_key, &route);
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
            media_worker_id = session_key.media_worker_id(),
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

struct PacketRouteContext<'a> {
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    metrics: &'a RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    now: Instant,
}

fn route_packet_to_session(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    route: &PacketRouteContext<'_>,
) -> bool {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return false;
    };
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        log_malformed_datagram(route.source_addr);
        return false;
    };
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        warn!(
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "failed to feed incoming UDP datagram into rtc session state"
        );
    } else {
        state.mark_session_dirty(session_key);
    }
    if state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key)
        && let Ok(mut snapshot) = route.snapshot_state.lock()
    {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(route.source_addr, session_key);
    }
    route
        .metrics
        .record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    true
}

fn route_packet_by_single_session(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    {
        routing_state.scan_attempts = routing_state.scan_attempts.saturating_add(1);
    }
    let Some(session_key) = state.sessions.keys().next().cloned() else {
        return;
    };
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        route
            .metrics
            .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
        log_malformed_datagram(route.source_addr);
        return;
    };
    let accepts_input = state
        .sessions
        .get(&session_key)
        .is_some_and(|session_state| session_state.rtc.accepts(&input));
    route.metrics.record_rtc_datagram_fallback_scan(1);
    if !accepts_input {
        route
            .metrics
            .record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
        routing_state.record_miss(miss_key, route.packet);
        trace!(
            source = %route.source_addr,
            "dropping UDP datagram because no rtc session accepted it"
        );
        return;
    }
    if route_packet_to_session(state, &session_key, route) {
        routing_state.forget_miss(miss_key, route.packet);
    }
}

fn route_packet_by_scan(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    {
        routing_state.scan_attempts = routing_state.scan_attempts.saturating_add(1);
    }
    let session_key =
        match matching_session_key_for_packet(
            state,
            route.source_addr,
            route.candidate_addr,
            route.packet,
            route.now,
        ) {
            PacketScanOutcome::Matched {
                session_key,
                examined_sessions,
            } => {
                route
                    .metrics
                    .record_rtc_datagram_fallback_scan(examined_sessions);
                session_key
            }
            PacketScanOutcome::NoMatch { examined_sessions } => {
                route
                    .metrics
                    .record_rtc_datagram_fallback_scan(examined_sessions);
                route
                    .metrics
                    .record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
                routing_state.record_miss(miss_key, route.packet);
                trace!(
                    source = %route.source_addr,
                    "dropping UDP datagram because no rtc session accepted it"
                );
                return;
            }
            PacketScanOutcome::Malformed => {
                route
                    .metrics
                    .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
                log_malformed_datagram(route.source_addr);
                return;
            }
        };
    if route_packet_to_session(state, &session_key, route) {
        routing_state.forget_miss(miss_key, route.packet);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use str0m::media::{KeyframeRequestKind, MediaKind, Mid};
    use str0m::rtp::Ssrc;
    use tokio::sync::mpsc;

    use super::*;
    use crate::config::MediaCodecFlags;
    use crate::runtime::recording::{MediaPacketSink, MediaSource, MediaTap, into_packet_sink};
    use crate::runtime::rtc_adapter::{
        bootstrap,
        commands::{RemoteSourceControl, RtcWorkerCommand},
        demux::{MediaRouteDestination, MediaRouteEntry},
        media_registry::RegisteredMediaHandle,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
        sample_forwarded_packet, sample_forwarded_packet_with_audio_activity,
    };
    use crate::runtime::transport_adapter::TransportMediaId;
    use crate::signaling::shared::SessionId;

    struct CountingSink {
        packets: AtomicUsize,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                packets: AtomicUsize::new(0),
            }
        }
    }

    impl MediaPacketSink for CountingSink {
        fn record_packet(
            &self,
            _session_key: &TransportSessionKey,
            _transport_media_id: TransportMediaId,
            _received_at: Instant,
            _payload: &[u8],
        ) {
            self.packets.fetch_add(1, Ordering::Relaxed);
        }
    }

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

    #[test]
    fn recording_forward_destination_captures_packets_without_bypassing_the_contract() {
        let producer_session = TransportSessionKey::new(18, 0, 19, SessionId::Integer(20));
        let mut state = RtcBootstrapState::default();
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let sink = Arc::new(CountingSink::new());
        let _source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("aud-up"),
            });
        let mut buffers = PacketLoopBuffers::new();
        let metrics = RuntimeMetrics::default();

        media_tap.activate_channel(
            producer_session.channel_runtime_id(),
            into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        );
        buffers.pending_packets.push(sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        ));

        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &buffers.pending_packets,
            &mut buffers.forwards,
        );
        flush_forward_routes(&mut state, &metrics, &mut buffers);

        assert_eq!(buffers.forwards.len(), 1);
        assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.snapshot().rtp_payload_bytes_egress, 0);
    }

    #[test]
    fn silent_audio_packets_are_dropped_from_routed_fanout_after_transport_activity_tracking() {
        let producer_session = TransportSessionKey::new(28, 0, 29, SessionId::Integer(30));
        let consumer_session = TransportSessionKey::new(28, 0, 31, SessionId::Integer(32));
        let mut state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let media_tap = MediaTap::default();
        let relay_registry = RelayRegistry::default();
        let metrics = RuntimeMetrics::default();
        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: producer_session.clone(),
                mid: Mid::from("aud-up"),
            });
        let consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: Mid::from("aud-down"),
                source_transport_media_id,
            });
        state.media_route_index.insert(
            source_transport_media_id,
            MediaRouteEntry {
                source_active: true,
                destinations: vec![MediaRouteDestination {
                    dest_session: consumer_session,
                    dest_transport_media_id: consumer_transport_media_id,
                    dest_mid: Mid::from("aud-down"),
                    active: true,
                    packet_gate: PacketLayerGate::Open,
                }],
            },
        );
        let mut buffers = PacketLoopBuffers::new();
        buffers
            .pending_packets
            .push(sample_forwarded_packet_with_audio_activity(
                producer_session,
                "aud-up",
                Some(false),
                Some(-72),
                b"payload",
            ));

        record_incoming_stats(&mut state, &snapshot_state, &metrics, &buffers);
        populate_forward_routes(
            &state,
            &media_tap,
            &relay_registry,
            &metrics,
            &buffers.pending_packets,
            &mut buffers.forwards,
        );

        assert!(buffers.forwards.is_empty());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_layer_dropped, 1);
        assert_eq!(snapshot.rtc_route_control_layer_allowed, 0);
    }

    #[test]
    fn drain_relay_packets_ingests_owned_forwarded_packets_from_the_mailbox() {
        let source_session = TransportSessionKey::new(25, 0, 26, SessionId::Integer(27));
        let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload");
        let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
        let mut pending_packets = Vec::new();

        mailbox.forward_packet(&packet, TransportMediaId::new(17));
        drain_relay_packets(&mut relay_rx, &mut pending_packets);

        assert_eq!(pending_packets.len(), 1);
        let forwarded = pending_packets.first();
        assert!(forwarded.is_some());
        let Some(forwarded) = forwarded else {
            return;
        };
        assert_eq!(forwarded.source_session_key(), &source_session);
        assert_eq!(forwarded.payload().as_slice(), b"payload");
        assert_eq!(
            forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default()),
            Some(TransportMediaId::new(17))
        );
    }

    #[test]
    fn flush_pending_keyframe_requests_marks_local_source_sessions_dirty() {
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_050));
        let source_session = TransportSessionKey::new(61, 0, 62, SessionId::Integer(63));
        let consumer_session = TransportSessionKey::new(61, 0, 64, SessionId::Integer(65));
        let source_mid = Mid::from("cam-up");
        let consumer_mid = Mid::from("cam-down");
        let mut state = RtcBootstrapState::default();
        let mut buffers = PacketLoopBuffers::new();
        let metrics = RuntimeMetrics::default();

        assert!(
            bootstrap::ensure_session_rtc_state(
                &mut state.sessions,
                &source_session,
                candidate_addr,
                MediaCodecFlags::default(),
            )
            .is_ok()
        );
        let Some(source_session_state) = state.sessions.get_mut(&source_session) else {
            return;
        };
        let mut direct_api = source_session_state.rtc.direct_api();
        direct_api.declare_media(source_mid, MediaKind::Video);
        direct_api.expect_stream_rx(Ssrc::from(44_444_u32), None, source_mid, None);

        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: source_session.clone(),
                mid: source_mid,
            });
        let _consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: consumer_mid,
                source_transport_media_id,
            });
        buffers.pending_keyframe_requests.push((
            consumer_session,
            PendingKeyframeRequest {
                consumer_mid,
                consumer_rid: None,
                kind: KeyframeRequestKind::Pli,
            },
        ));

        flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

        assert!(state.dirty_sessions.contains(&source_session));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_forwarded, 1);
        assert_eq!(snapshot.rtc_route_control_absorbed, 0);
    }

    #[test]
    fn flush_pending_keyframe_requests_forwards_remote_sources_by_transport_media_id() {
        let source_session = TransportSessionKey::new(71, 0, 72, SessionId::Integer(73));
        let consumer_session = TransportSessionKey::new(71, 1, 74, SessionId::Integer(75));
        let consumer_mid = Mid::from("cam-down");
        let source_transport_media_id = TransportMediaId::new(91);
        let mut state = RtcBootstrapState::default();
        let mut buffers = PacketLoopBuffers::new();
        let metrics = RuntimeMetrics::default();
        let (control_tx, mut control_rx) = mpsc::channel(1);

        assert!(
            state
                .register_remote_source(
                    source_transport_media_id,
                    &source_session,
                    RemoteSourceControl::new(control_tx, RelayTargetId::new(1)),
                )
                .is_ok()
        );
        let _consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer_session.clone(),
                mid: consumer_mid,
                source_transport_media_id,
            });
        buffers.pending_keyframe_requests.push((
            consumer_session,
            PendingKeyframeRequest {
                consumer_mid,
                consumer_rid: None,
                kind: KeyframeRequestKind::Fir,
            },
        ));

        flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

        let command = control_rx.try_recv().ok();
        assert!(matches!(
            command,
            Some(RtcWorkerCommand::RequestRemoteKeyframe {
                source_session_key,
                source_transport_media_id: forwarded_transport_media_id,
                target_id,
                rid: None,
                kind: KeyframeRequestKind::Fir,
            }) if source_session_key == source_session
                && target_id == RelayTargetId::new(1)
                && forwarded_transport_media_id == source_transport_media_id
        ));
        assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
    }

    #[test]
    fn flush_pending_keyframe_requests_coalesces_duplicate_remote_requests() {
        let source_session = TransportSessionKey::new(81, 0, 82, SessionId::Integer(83));
        let first_consumer_session = TransportSessionKey::new(81, 1, 84, SessionId::Integer(85));
        let second_consumer_session = TransportSessionKey::new(81, 1, 86, SessionId::Integer(87));
        let consumer_mid = Mid::from("cam-down");
        let source_transport_media_id = TransportMediaId::new(101);
        let mut state = RtcBootstrapState::default();
        let mut buffers = PacketLoopBuffers::new();
        let metrics = RuntimeMetrics::default();
        let (control_tx, mut control_rx) = mpsc::channel(2);

        assert!(
            state
                .register_remote_source(
                    source_transport_media_id,
                    &source_session,
                    RemoteSourceControl::new(control_tx, RelayTargetId::new(4)),
                )
                .is_ok()
        );
        let _first_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: first_consumer_session.clone(),
                mid: consumer_mid,
                source_transport_media_id,
            });
        let _second_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: second_consumer_session.clone(),
                mid: Mid::from("cam-down-2"),
                source_transport_media_id,
            });
        buffers.pending_keyframe_requests.push((
            first_consumer_session,
            PendingKeyframeRequest {
                consumer_mid,
                consumer_rid: None,
                kind: KeyframeRequestKind::Pli,
            },
        ));
        buffers.pending_keyframe_requests.push((
            second_consumer_session,
            PendingKeyframeRequest {
                consumer_mid: Mid::from("cam-down-2"),
                consumer_rid: None,
                kind: KeyframeRequestKind::Fir,
            },
        ));

        flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

        let command = control_rx.try_recv().ok();
        assert!(matches!(
            command,
            Some(RtcWorkerCommand::RequestRemoteKeyframe {
                source_session_key,
                source_transport_media_id: forwarded_transport_media_id,
                target_id,
                rid: None,
                kind: KeyframeRequestKind::Fir,
            }) if source_session_key == source_session
                && target_id == RelayTargetId::new(4)
                && forwarded_transport_media_id == source_transport_media_id
        ));
        assert!(control_rx.try_recv().is_err());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_forwarded, 1);
        assert_eq!(snapshot.rtc_route_control_absorbed, 0);
    }

    #[test]
    fn flush_pending_keyframe_requests_absorbs_duplicate_local_requests_within_one_flush() {
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_060));
        let source_session = TransportSessionKey::new(91, 0, 92, SessionId::Integer(93));
        let first_consumer_session = TransportSessionKey::new(91, 0, 94, SessionId::Integer(95));
        let second_consumer_session = TransportSessionKey::new(91, 0, 96, SessionId::Integer(97));
        let source_mid = Mid::from("cam-up");
        let mut state = RtcBootstrapState::default();
        let mut buffers = PacketLoopBuffers::new();
        let metrics = RuntimeMetrics::default();

        assert!(
            bootstrap::ensure_session_rtc_state(
                &mut state.sessions,
                &source_session,
                candidate_addr,
                MediaCodecFlags::default(),
            )
            .is_ok()
        );
        let Some(source_session_state) = state.sessions.get_mut(&source_session) else {
            return;
        };
        let mut direct_api = source_session_state.rtc.direct_api();
        direct_api.declare_media(source_mid, MediaKind::Video);
        direct_api.expect_stream_rx(Ssrc::from(55_555_u32), None, source_mid, None);

        let source_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Producer {
                session_key: source_session.clone(),
                mid: source_mid,
            });
        let _first_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: first_consumer_session.clone(),
                mid: Mid::from("cam-down-1"),
                source_transport_media_id,
            });
        let _second_consumer_transport_media_id =
            state.register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: second_consumer_session.clone(),
                mid: Mid::from("cam-down-2"),
                source_transport_media_id,
            });
        buffers.pending_keyframe_requests.push((
            first_consumer_session,
            PendingKeyframeRequest {
                consumer_mid: Mid::from("cam-down-1"),
                consumer_rid: None,
                kind: KeyframeRequestKind::Pli,
            },
        ));
        buffers.pending_keyframe_requests.push((
            second_consumer_session,
            PendingKeyframeRequest {
                consumer_mid: Mid::from("cam-down-2"),
                consumer_rid: None,
                kind: KeyframeRequestKind::Fir,
            },
        ));

        flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_route_control_forwarded, 1);
        assert_eq!(snapshot.rtc_route_control_absorbed, 0);
        assert!(state.dirty_sessions.contains(&source_session));
        assert_eq!(
            state
                .route_control
                .decide_keyframe_request(source_transport_media_id, Instant::now()),
            KeyframeRequestDecision::Absorb
        );
    }
}
