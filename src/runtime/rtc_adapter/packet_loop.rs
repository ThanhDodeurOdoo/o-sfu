use std::{
    collections::BTreeMap,
    io::Error as IoError,
    net::{IpAddr, SocketAddr},
    slice::Iter,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use str0m::IceConnectionState;
use str0m::ice::StunMessage;
use str0m::media::{KeyframeRequest, KeyframeRequestKind, Mid, Rid};
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output};
use tokio::{net::UdpSocket, sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use super::{
    commands::RtcWorkerCommand,
    forwarded_packet::ForwardedPacket,
    forwarding_destination::{ForwardingDestination, PacketForward},
    forwarding_planner::populate_forward_routes,
    relay_registry::RelayRegistry,
    route_control::{KeyframeRequestDecision, coalesce_keyframe_kind},
    routing_miss::{PacketLoopRoutingMissKey, PacketLoopRoutingState},
    state::{RtcBootstrapState, RtcSessionState, RtcSnapshotState, TransportSessionHealth},
    worker::{WorkerCommandContext, handle_worker_command, request_keyframe_for_source},
};
use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::metrics::{
    RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome, RtpForwardDestinationKind,
    RuntimeMetrics, TransportIceState,
};
use crate::runtime::recording::MediaTap;
use crate::runtime::rtc_adapter::media_registry::RegisteredMediaHandle;
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

const RECEIVE_BUFFER_LEN: usize = 2000;
const MAX_RELAY_PACKETS_PER_ITERATION: usize = 64;

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

enum IndexedSessionRecoveryOutcome {
    Matched {
        session_key: TransportSessionKey,
        examined_sessions: usize,
    },
    NoMatch {
        examined_sessions: usize,
    },
    Malformed,
}

enum PacketIndexProbe {
    LocalIceUfrag(String),
    RemoteCandidateAddr(SocketAddr),
}

impl PacketIndexProbe {
    fn describe(&self) -> String {
        match self {
            Self::LocalIceUfrag(local_ice_ufrag) => {
                format!("local-ice-ufrag:{local_ice_ufrag}")
            }
            Self::RemoteCandidateAddr(remote_candidate_addr) => {
                format!("remote-candidate-addr:{remote_candidate_addr}")
            }
        }
    }
}

enum CandidateSessionKeys<'a> {
    Single(Option<&'a TransportSessionKey>),
    Slice(Iter<'a, TransportSessionKey>),
}

impl<'a> Iterator for CandidateSessionKeys<'a> {
    type Item = &'a TransportSessionKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(session_key) => session_key.take(),
            Self::Slice(iter) => iter.next(),
        }
    }
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
    for packet in &buffers.pending_packets {
        if let Some(transport_media_id) = packet.resolve_source_transport_media_id(state) {
            let payload_len = packet.payload_len();
            state.route_control.observe_audio_activity(
                transport_media_id,
                packet.route_control_voice_activity(),
                packet.route_control_audio_level(),
                packet.received_at(),
            );
            let first_ingress = snapshot_state.lock().is_ok_and(|mut snapshot| {
                snapshot.record_incoming_media(
                    packet.source_session_key(),
                    transport_media_id,
                    packet.received_at(),
                    payload_len,
                )
            });
            if first_ingress {
                debug!(
                    session_id = ?packet.source_session_key().session_id(),
                    media_worker_id = packet.source_session_key().media_worker_id(),
                    ?transport_media_id,
                    payload_bytes = payload_len,
                    "observed first RTP ingress for published media"
                );
            }
            metrics.record_rtp_ingress(payload_len);
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

    drain_relay_packets(
        relay_rx,
        &mut buffers.pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );
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
    max_packets: usize,
) -> usize {
    let mut drained_packets = 0;
    while drained_packets < max_packets {
        match relay_rx.try_recv() {
            Ok(packet) => {
                pending_packets.push(packet);
                drained_packets += 1;
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
    drained_packets
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
    if let Some(RegisteredMediaHandle::Producer { session_key, .. }) =
        state.mid_registry.get(&source_transport_media_id.as_u64())
    {
        return Some(ResolvedKeyframeRoute::Local {
            source_session_key: session_key.clone(),
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
    let mut relay_packets = Vec::with_capacity(pending_packets.len());
    relay_packets.resize_with(pending_packets.len(), || None);
    for (forward_idx, forward) in forwards.iter().enumerate() {
        let is_last_destination = forwards
            .get(forward_idx + 1)
            .is_none_or(|next_forward| next_forward.packet_idx() != forward.packet_idx());
        let packet_idx = forward.packet_idx();
        let Some(packet) = pending_packets.get_mut(packet_idx) else {
            continue;
        };
        let destination = forward.destination();
        let destination_kind = match destination {
            ForwardingDestination::LocalRtc(_) => RtpForwardDestinationKind::LocalRtc,
            ForwardingDestination::Recording(_) => RtpForwardDestinationKind::Recording,
            ForwardingDestination::IntraNodeRelay(_) => RtpForwardDestinationKind::IntraNodeRelay,
            ForwardingDestination::InterNodeRelay(_) => RtpForwardDestinationKind::InterNodeRelay,
        };
        let payload_len = packet.payload_len();
        let relay_packet = match destination {
            ForwardingDestination::IntraNodeRelay(_) | ForwardingDestination::InterNodeRelay(_) => {
                let Some(source_transport_media_id) =
                    packet.resolve_source_transport_media_id(state)
                else {
                    continue;
                };
                let Some(shared_packet) = relay_packets.get_mut(packet_idx) else {
                    continue;
                };
                Some(
                    shared_packet
                        .get_or_insert_with(|| packet.share_for_relay(source_transport_media_id)),
                )
            }
            ForwardingDestination::LocalRtc(_) | ForwardingDestination::Recording(_) => None,
        };
        let packet = relay_packet.unwrap_or(packet);
        match destination.send(state, packet, is_last_destination) {
            Ok(Some(payload_len)) => {
                metrics.record_rtp_egress(payload_len);
                metrics.record_rtp_forwarded(destination_kind, payload_len);
            }
            Ok(None)
                if matches!(
                    destination,
                    ForwardingDestination::Recording(_)
                        | ForwardingDestination::IntraNodeRelay(_)
                        | ForwardingDestination::InterNodeRelay(_)
                ) =>
            {
                metrics.record_rtp_forwarded(destination_kind, payload_len);
            }
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
            routing_state.record_route_success(miss_key, packet, source_addr);
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
    let now = Instant::now();
    if routing_state.should_rate_limit_source(source_addr, now) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
        trace!(
            source = %source_addr,
            "dropping UDP datagram because sustained unknown-source misses exhausted the rtc recovery budget for this source"
        );
        return;
    }
    let route = PacketRouteContext {
        snapshot_state,
        metrics,
        source_addr,
        candidate_addr,
        packet,
        now,
    };
    if state.sessions.len() == 1 {
        route_packet_by_single_session(state, routing_state, miss_key, &route);
        return;
    }
    route_packet_by_recovery_index(state, routing_state, miss_key, &route);
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
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "indexed rtc source address no longer matched the cached session; clearing source-address pin"
        );
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

fn matching_indexed_session_key_for_packet(
    state: &mut RtcBootstrapState,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    now: Instant,
) -> IndexedSessionRecoveryOutcome {
    let packet_index_probe = match packet_index_probe(source_addr, packet) {
        Ok(packet_index_probe) => packet_index_probe,
        Err(
            IndexedSessionRecoveryOutcome::Malformed
            | IndexedSessionRecoveryOutcome::Matched { .. }
            | IndexedSessionRecoveryOutcome::NoMatch { .. },
        ) => {
            return IndexedSessionRecoveryOutcome::Malformed;
        }
    };
    let packet_index_probe_description = packet_index_probe.describe();
    let candidate_session_keys = match packet_index_probe {
        PacketIndexProbe::LocalIceUfrag(local_ice_ufrag) => CandidateSessionKeys::Single(
            state
                .remote_addr_demux
                .session_key_for_local_ice_ufrag(&local_ice_ufrag),
        ),
        PacketIndexProbe::RemoteCandidateAddr(remote_candidate_addr) => state
            .remote_addr_demux
            .candidate_sessions_for_source_addr(remote_candidate_addr)
            .map_or(
                CandidateSessionKeys::Single(None),
                |candidate_session_keys| CandidateSessionKeys::Slice(candidate_session_keys.iter()),
            ),
    };
    let Some(input) = receive_input(now, source_addr, candidate_addr, packet) else {
        return IndexedSessionRecoveryOutcome::Malformed;
    };
    let mut examined_sessions: usize = 0;
    let mut stale_session_keys = Vec::new();
    let matched_session_key = {
        let mut matched_session_key = None;
        for session_key in candidate_session_keys {
            let Some(session_state) = state.sessions.get(session_key) else {
                stale_session_keys.push(session_key.clone());
                continue;
            };
            examined_sessions = examined_sessions.saturating_add(1);
            if session_state.rtc.accepts(&input) {
                matched_session_key = Some(session_key.clone());
                break;
            }
        }
        matched_session_key
    };
    if let Some(matched_session_key) = matched_session_key {
        for stale_session_key in stale_session_keys {
            state
                .remote_addr_demux
                .forget_session_remote_candidate_addrs(&stale_session_key);
            state
                .remote_addr_demux
                .forget_session_local_ice_ufrag(&stale_session_key);
        }
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            probe = %packet_index_probe_description,
            session_id = ?matched_session_key.session_id(),
            media_worker_id = matched_session_key.media_worker_id(),
            examined_sessions,
            "recovered rtc session routing from packet probe"
        );
        return IndexedSessionRecoveryOutcome::Matched {
            session_key: matched_session_key,
            examined_sessions,
        };
    }
    for stale_session_key in stale_session_keys {
        state
            .remote_addr_demux
            .forget_session_remote_candidate_addrs(&stale_session_key);
        state
            .remote_addr_demux
            .forget_session_local_ice_ufrag(&stale_session_key);
    }
    debug!(
        source_addr = %source_addr,
        candidate_addr = %candidate_addr,
        probe = %packet_index_probe_description,
        examined_sessions,
        "packet probe did not match any rtc session"
    );
    IndexedSessionRecoveryOutcome::NoMatch { examined_sessions }
}

fn packet_index_probe(
    source_addr: SocketAddr,
    packet: &[u8],
) -> Result<PacketIndexProbe, IndexedSessionRecoveryOutcome> {
    let Some(byte0) = packet.first().copied() else {
        return Err(IndexedSessionRecoveryOutcome::Malformed);
    };
    let packet_len = packet.len();
    if byte0 < 2 && packet_len >= 20 {
        let message = StunMessage::parse(packet)
            .map_err(|_error| IndexedSessionRecoveryOutcome::Malformed)?;
        if let Some((local_ice_ufrag, _remote_ice_ufrag)) = message.split_username() {
            return Ok(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag.to_owned()));
        }
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (20..64).contains(&byte0) {
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (128..192).contains(&byte0) && packet_len > 2 {
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    Err(IndexedSessionRecoveryOutcome::Malformed)
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
    route_resolution: &'static str,
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
    let previous_session_key = state
        .remote_addr_demux
        .session_key_for_remote_addr(route.source_addr)
        .cloned();
    if state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key)
        && let Ok(mut snapshot) = route.snapshot_state.lock()
    {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(route.source_addr, session_key);
        match previous_session_key {
            Some(previous_session_key) => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    previous_session_id = ?previous_session_key.session_id(),
                    previous_media_worker_id = previous_session_key.media_worker_id(),
                    session_id = ?session_key.session_id(),
                    media_worker_id = session_key.media_worker_id(),
                    "remapped rtc source address to a different session"
                );
            }
            None => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    session_id = ?session_key.session_id(),
                    media_worker_id = session_key.media_worker_id(),
                    "pinned rtc source address to session"
                );
            }
        }
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
    routing_state.record_fallback_attempt();
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
        routing_state.record_miss(miss_key, route.packet, route.source_addr, route.now);
        trace!(
            source = %route.source_addr,
            "dropping UDP datagram because no rtc session accepted it"
        );
        return;
    }
    if route_packet_to_session(state, &session_key, route, "single-session-scan") {
        routing_state.record_route_success(miss_key, route.packet, route.source_addr);
    }
}

fn route_packet_by_recovery_index(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    routing_state.record_fallback_attempt();
    let session_key = match matching_indexed_session_key_for_packet(
        state,
        route.source_addr,
        route.candidate_addr,
        route.packet,
        route.now,
    ) {
        IndexedSessionRecoveryOutcome::Matched {
            session_key,
            examined_sessions,
        } => {
            route
                .metrics
                .record_rtc_datagram_fallback_scan(examined_sessions);
            session_key
        }
        IndexedSessionRecoveryOutcome::NoMatch { examined_sessions } => {
            route
                .metrics
                .record_rtc_datagram_fallback_scan(examined_sessions);
            route
                .metrics
                .record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
            routing_state.record_miss(miss_key, route.packet, route.source_addr, route.now);
            trace!(
                source = %route.source_addr,
                "dropping UDP datagram because no rtc session accepted it"
            );
            return;
        }
        IndexedSessionRecoveryOutcome::Malformed => {
            route
                .metrics
                .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            log_malformed_datagram(route.source_addr);
            return;
        }
    };
    if route_packet_to_session(state, &session_key, route, "recovery-index") {
        routing_state.record_route_success(miss_key, route.packet, route.source_addr);
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
        relay_registry::{InterNodeRelaySender, RelayPacketMailbox, RelayTargetId},
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

        assert_eq!(routing_state.fallback_attempts(), 1);
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

        assert_eq!(routing_state.fallback_attempts(), 2);
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

        assert_eq!(routing_state.fallback_attempts(), 2);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 2);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 2);
        assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
        assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 0);
    }

    #[test]
    fn source_rate_limiter_bounds_varied_unknown_source_misses() {
        let mut bootstrap_state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let mut routing_state = PacketLoopRoutingState::new();
        let metrics = RuntimeMetrics::default();
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_026));
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_025));

        for (sequence, ssrc) in [
            (5_u16, 55_u32),
            (6, 66),
            (7, 77),
            (8, 88),
            (9, 99),
            (10, 110),
        ] {
            route_packet_to_matching_session(
                &mut bootstrap_state,
                &snapshot_state,
                &mut routing_state,
                &metrics,
                source_addr,
                candidate_addr,
                &valid_rtp_packet(sequence, ssrc),
            );
        }

        assert_eq!(routing_state.fallback_attempts(), 4);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 4);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 4);
        assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
        assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 2);
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
        assert_eq!(routing_state.fallback_attempts(), 1);
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 0);
        assert_eq!(snapshot.rtc_datagram_scan_sessions, 0);
        assert_eq!(snapshot.rtc_datagram_drops_malformed, 1);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 0);
        assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 0);
    }

    #[test]
    fn multi_session_unknown_source_recovery_drops_without_whole_session_scan() {
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_040));
        let mut bootstrap_state = RtcBootstrapState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let mut routing_state = PacketLoopRoutingState::new();
        let metrics = RuntimeMetrics::default();
        let first_session = TransportSessionKey::new(51, 0, 52, SessionId::Integer(53));
        let second_session = TransportSessionKey::new(51, 0, 54, SessionId::Integer(55));
        let packet = [22, 0, 0, 0];
        let unknown_source_addr = SocketAddr::from(([127, 0, 0, 1], 45_041));

        let first_created = bootstrap::ensure_session_rtc_state(
            &mut bootstrap_state.sessions,
            &first_session,
            candidate_addr,
            MediaCodecFlags::default(),
        );
        let second_created = bootstrap::ensure_session_rtc_state(
            &mut bootstrap_state.sessions,
            &second_session,
            candidate_addr,
            MediaCodecFlags::default(),
        );

        assert_eq!(first_created, Ok(true));
        assert_eq!(second_created, Ok(true));

        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            unknown_source_addr,
            candidate_addr,
            &packet,
        );

        let snapshot = metrics.snapshot();
        assert_eq!(routing_state.fallback_attempts(), 1);
        assert_eq!(snapshot.rtc_datagram_fallback_scans, 1);
        assert_eq!(snapshot.rtc_datagram_scan_sessions, 0);
        assert_eq!(snapshot.rtc_datagram_drops_no_session, 1);
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
        assert_eq!(metrics.snapshot().rtp_forwarded_packets_recording, 1);
    }

    #[test]
    fn flush_forward_routes_records_non_local_forwarding_volume_by_destination() {
        let source_session = TransportSessionKey::new(118, 0, 119, SessionId::Integer(120));
        let source_transport_media_id = TransportMediaId::new(121);
        let mut state = RtcBootstrapState::default();
        let sink = Arc::new(CountingSink::new());
        let (relay_mailbox, mut intra_node_rx) = RelayPacketMailbox::channel_for_test();
        let (inter_node_sender, mut inter_node_rx) = InterNodeRelaySender::channel_for_test();
        let mut buffers = PacketLoopBuffers::new();
        let metrics = RuntimeMetrics::default();
        let packet = sample_forwarded_packet(source_session, "aud-up", b"payload")
            .share_for_relay(source_transport_media_id);

        buffers.pending_packets.push(packet);
        buffers.forwards.push(PacketForward::from_recording_sink(
            0,
            source_transport_media_id,
            into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        ));
        buffers
            .forwards
            .push(PacketForward::from_intra_node_relay_sink(
                0,
                source_transport_media_id,
                relay_mailbox,
            ));
        buffers
            .forwards
            .push(PacketForward::from_inter_node_relay_sink(
                0,
                source_transport_media_id,
                inter_node_sender,
            ));

        flush_forward_routes(&mut state, &metrics, &mut buffers);

        assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
        assert!(intra_node_rx.try_recv().is_ok());
        assert!(inter_node_rx.try_recv().is_ok());

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rtp_forwarded_packets_local_rtc, 0);
        assert_eq!(snapshot.rtp_forwarded_packets_recording, 1);
        assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay, 1);
        assert_eq!(snapshot.rtp_forwarded_packets_inter_node_relay, 1);
        assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc, 0);
        assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording, 7);
        assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay, 7);
        assert_eq!(snapshot.rtp_forwarded_payload_bytes_inter_node_relay, 7);
        assert_eq!(snapshot.rtp_payload_bytes_egress, 0);
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
        drain_relay_packets(
            &mut relay_rx,
            &mut pending_packets,
            MAX_RELAY_PACKETS_PER_ITERATION,
        );

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
    fn drain_relay_packets_stops_at_the_configured_cap() {
        let source_session = TransportSessionKey::new(26, 0, 27, SessionId::Integer(28));
        let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
        let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
        let mut pending_packets = Vec::new();

        mailbox.forward_packet(&packet, TransportMediaId::new(18));
        mailbox.forward_packet(&packet, TransportMediaId::new(18));

        let drained = drain_relay_packets(&mut relay_rx, &mut pending_packets, 1);

        assert_eq!(drained, 1);
        assert_eq!(pending_packets.len(), 1);
        assert!(relay_rx.try_recv().is_ok());
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
