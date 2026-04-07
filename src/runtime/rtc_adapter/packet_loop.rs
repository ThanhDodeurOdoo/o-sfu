use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use str0m::media::{MediaData, Mid};
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output};
use tokio::{
    net::UdpSocket,
    time::{sleep, timeout},
};
use tracing::{debug, trace, warn};

use super::{RtcBootstrapState, RtcSessionState};
use crate::signaling::shared::SessionId;

const IDLE_SLEEP: Duration = Duration::from_millis(50);
const MAX_SOCKET_WAIT: Duration = Duration::from_millis(100);
const RECEIVE_BUFFER_LEN: usize = 2000;

#[derive(Debug)]
struct PendingTransmit {
    destination: SocketAddr,
    contents: Vec<u8>,
}

/// Reusable buffers for the packet loop, allocated once and cleared per iteration
/// to avoids steady-state heap allocations
struct PacketLoopBuffers {
    pending_transmits: Vec<PendingTransmit>,
    pending_media: Vec<(SessionId, MediaData)>,
    forwards: Vec<(usize, SessionId, Mid)>,
}

impl PacketLoopBuffers {
    fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_media: Vec::with_capacity(32),
            forwards: Vec::with_capacity(64),
        }
    }

    fn clear(&mut self) {
        self.pending_transmits.clear();
        self.pending_media.clear();
        self.forwards.clear();
    }
}

struct SnapshotInfo {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    next_timeout: Option<Instant>,
}

pub(super) async fn run_packet_loop(bootstrap_state: Arc<Mutex<RtcBootstrapState>>) {
    let mut receive_buffer = vec![0_u8; RECEIVE_BUFFER_LEN];
    let mut buffers = PacketLoopBuffers::new();
    loop {
        let Some(info) = snapshot_and_pump(&bootstrap_state, &mut buffers) else {
            sleep(IDLE_SLEEP).await;
            continue;
        };
        for pending_transmit in &buffers.pending_transmits {
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
        let wait_duration = socket_wait_duration(info.next_timeout);
        match timeout(wait_duration, info.socket.recv_from(&mut receive_buffer)).await {
            Ok(Ok((received_size, source_addr))) => {
                if received_size == 0 {
                    continue;
                }
                let Some(packet) = receive_buffer.get(..received_size) else {
                    continue;
                };
                route_incoming_packet(&bootstrap_state, source_addr, info.candidate_addr, packet);
            }
            Ok(Err(_error)) => {
                warn!("rtc packet loop failed to receive datagram");
            }
            Err(_elapsed) => {}
        }
    }
}

fn snapshot_and_pump(
    bootstrap_state: &Arc<Mutex<RtcBootstrapState>>,
    buffers: &mut PacketLoopBuffers,
) -> Option<SnapshotInfo> {
    buffers.clear();
    let Ok(mut state) = bootstrap_state.lock() else {
        return None;
    };
    let (socket, candidate_addr) = {
        let shared_socket = state.shared_socket.as_ref()?;
        (
            Arc::clone(&shared_socket.socket),
            shared_socket.candidate_addr,
        )
    };
    let mut next_timeout: Option<Instant> = None;
    for (session_id, session_state) in &mut state.sessions {
        let session_timeout = drain_single_session(
            session_id,
            session_state,
            &mut buffers.pending_transmits,
            &mut buffers.pending_media,
        );
        if let Some(t) = session_timeout {
            next_timeout = Some(next_timeout.map_or(t, |current| current.min(t)));
        }
    }

    // Two-pass: collect matching routes, then write to destination sessions.
    // Separating the read of the route index from the mutable session access
    // avoids a borrow conflict on `state`.
    for (media_idx, (source_session, media)) in buffers.pending_media.iter().enumerate() {
        if let Some(destinations) = state
            .media_route_index
            .get(&(source_session.clone(), media.mid))
        {
            for dest in destinations {
                buffers
                    .forwards
                    .push((media_idx, dest.dest_session.clone(), dest.dest_mid));
            }
        }
    }
    for &(media_idx, ref dest_session, dest_mid) in &buffers.forwards {
        let Some((_source_session, media)) = buffers.pending_media.get(media_idx) else {
            continue;
        };
        let Some(dest_session_state) = state.sessions.get_mut(dest_session) else {
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
        if let Err(error) =
            data_writer.write(pt, media.network_time, media.time, media.data.clone())
        {
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
        next_timeout,
    })
}

/// Drain all ready outputs from a single session's `Rtc` instance.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_id: &SessionId,
    session_state: &mut RtcSessionState,
    pending_transmits: &mut Vec<PendingTransmit>,
    pending_media: &mut Vec<(SessionId, MediaData)>,
) -> Option<Instant> {
    loop {
        let now = Instant::now();
        match session_state.rtc.poll_output() {
            Ok(Output::Transmit(transmit)) => {
                pending_transmits.push(PendingTransmit {
                    destination: transmit.destination,
                    contents: Vec::from(transmit.contents),
                });
            }
            Ok(Output::Event(Event::MediaData(data))) => {
                pending_media.push((session_id.clone(), data));
            }
            Ok(Output::Event(event)) => {
                log_rtc_event(session_id, &event);
            }
            Ok(Output::Timeout(timeout_at)) => {
                if timeout_at <= now {
                    if session_state.rtc.handle_input(Input::Timeout(now)).is_err() {
                        warn!(
                            session_id = ?session_id,
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
                    session_id = ?session_id,
                    ?error,
                    "rtc packet loop failed while polling output"
                );
                return None;
            }
        }
    }
}

fn log_rtc_event(session_id: &SessionId, event: &Event) {
    match event {
        Event::IceConnectionStateChange(state) => {
            debug!(
                session_id = ?session_id,
                ?state,
                "rtc ICE connection state transition"
            );
        }
        Event::Connected => {
            debug!(
                session_id = ?session_id,
                "rtc DTLS transport reached connected state"
            );
        }
        _ => {
            trace!(session_id = ?session_id, ?event, "rtc packet loop event");
        }
    }
}

fn socket_wait_duration(next_timeout: Option<Instant>) -> Duration {
    let timeout_duration = next_timeout.map_or(MAX_SOCKET_WAIT, |timeout_at| {
        timeout_at
            .saturating_duration_since(Instant::now())
            .min(MAX_SOCKET_WAIT)
    });
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}

fn route_incoming_packet(
    bootstrap_state: &Arc<Mutex<RtcBootstrapState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    let Ok(mut state) = bootstrap_state.lock() else {
        return;
    };
    route_packet_to_matching_session(&mut state.sessions, source_addr, candidate_addr, packet);
}

fn route_packet_to_matching_session(
    sessions: &mut BTreeMap<SessionId, RtcSessionState>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    let Ok(receive) = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet) else {
        trace!(
            source = %source_addr,
            "ignoring malformed UDP datagram in rtc packet loop"
        );
        return;
    };
    let now = Instant::now();
    let input = Input::Receive(now, receive);
    for (session_id, session_state) in sessions {
        if session_state.rtc.accepts(&input) {
            if session_state.rtc.handle_input(input).is_err() {
                warn!(
                    session_id = ?session_id,
                    "failed to feed incoming UDP datagram into rtc session state"
                );
            }
            return;
        }
    }
    trace!(
        source = %source_addr,
        "dropping UDP datagram because no rtc session accepted it"
    );
}
