use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

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

#[derive(Debug)]
struct Snapshot {
    socket: Arc<UdpSocket>,
    candidate_addr: SocketAddr,
    pending_transmits: Vec<PendingTransmit>,
    next_timeout: Option<Instant>,
}

pub(super) async fn run_packet_loop(bootstrap_state: Arc<Mutex<RtcBootstrapState>>) {
    let mut receive_buffer = vec![0_u8; RECEIVE_BUFFER_LEN];
    loop {
        let Some(snapshot) = snapshot_and_pump(&bootstrap_state) else {
            sleep(IDLE_SLEEP).await;
            continue;
        };
        for pending_transmit in &snapshot.pending_transmits {
            if snapshot
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
        let wait_duration = socket_wait_duration(snapshot.next_timeout);
        match timeout(
            wait_duration,
            snapshot.socket.recv_from(&mut receive_buffer),
        )
        .await
        {
            Ok(Ok((received_size, source_addr))) => {
                if received_size == 0 {
                    continue;
                }
                let Some(packet) = receive_buffer.get(..received_size) else {
                    continue;
                };
                route_incoming_packet(
                    &bootstrap_state,
                    source_addr,
                    snapshot.candidate_addr,
                    packet,
                );
            }
            Ok(Err(_error)) => {
                warn!("rtc packet loop failed to receive datagram");
            }
            Err(_elapsed) => {}
        }
    }
}

fn snapshot_and_pump(bootstrap_state: &Arc<Mutex<RtcBootstrapState>>) -> Option<Snapshot> {
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
    let (pending_transmits, next_timeout) = pump_session_outputs(&mut state.sessions);
    Some(Snapshot {
        socket,
        candidate_addr,
        pending_transmits,
        next_timeout,
    })
}

fn pump_session_outputs(
    sessions: &mut BTreeMap<SessionId, RtcSessionState>,
) -> (Vec<PendingTransmit>, Option<Instant>) {
    let mut pending_transmits = Vec::new();
    let mut next_timeout: Option<Instant> = None;
    for (session_id, session_state) in sessions {
        let session_timeout =
            drain_single_session(session_id, session_state, &mut pending_transmits);
        if let Some(t) = session_timeout {
            next_timeout = Some(next_timeout.map_or(t, |current| current.min(t)));
        }
    }
    (pending_transmits, next_timeout)
}

/// Drain all ready outputs from a single session's `Rtc` instance.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_id: &SessionId,
    session_state: &mut RtcSessionState,
    pending_transmits: &mut Vec<PendingTransmit>,
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
