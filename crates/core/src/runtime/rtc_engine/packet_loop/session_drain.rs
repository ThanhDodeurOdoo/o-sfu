//! Draining ready RTC engine sessions into packet-loop buffers.
//!
//! `str0m` is Sans-I/O. It produces transmits, RTP packets, feedback events,
//! transport events and timeout deadlines only when the host polls it. This
//! module contain that polling for sessions that are dirty or whose timeout has
//! elapsed.
//!
//! The scheduler lives in `PacketLoopState`. This file only consumes the
//! ready set returned by that scheduler and writes newly produced work into
//! `PacketLoopBuffers`. It avoids scanning all sessions on every
//! turn.

use std::{
    io::ErrorKind,
    mem::take,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::{Event, Input, Output};
use tokio::net::UdpSocket;
use tracing::{trace, warn};

use super::{
    super::{
        slots::SessionHandle,
        state::{PacketLoopState, RtcSessionState, RtcSnapshotState},
    },
    buffers::PacketLoopBuffers,
    event_observation::{log_rtc_event, observe_rtc_event},
    keyframe_requests::PendingKeyframeRequest,
};
use crate::runtime::{
    diagnostics::DiagnosticsStore,
    media_transport::{SourcePolicySignal, TransportSessionKey},
    metrics::RuntimeMetrics,
};

pub(super) struct SessionDrainContext<'a> {
    pub(super) snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub(super) diagnostics: &'a Arc<DiagnosticsStore>,
    pub(super) metrics: &'a RuntimeMetrics,
    pub(super) source_policy_signal: &'a SourcePolicySignal,
    pub(super) socket: &'a UdpSocket,
}

/// Poll every session that the scheduler reports as ready.
///
/// Readiness comes from dirty-session marks and exact timeout deadlines stored
/// in `PacketLoopState`. Sessions that disappeared before the drain are
/// ignored, which keeps teardown races harmless for already queued wakeups.
pub(super) fn drain_ready_sessions(
    state: &mut PacketLoopState,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) {
    state.collect_ready_sessions(now, &mut buffers.ready_sessions);
    let mut ready_sessions = take(&mut buffers.ready_sessions);
    for session_handle in ready_sessions.drain(..) {
        let session_timeout = {
            let Some((session_key, session_state)) =
                state.users.get_key_value_mut_by_handle(session_handle)
            else {
                continue;
            };
            drain_single_session(session_handle, session_key, session_state, context, buffers)
        };
        state.update_session_timeout_by_handle(session_handle, session_timeout);
    }
    buffers.ready_sessions = ready_sessions;
}

/// Drain all ready outputs from one session's `Rtc` instance.
///
/// # Output contract
///
/// `Output::Transmit` is staged for UDP send, RTP packets are staged for
/// forwarding, keyframe requests are staged for source resolution and selected
/// transport events update observability side channels. Immediate timeouts are
/// fed back into `str0m` in the same drain so the session reaches a stable next
/// deadline before control returns to the driver.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_handle: SessionHandle,
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
) -> Option<Instant> {
    loop {
        match session_state.rtc.poll_output() {
            Ok(Output::Transmit(transmit)) => {
                try_send_or_stage_transmit(
                    context.socket,
                    buffers,
                    transmit.destination,
                    &transmit.contents,
                );
            }
            Ok(Output::Event(Event::RtpPacket(packet))) => {
                buffers.pending_packets.push(
                    super::super::forwarded_packet::ForwardedPacket::from_rtp_packet(
                        session_handle,
                        packet,
                    ),
                );
            }
            Ok(Output::Event(Event::KeyframeRequest(request))) => {
                buffers
                    .pending_keyframe_requests
                    .push((session_key.clone(), PendingKeyframeRequest::new(request)));
                trace!(
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id(),
                    mid = %request.mid,
                    rid = ?request.rid,
                    kind = ?request.kind,
                    "queued route-level keyframe request from rtc packet-loop event"
                );
            }
            Ok(Output::Event(event)) => {
                observe_rtc_event(
                    context.snapshot_state,
                    context.diagnostics,
                    context.metrics,
                    context.source_policy_signal,
                    session_key,
                    &event,
                );
                log_rtc_event(session_key, &event);
            }
            Ok(Output::Timeout(timeout_at)) => {
                let now = Instant::now();
                if timeout_at <= now {
                    if session_state.rtc.handle_input(Input::Timeout(now)).is_err() {
                        warn!(
                            user_id = ?session_key.user_id(),
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
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id(),
                    ?error,
                    "rtc packet loop failed while polling output"
                );
                return None;
            }
        }
    }
}

fn try_send_or_stage_transmit(
    socket: &UdpSocket,
    buffers: &mut PacketLoopBuffers,
    destination: SocketAddr,
    contents: &[u8],
) {
    match socket.try_send_to(contents, destination) {
        Ok(_sent) => {}
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            buffers.push_pending_transmit(destination, contents);
        }
        Err(_error) => {
            warn!(
                destination = %destination,
                "failed to send packet-loop transport datagram"
            );
        }
    }
}
