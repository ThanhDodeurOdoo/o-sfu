//! Draining ready RTC adapter sessions into packet-loop buffers.
//!
//! # Boundary role
//!
//! `str0m` is Sans-I/O. It produces transmits, RTP packets, feedback events,
//! transport events and timeout deadlines only when the host polls it. This
//! module owns that polling for sessions that are dirty or whose timeout has
//! elapsed.
//!
//! The scheduler lives in `RtcBootstrapState`. This file only consumes the
//! ready set returned by that scheduler and writes newly produced work into
//! `PacketLoopBuffers`. It deliberately avoids scanning all sessions on every
//! turn.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::{Event, Input, Output};
use tracing::{trace, warn};

use super::{
    super::state::{RtcBootstrapState, RtcSessionState, RtcSnapshotState},
    buffers::PacketLoopBuffers,
    event_observation::{log_rtc_event, observe_rtc_event},
    keyframe_requests::PendingKeyframeRequest,
};
use crate::runtime::{
    diagnostics::DiagnosticsStore,
    metrics::RuntimeMetrics,
    transport_adapter::{SourcePolicySignal, TransportSessionKey},
};

/// Poll every session that the scheduler reports as ready.
///
/// Readiness comes from dirty-session marks and exact timeout deadlines stored
/// in `RtcBootstrapState`. Sessions that disappeared before the drain are
/// ignored, which keeps teardown races harmless for already queued wakeups.
pub(super) fn drain_ready_sessions(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    diagnostics: &Arc<DiagnosticsStore>,
    metrics: &RuntimeMetrics,
    source_policy_signal: &SourcePolicySignal,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) {
    let ready_sessions = state.take_ready_sessions(now);
    for user_id in ready_sessions {
        let session_timeout = {
            let Some(session_state) = state.users.get_mut(&user_id) else {
                continue;
            };
            drain_single_session(
                &user_id,
                session_state,
                snapshot_state,
                diagnostics,
                metrics,
                source_policy_signal,
                buffers,
            )
        };
        state.update_session_timeout(&user_id, session_timeout);
    }
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
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    diagnostics: &Arc<DiagnosticsStore>,
    metrics: &RuntimeMetrics,
    source_policy_signal: &SourcePolicySignal,
    buffers: &mut PacketLoopBuffers,
) -> Option<Instant> {
    loop {
        let now = Instant::now();
        match session_state.rtc.poll_output() {
            Ok(Output::Transmit(transmit)) => {
                buffers.push_pending_transmit(transmit.destination, &transmit.contents);
            }
            Ok(Output::Event(Event::RtpPacket(packet))) => {
                buffers.pending_packets.push(
                    super::super::forwarded_packet::ForwardedPacket::from_rtp_packet(
                        session_key.clone(),
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
                    snapshot_state,
                    diagnostics,
                    metrics,
                    source_policy_signal,
                    session_key,
                    &event,
                );
                log_rtc_event(session_key, &event);
            }
            Ok(Output::Timeout(timeout_at)) => {
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
