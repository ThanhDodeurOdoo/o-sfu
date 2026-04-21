use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::{Event, Input, Output};
use tracing::{trace, warn};

use super::super::state::{RtcBootstrapState, RtcSessionState, RtcSnapshotState};
use super::{
    buffers::PacketLoopBuffers,
    event_observation::{log_rtc_event, observe_rtc_event},
    keyframe_requests::PendingKeyframeRequest,
};
use crate::runtime::diagnostics::DiagnosticsStore;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::transport_adapter::TransportSessionKey;

pub(super) fn drain_ready_sessions(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    diagnostics: &Arc<DiagnosticsStore>,
    metrics: &RuntimeMetrics,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) {
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
                diagnostics,
                metrics,
                buffers,
            )
        };
        state.update_session_timeout(&session_id, session_timeout);
    }
}

/// Drain all ready outputs from a single session's `Rtc` instance.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    diagnostics: &Arc<DiagnosticsStore>,
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
                    session_id = ?session_key.session_id(),
                    media_worker_id = session_key.media_worker_id(),
                    mid = %request.mid,
                    rid = ?request.rid,
                    kind = ?request.kind,
                    "queued route-level keyframe request from rtc packet-loop event"
                );
            }
            Ok(Output::Event(event)) => {
                observe_rtc_event(snapshot_state, diagnostics, metrics, session_key, &event);
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
