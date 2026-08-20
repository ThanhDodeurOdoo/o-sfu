//! str0m's Sans-I/O drain boundary for ready RTC sessions.
//!
//! A successful ready-session drain polls [`str0m::Rtc`] until
//! [`Output::Timeout`]. That timeout proves queued output is exhausted and
//! supplies the next host-driven deadline.
//!
//! [`PacketLoopState`] merges dirty marks with due deadlines so the worker does
//! not scan every session. Work that needs socket I/O or worker-wide route state
//! stays in [`PacketLoopBuffers`] until the mutable session borrow ends.

use std::{
    mem::take,
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::{Event, Input, Output};
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
use crate::engine::{
    media_transport::{SourcePolicySignal, TransportSessionKey},
    metrics::RuntimeMetrics,
};

/// Non-authoritative observation and policy-wakeup side channels kept outside
/// [`PacketLoopState`].
pub struct SessionDrainContext<'a> {
    pub snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub metrics: &'a RuntimeMetrics,
    pub source_policy_signal: &'a SourcePolicySignal,
}

/// Drains each session selected by a dirty mark or due deadline once.
pub fn drain_ready_sessions(
    state: &mut PacketLoopState,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) {
    // Resolve every due deadline against one turn clock. Resampling per session
    // would make iteration order and host speed change this turn's work.
    state.collect_ready_sessions(now, &mut buffers.ready_sessions);
    let mut ready_sessions = take(&mut buffers.ready_sessions);
    for session_handle in ready_sessions.drain(..) {
        let session_timeout = {
            // The handle carries the slot generation, so a stale ready entry
            // cannot poll a later occupant.
            let Some((session_key, session_state)) =
                state.users.get_key_value_mut_by_handle(session_handle)
            else {
                continue;
            };
            // Keep a successful drain indivisible and on the turn's fixed `now`.
            // Returning before a future `Output::Timeout` would let the next
            // packet-loop mutation overtake queued str0m output.
            drain_single_session(
                session_handle,
                session_key,
                session_state,
                context,
                buffers,
                now,
            )
        };
        state.update_session_timeout_by_handle(session_handle, session_timeout);
    }
    buffers.ready_sessions = ready_sessions;
}

/// Drains one [`str0m::Rtc`] and stages its output.
///
/// Returns the next future deadline or `None` after a poll or timeout-input
/// error.
fn drain_single_session(
    session_handle: SessionHandle,
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) -> Option<Instant> {
    loop {
        match session_state.rtc.poll_output() {
            Ok(Output::Transmit(transmit)) => {
                buffers.push_pending_transmit(
                    transmit.destination,
                    Vec::<u8>::from(transmit.contents),
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
                // Consumer MID/RID names the receiving leg. Preserve `session_key`
                // so route state can resolve its current producer after this borrow.
                buffers
                    .pending_keyframe_requests
                    .push((session_key.clone(), PendingKeyframeRequest::new(request)));
                trace!(
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    mid = %request.mid,
                    rid = ?request.rid,
                    kind = ?request.kind,
                    "queued route-level keyframe request from rtc packet-loop event"
                );
            }
            Ok(Output::Event(event)) => {
                observe_rtc_event(
                    context.snapshot_state,
                    context.metrics,
                    context.source_policy_signal,
                    session_state.room_id.as_ref(),
                    session_key,
                    &event,
                );
                log_rtc_event(session_key, &event);
            }
            Ok(Output::Timeout(timeout_at)) => {
                // `Output::Timeout` marks current output exhausted. Elapsed host
                // time alone does not advance str0m's clock, so feed an already-due
                // deadline back and drain again.
                if timeout_at <= now {
                    if session_state.rtc.handle_input(Input::Timeout(now)).is_err() {
                        warn!(
                            user_id = ?session_key.user_id(),
                            media_worker_id = session_key.media_worker_id().as_usize(),
                            "failed to apply immediate rtc packet-loop timeout input"
                        );
                        return None;
                    }
                    continue;
                }
                return Some(timeout_at);
            }
            Err(error) => {
                // A failed poll yields no replacement deadline. Returning `None`
                // removes the schedule rather than retrying an unclassified `RtcError`.
                warn!(
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    ?error,
                    "rtc packet loop failed while polling output"
                );
                return None;
            }
        }
    }
}
