//! Host polling and machine ingestion for RTC engine session output.
//!
//! `str0m` is Sans-I/O. It produces transmits, RTP packets, feedback events,
//! transport events and timeout deadlines only when the host polls it. The
//! host drains those outputs before a machine turn starts. The machine turn then
//! consumes the normalized outputs without calling back into `str0m`.
//!
//! The scheduler lives in `RtcBootstrapState`. This file only consumes the
//! ready set returned by that scheduler. It avoids scanning all sessions on
//! every turn.

use std::time::Instant;

use tracing::{trace, warn};

use super::{
    super::{
        session_adapter::HostSessionOutput,
        state::{RtcBootstrapState, RtcSessionState},
    },
    event_observation::observe_rtc_event,
    host_clock::PacketLoopClock,
    keyframe_requests::PendingKeyframeRequest,
    machine::{effect::PacketLoopEffects, scratch::PacketLoopScratch},
    time::PacketLoopTime,
};
use crate::runtime::media_transport::TransportSessionKey;

pub(in crate::runtime::rtc_engine::packet_loop) struct SessionPollContext {
    pub(super) host_now: Instant,
    pub(super) packet_now: PacketLoopTime,
    pub(super) clock: PacketLoopClock,
}

pub struct DrainedSessionOutput {
    session_key: TransportSessionKey,
    output: HostSessionOutput,
}

impl DrainedSessionOutput {
    pub(in crate::runtime::rtc_engine::packet_loop) fn new(
        session_key: TransportSessionKey,
        output: HostSessionOutput,
    ) -> Self {
        Self {
            session_key,
            output,
        }
    }
}

/// Poll every session that the scheduler reports as ready.
///
/// Readiness comes from dirty-session marks and exact timeout deadlines stored in
/// `RtcBootstrapState`. Sessions that disappeared before the drain are ignored,
/// which keeps teardown races harmless for already queued wakeups.
pub(in crate::runtime::rtc_engine::packet_loop) fn drain_ready_session_outputs(
    state: &mut RtcBootstrapState,
    outputs: &mut Vec<DrainedSessionOutput>,
    ready_sessions: &mut Vec<TransportSessionKey>,
    context: &SessionPollContext,
) {
    outputs.clear();
    state
        .packet_loop
        .drain_ready_sessions(context.packet_now, ready_sessions);
    for user_id in ready_sessions.drain(..) {
        let session_timeout = {
            let Some(session_state) = state.users.get_mut(&user_id) else {
                continue;
            };
            drain_single_session(&user_id, session_state, outputs, context)
        };
        state
            .packet_loop
            .update_session_timeout(&user_id, session_timeout);
    }
}

/// Drain all ready outputs from one host session.
///
/// # Output contract
///
/// Transmit outputs are staged for UDP send, RTP packets are staged for
/// forwarding, keyframe requests are staged for source resolution and selected
/// transport events update observability side channels. Immediate timeouts are
/// fed back through the host adapter in the same drain so the session reaches a
/// stable next deadline before control returns to the driver.
///
/// Returns the next timeout requested by the session, if any.
fn drain_single_session(
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    outputs: &mut Vec<DrainedSessionOutput>,
    context: &SessionPollContext,
) -> Option<PacketLoopTime> {
    loop {
        match session_state.host_session.poll_output() {
            Ok(HostSessionOutput::Timeout(timeout_at)) => {
                if timeout_at <= context.host_now {
                    if session_state
                        .host_session
                        .handle_timeout(context.host_now)
                        .is_err()
                    {
                        warn!(
                            user_id = ?session_key.user_id(),
                            media_worker_id = session_key.media_worker_id(),
                            "failed to apply immediate rtc packet-loop timeout input"
                        );
                        return None;
                    }
                } else {
                    return Some(context.clock.to_packet_time(timeout_at));
                }
            }
            Ok(output) => outputs.push(DrainedSessionOutput::new(session_key.clone(), output)),
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

pub(in crate::runtime::rtc_engine::packet_loop) fn apply_session_outputs(
    outputs: &mut Vec<DrainedSessionOutput>,
    scratch: &mut PacketLoopScratch,
    effects: &mut PacketLoopEffects,
) {
    for output in outputs.drain(..) {
        apply_session_output(output, scratch, effects);
    }
}

fn apply_session_output(
    output: DrainedSessionOutput,
    scratch: &mut PacketLoopScratch,
    effects: &mut PacketLoopEffects,
) {
    let DrainedSessionOutput {
        session_key,
        output,
    } = output;
    match output {
        HostSessionOutput::Transmit {
            destination,
            contents,
        } => {
            scratch.push_pending_transmit(destination, &contents);
        }
        HostSessionOutput::RtpPacket(packet) => {
            scratch.push_pending_packet(
                super::super::forwarded_packet::ForwardedPacket::from_rtp_packet(
                    session_key,
                    packet,
                ),
            );
        }
        HostSessionOutput::KeyframeRequest(request) => {
            scratch.push_pending_keyframe_request(
                session_key.clone(),
                PendingKeyframeRequest::new(request),
            );
            trace!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                mid = %request.mid,
                rid = ?request.rid,
                kind = ?request.kind,
                "queued route-level keyframe request from rtc packet-loop event"
            );
        }
        HostSessionOutput::Event(event) => {
            observe_rtc_event(&session_key, &event, effects);
        }
        HostSessionOutput::Timeout(_) => {
            warn!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                "rtc packet loop received a timeout after host session polling"
            );
        }
    }
}
