//! Worker-local media packet loop.
//!
//! The packet loop is the RTC engine's transport hot path. Each
//! [`RtcWorker`](super::worker::RtcWorker) runs one packet loop on its worker
//! thread. The loop holds one mutable
//! [`PacketLoopState`](super::state::PacketLoopState), drives every [`str0m::Rtc`]
//! for that worker and sends through the shared socket. A spawned [`UdpIngress`]
//! task forwards completed receives over mpsc.
//!
//! The loop is below room policy and above raw `str0m` I/O. Room
//! and router code project intent into transport state before packets arrive
//! here. This module applies that already-projected state to inbound UDP,
//! `str0m` outputs, local fanout, relay fanout, packet sinks and transport
//! observability.
//!
//! Authoritative media state lives in [`PacketLoopState`](super::state::PacketLoopState).
//! [`RtcSnapshotState`](super::state::RtcSnapshotState), bitrate counters,
//! diagnostics, metrics, packet sinks and source-policy signals are side
//! channels used to expose observations or enqueue work without letting
//! external callers mutate the hot-path state directly. Relay routing state stays in
//! [`PacketLoopState`](super::state::PacketLoopState).
//!
//! # Packet-loop turn
//!
//! ```text
//! packet-loop turn
//!   |
//!   v
//! PacketLoopTurn::apply_input
//!   |
//!   +--> control -> mutate PacketLoopState and clear demux recovery hints
//!   |
//!   +--> timeout -> keep already due sessions ready
//!   |
//!   +--> UDP datagram -> route through demux recovery
//!   |
//!   v
//! PacketLoopTurn::pump
//!   |
//!   +--> drain dirty and timed-out Rtc sessions
//!   |      |
//!   |      v
//!   |    collect str0m Output values
//!   |
//!   +--> drain bounded relay mailbox
//!   |
//!   +--> resolve keyframe requests
//!   |
//!   +--> for each staged RTP packet
//!   |      |
//!   |      +--> resolve PacketFacts and update observations
//!   |      |
//!   |      +--> plan current destinations
//!   |      |
//!   |      +--> flush before the next packet can change route state
//!   |
//!   +--> request ingress recovery keyframes and flush policy wakeups
//!   |
//!   +--> drain pending keyframe retries
//!   |
//!   v
//! PacketLoopTurn::flush_outputs
//!   |
//!   v
//! PacketLoopTurn::wait_for_next_input
//!   |
//!   +--> shutdown or closed command channel -> return
//!   |
//!   +--> one control, timeout, relay wake or UDP datagram -> next turn
//!   |
//!   +--> relay wake stages one packet for bounded pump-phase draining
//! ```
//!
//! each turn applies at most one control, timeout or UDP datagram input before
//! media pumping
//! the `tokio::select!` wait is biased toward shutdown and commands
//! this keeps lifecycle work responsive even when media traffic is heavy
//! relay packets remain pump-phase media work, with one relay wake allowed to
//! resume an idle loop
//! a packet that enters `str0m` marks its session dirty and `pump` drains the
//! resulting output in the same turn
//! local and relay fanout mark a destination session after draining, so its
//! output leaves on the next turn
//!
//! # Submodules
//!
//! - [`loop_driver`] owns worker lifecycle, turn ordering, completed-datagram
//!   waits and the shared configuration passed in by the worker.
//! - [`udp`] owns the private target-specific RTC UDP socket and ingress pump.
//! - [`ingress_routing`] maps inbound UDP datagrams to a session with
//!   source-address pins, bounded recovery and `Rtc::accepts()` as the final
//!   authority.
//! - [`session_drain`] pulls ready `str0m` outputs out of dirty or timed-out
//!   sessions without scanning every live session on every turn.
//! - [`forward_flush`] records packet-path stats, executes planned destinations
//!   and accounts for local, relay and sink egress.
//! - [`keyframe_requests`] resolves consumer RTCP feedback back to the producer
//!   source, then dispatches bounded requests and pending retries.
//! - [`event_observation`] translates selected `str0m` events into snapshots,
//!   diagnostics, source-policy wakeups and metrics.
//! - [`buffers`] owns the reusable allocation surface used by the whole turn.

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod buffers;
mod delay;
mod event_observation;
mod forward_flush;
mod ingress_routing;
mod input;
mod keyframe_requests;
mod loop_driver;
mod session_drain;
mod udp;

#[cfg(test)]
pub use event_observation::{transport_health_from_event, transport_ice_state};

#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use self::forward_flush::flush_packet_forwards;
#[cfg(any(test, feature = "internal-benchmarks", fuzzing))]
pub use self::ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at};
#[cfg(feature = "internal-benchmarks")]
pub use self::{
    buffers::PacketLoopBuffers,
    forward_flush::{drain_relay_packets, record_incoming_stats_for_benchmark},
    keyframe_requests::{PendingKeyframeRequest, flush_pending_kf_reqs_at},
    loop_driver::route_queued_ingress_datagrams_for_benchmark,
    session_drain::{SessionDrainContext, drain_ready_sessions},
    udp::UdpIngressBenchHarness,
};
pub use self::{
    delay::PacketLoopDelaySnapshot,
    input::PacketLoopInputReceivers,
    loop_driver::{PacketLoopConfig, run_packet_loop},
    udp::{RtcUdpSocket, UdpIngress},
};
