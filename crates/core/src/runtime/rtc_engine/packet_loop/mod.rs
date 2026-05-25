//! Worker-local media packet loop.
//!
//! The packet loop is the RTC engine's transport hot path. Each
//! `RtcWorker` starts one Tokio task that owns one mutable
//! `PacketLoopState`, drives all `str0m::Rtc` instances for that worker and
//! performs the UDP reads and writes for the shared worker socket.
//!
//! The loop is below room policy and above raw `str0m` I/O. Room
//! and router code project intent into transport state before packets arrive
//! here. This module applies that already-projected state to inbound UDP,
//! `str0m` outputs, local fanout, relay fanout, packet sinks and transport
//! observability.
//!
//! Authoritative media state lives in the worker-owned `PacketLoopState`.
//! `RtcSnapshotState`, bitrate counters, diagnostics, metrics, packet sinks and
//! source-policy signals are side channels used to expose observations or
//! enqueue work without letting external callers mutate the hot-path state
//! directly. Relay routing state stays in the worker-owned
//! `PacketLoopState`.
//!
//! # Packet-loop turn
//!
//! ```text
//! packet-loop turn
//!   |
//!   v
//! PacketLoopTurn::apply_input
//!   |
//!   +--> control -> mutate PacketLoopState and clear routing hints
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
//!   +--> coalesce keyframe requests
//!   |
//!   +--> record ingress stats and source policy dirtiness
//!   |
//!   +--> populate forwarding destinations
//!   |
//!   +--> flush local RTC, relay and packet-sink destinations
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
//! a packet that enters `str0m` marks its session dirty, then the next turn
//! drains any output produced by that input
//!
//! # Submodules
//!
//! - [`loop_driver`] owns worker lifecycle, turn ordering, socket waits and the
//!   shared configuration passed in by the worker.
//! - [`ingress_routing`] maps inbound UDP datagrams to a session with
//!   source-address pins, bounded recovery and `Rtc::accepts()` as the final
//!   authority.
//! - [`session_drain`] pulls ready `str0m` outputs out of dirty or timed-out
//!   sessions without scanning every live session on every turn.
//! - [`forward_flush`] records packet-path stats, executes planned destinations
//!   and accounts for local, relay and sink egress.
//! - [`keyframe_requests`] resolves consumer RTCP feedback back to the producer
//!   source, then coalesces duplicate requests before they leave the worker.
//! - [`event_observation`] translates selected `str0m` events into snapshots,
//!   diagnostics, source-policy wakeups and metrics.
//! - [`buffers`] owns the reusable allocation surface used by the whole turn.

mod buffers;
mod event_observation;
mod forward_flush;
mod ingress_routing;
mod input;
mod keyframe_requests;
mod lag;
mod loop_driver;
mod session_drain;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub use event_observation::{transport_health_from_event, transport_ice_state};

#[cfg(any(test, feature = "internal-benchmarks", feature = "fuzzing"))]
pub(in crate::runtime::rtc_engine) use self::ingress_routing::{
    PacketRouteDatagram, route_packet_to_matching_session_at,
};
#[cfg(feature = "internal-benchmarks")]
pub(in crate::runtime::rtc_engine) use self::{
    buffers::PacketLoopBuffers,
    forward_flush::{flush_forward_routes, record_incoming_stats_for_benchmark},
    keyframe_requests::{PendingKeyframeRequest, flush_pending_keyframe_requests_at},
};
pub(in crate::runtime::rtc_engine) use self::{
    input::PacketLoopInputReceivers,
    lag::PacketLoopLagSnapshot,
    loop_driver::{PacketLoopConfig, run_packet_loop},
};
