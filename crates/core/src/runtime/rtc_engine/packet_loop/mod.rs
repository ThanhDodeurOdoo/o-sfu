//! Shard-local media packet loop.
//!
//! The packet loop is the RTC engine's transport hot path. Each
//! `RtcTransportShard` starts one Tokio task that owns one mutable
//! `RtcBootstrapState`, drives all `str0m::Rtc` instances for that shard and
//! performs the UDP reads and writes for the shared shard socket.
//!
//! The loop is below room policy and above raw `str0m` I/O. Room
//! and router code project intent into transport state before packets arrive
//! here. This module applies that already-projected state to inbound UDP,
//! `str0m` outputs, local fanout, relay fanout, packet sinks and transport
//! observability.
//!
//! Authoritative media state lives in the worker-owned `RtcBootstrapState`.
//! `RtcSnapshotState`, bitrate counters, diagnostics, metrics, packet sinks and
//! source-policy signals are side channels used to expose observations or
//! enqueue work without letting external callers mutate the hot-path state
//! directly. Relay routing state stays in the worker-owned
//! `RtcBootstrapState`.
//!
//! # Packet-loop turn
//!
//! ```text
//! packet-loop turn
//!   |
//!   v
//! drain pending worker commands
//!   |
//!   v
//! mutate RtcBootstrapState and clear routing hints
//!   |
//!   v
//! snapshot_and_pump
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
//! send queued UDP transmits
//!   |
//!   v
//! biased wait for shutdown, command, timeout or UDP datagram
//!   |
//!   +--> shutdown or closed command channel -> return
//!   |
//!   +--> command -> mutate state and clear routing hints -> next turn
//!   |
//!   +--> timeout -> next turn
//!   |
//!   +--> UDP datagram -> route through demux recovery -> next turn
//! ```
//!
//! Commands are handled before media pumping, and the `tokio::select!` wait is
//! biased toward shutdown and commands. This keeps lifecycle work responsive
//! even when media traffic is heavy. Socket reads are still part of the same
//! turn. A packet that enters `str0m` marks its session dirty, then the next
//! turn drains any output produced by that input.
//!
//! # Submodules
//!
//! - [`loop_driver`] owns worker lifecycle, turn ordering, socket waits and the
//!   shared configuration passed in by the shard.
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
pub(in crate::runtime::rtc_engine) use input::PacketLoopInputReceivers;
pub(in crate::runtime::rtc_engine) use lag::PacketLoopLagSnapshot;
pub(in crate::runtime::rtc_engine) use loop_driver::{PacketLoopConfig, run_packet_loop};
