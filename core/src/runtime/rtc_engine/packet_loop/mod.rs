//! Shard-local media packet loop.
//!
//! The packet loop is the RTC engine's transport hot path. Each
//! `RtcTransportShard` starts one Tokio task that owns one mutable
//! `RtcBootstrapState`, asks the host session adapter to drive shard-local
//! `RtcHostSession` instances and performs the UDP reads and writes for the shared
//! shard socket.
//!
//! The loop is below room policy and above raw `str0m` I/O. Room
//! and router code project intent into transport state before packets arrive
//! here. This module applies that already-projected state to inbound UDP,
//! `str0m` outputs, local fanout, relay fanout, packet sinks and transport
//! observability.
//!
//! Authoritative media state lives in worker-owned packet-loop state. The host
//! keeps sockets, host sessions, bitrate mirrors, diagnostics, metrics, packet
//! sinks and source-policy signals outside the deterministic turn.
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
//! mutate host state and clear routing hints
//!   |
//!   v
//! host polls dirty and timed-out sessions
//!   |
//!   v
//! collect host session outputs
//!   |
//!   v
//! PacketLoopTurn::step
//!   |
//!   +--> consume host session outputs
//!   |
//!   +--> drain bounded relay mailbox
//!   |
//!   +--> coalesce keyframe requests
//!   |
//!   +--> record ingress stats and source policy dirtiness
//!   |
//!   +--> populate forwarding destinations
//!   |
//!   +--> emit typed forwarding and observability effects
//!   |
//!   v
//! execute host effects in order
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
//! - [`loop_driver`] owns worker lifecycle, socket waits, effect execution and
//!   the shared configuration passed in by the shard.
//! - [`ingress_routing`] maps inbound UDP datagrams to a session with
//!   source-address pins, bounded recovery and the host session adapter as the
//!   final `str0m` authority.
//! - [`session_drain`] lets the host pull ready `str0m` outputs out of dirty or
//!   timed-out sessions, then lets the machine turn consume the normalized
//!   outputs without polling `str0m`.
//! - [`forward_flush`] records packet-path state and emits forwarding effects
//!   for local, relay and sink egress.
//! - [`route_snapshot`] gives the machine turn explicit source facts and stable
//!   host-route references without exposing host handles.
//! - [`host_effects`] executes typed effects against snapshots, metrics,
//!   diagnostics, packet sinks, relay mailboxes and local RTC sessions.
//! - [`keyframe_requests`] resolves consumer RTCP feedback back to the producer
//!   source, then coalesces duplicate requests before they leave the worker.
//! - [`event_observation`] translates selected `str0m` events into snapshots,
//!   diagnostics, source-policy wakeups and metrics.
//! - [`machine::scratch`] owns the reusable allocation surface used by the whole turn.
//! - [`machine::effect`] owns the ordered packet-loop effect values.
//! - [`machine::turn`] owns the deterministic synchronous packet-loop step.
//! - [`time`] owns deterministic packet-loop-local timestamps.
//! - [`host_clock`] owns host `Instant` to packet-loop time translation.

mod event_observation;
mod forward_flush;
mod host_clock;
mod host_effects;
mod ingress_routing;
mod input;
pub(in crate::runtime::rtc_engine) mod keyframe_requests;
mod loop_driver;
pub(in crate::runtime::rtc_engine) mod machine;
pub(in crate::runtime::rtc_engine) mod route_snapshot;
pub(in crate::runtime::rtc_engine) mod selected_rid;
mod session_drain;
#[cfg(test)]
mod tests;
pub(in crate::runtime::rtc_engine) mod time;
#[cfg(any(test, feature = "packet-loop-verification"))]
pub(in crate::runtime::rtc_engine) mod verification;

#[cfg(test)]
pub use event_observation::{transport_health_from_event, transport_ice_state};
pub(in crate::runtime::rtc_engine) use input::PacketLoopInputReceivers;
pub(in crate::runtime::rtc_engine) use loop_driver::{PacketLoopConfig, run_packet_loop};
