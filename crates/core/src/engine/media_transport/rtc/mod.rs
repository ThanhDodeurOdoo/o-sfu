//! Private RTC backend for the media transport.
//!
//! This module contains the Str0m-backed implementation that turns media transport
//! intents into actual WebRTC state and packet movement. It sits below
//! [`crate::engine::media_transport`]: higher layers should not import this
//! module to create offers, publish media, or inspect transport state unless
//! they are writing focused RTC backend tests or backend integration code.
//!
//! The backend is worker-oriented. Each [`RtcWorker`] owns one lazy
//! packet loop, command and relay mailboxes, worker-local relay target state,
//! diagnostics hooks, packet-sink fanout, bitrate snapshots plus the state machines
//! needed to drive Str0m. The surrounding media transport worker manager decides
//! which session belongs to which worker and hides cross-worker relay setup
//! from room code.
//!
//! Internal ownership is split by the kind of RTC work being performed:
//!
//! - `worker`: worker API, lazy lifecycle, command handlers and
//!   production/test support entry points.
//! - `bootstrap`, `commands` and `state`: offer/answer bootstrap, mailbox
//!   contracts and pure RTC session state.
//! - `packet_loop`, `demux`, `source_route`, `forwarded_packet`, `forwarding_destination`,
//!   `forwarding_planner` and `local_forwarding`: UDP/RTP
//!   ingress, source route facts, fanout planning, local sends, recording
//!   packet sinks and zero-copy payload ownership.
//! - `media_registry`, `relay_registry`, `route_control`, `routing_miss`,
//!   `bitrate` and `negotiated_capabilities`: transport media ownership,
//!   relay mailbox and target primitives, packet gates, active-speaker observations,
//!   unknown-source recovery, observability snapshots plus answer-derived RTP
//!   capability projection.
//! - `simulcast`: RTC-edge simulcast negotiation helpers.

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(feature = "internal-benchmarks")]
#[path = "TESTS/benchmark_support/mod.rs"]
pub mod benchmark_support;
mod bitrate;
mod bootstrap;
mod commands;
mod demux;
mod forwarded_packet;
mod forwarding_destination;
mod forwarding_planner;
#[cfg(any(test, feature = "fuzzing"))]
#[path = "TESTS/fuzz_support/mod.rs"]
pub(crate) mod fuzz_support;
mod keyframe_tracker;
mod local_forwarding;
mod local_send_rewrite;
mod media_registry;
mod negotiated_capabilities;
mod packet_loop;
mod relay_registry;
mod route_control;
mod route_table;
mod routing_miss;
mod simulcast;
mod slots;
mod source_route;
mod state;
#[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;
mod worker;

pub use commands::{
    ConsumerPacketGateCommand, RouteControlRequest, RtcMediaControlCommand, RtcWorkerCommand,
    RtcWorkerResponse,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use forwarded_packet::ForwardedPacket;
pub use negotiated_capabilities::client_rtp_capabilities_from_answer;
pub use worker::RtcWorker;
