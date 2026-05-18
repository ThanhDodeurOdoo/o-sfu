//! Concrete RTC execution engine for the media transport.
//!
//! This module contain the Str0m-backed implementation that turns media transport
//! intents into actual WebRTC state and packet movement. It sits below
//! [`crate::runtime::media_transport`]: higher layers should not import this
//! module to create offers, publish media, or inspect transport state unless
//! they are writing focused RTC-engine tests or backend integration code.
//!
//! The engine is worker-oriented. Each [`RtcTransportWorker`] owns one lazy
//! packet loop, command and relay mailboxes, worker-local relay target state,
//! diagnostics hooks, packet-sink fanout, bitrate snapshots, and the state machines
//! needed to drive Str0m. The surrounding media transport worker manager decides
//! which session belongs to which worker and hides cross-worker relay setup
//! from room orchestration.
//!
//! Internal ownership is split by the kind of RTC work being performed:
//!
//! - `api`: worker facade, lazy worker lifecycle, command dispatch, and
//!   production/test support entry points;
//! - `bootstrap`, `commands`, `worker`, and `state`: offer/answer bootstrap,
//!   mailbox contracts, worker-local mutations, and pure RTC session state;
//! - `packet_loop`, `demux`, `forwarded_packet`, `forwarding_destination`,
//!   `forwarding_planner`, `local_forwarding`, and `shared_payload`: UDP/RTP
//!   ingress, routing, fanout planning, local sends, recording packet sinks,
//!   and zero-copy payload ownership;
//! - `media_registry`, `relay_registry`, `route_control`, `routing_miss`,
//!   `bitrate`, and `negotiated_capabilities`: transport media ownership,
//!   relay mailbox and target primitives, packet gates, active-speaker observations,
//!   unknown-source recovery, observability snapshots, and answer-derived RTP
//!   capability projection;
//! - `simulcast`: RTC-edge simulcast negotiation helpers.

mod api;
mod bitrate;
mod bootstrap;
mod commands;
mod demux;
mod forwarded_packet;
mod forwarding_destination;
mod forwarding_planner;
mod local_forwarding;
mod local_send_rewrite;
mod media_registry;
mod negotiated_capabilities;
mod packet_loop;
mod relay_registry;
mod route_control;
mod routing_miss;
mod shared_payload;
mod simulcast;
mod slots;
mod state;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
#[cfg(test)]
mod tests;
mod worker;

pub use api::RtcTransportWorker;
#[cfg(any(test, feature = "testing-transport"))]
pub use forwarded_packet::ForwardedPacket;
pub use negotiated_capabilities::client_rtp_capabilities_from_answer;

pub use crate::runtime::media_transport::TransportSessionHealth;
