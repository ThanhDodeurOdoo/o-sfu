//! Runtime transport adapter for the `rtc` WebRTC backend.
//!
//! Internal modules:
//! - `api`: runtime adapter facade and worker lifecycle
//! - `commands`: worker mailbox contract and debug-only test commands
//! - `worker`: command dispatch and worker-local state mutations
//! - `state`: pure state types and session scheduling
//! - `media_registry`: media handle tracking and mid registry
//! - `demux`: IP hash-indexed demux and media route entries
//! - `forwarded_packet`: adapter-local forwarded RTP packet model and local send edges
//! - `forwarding_destination`: named packet-forwarding destinations for local RTC, recording, intra-node relay, and inter-node relay sends
//! - `forwarding_planner`: adapter-local destination planning over forwarded packets
//! - `local_forwarding`: destination-local send boundary for packet fan-out
//! - `relay_registry`: source-media-scoped relay targets for inter-worker mailboxes and future inter-node forwarding
//! - `route_control`: transport-native control policy for keyed feedback absorption and gating
//! - `shared_payload`: adapter-local payload ownership boundary for forwarding and recording
//! - `bootstrap`: socket binding, session RTC state initialization, transport payload construction
//! - `packet_loop`: async UDP packet loop, session output pumping, incoming packet routing
//! - `validation`: DTLS/SDP/ICE parameter validation and diagnostic mapping
//! - `dtls`: DTLS parameter parsing (RFC 8122, RFC 4572)
//! - `ice`: ICE candidate parsing (RFC 8839, RFC 8445)
//! - `sdp`: SDP offer parsing (RFC 8866)
//! - `negotiated_capabilities`: answer-side RTP capability projection for native signaling
//! - `parse_diagnostic`: shared parse diagnostic infrastructure

mod api;
mod bootstrap;
mod commands;
mod demux;
#[cfg(test)]
mod dtls;
mod forwarded_packet;
mod forwarding_destination;
mod forwarding_planner;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod ice;
mod local_forwarding;
mod media_registry;
mod negotiated_capabilities;
mod packet_loop;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod parse_diagnostic;
mod relay_registry;
mod route_control;
#[cfg(test)]
mod sdp;
mod shared_payload;
mod state;
#[cfg(test)]
mod tests;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod validation;
mod worker;

pub(crate) use api::RtcTransportAdapter;
#[cfg(test)]
pub(crate) use commands::DebugRouteEntry;
pub(crate) use commands::RelayCleanup;
pub(crate) use forwarded_packet::ForwardedPacket;
#[cfg(test)]
pub(crate) use forwarded_packet::sample_forwarded_packet;
#[cfg(test)]
pub(crate) use forwarded_packet::sample_forwarded_packet_with_audio_activity;
#[cfg(test)]
pub(crate) use forwarded_packet::sample_forwarded_packet_with_rid;
pub(crate) use negotiated_capabilities::client_rtp_capabilities_from_answer;
pub(crate) use state::TransportSessionHealth;
