//! Runtime transport adapter for the `rtc` WebRTC backend.
//!
//! Internal modules:
//! - `api`: runtime adapter facade and worker lifecycle
//! - `commands`: worker mailbox contract and debug-only test commands
//! - `worker`: command dispatch and worker-local state mutations
//! - `state`: pure state types and session scheduling
//! - `media_registry`: media handle tracking and mid registry
//! - `demux`: IP hash-indexed demux and media route entries
//! - `forwarded_packet`: adapter-local forwarded packet model and packet-mode-specific send edges
//! - `local_forwarding`: destination-local send boundary for packet fan-out
//! - `shared_payload`: adapter-local payload ownership boundary for forwarding and recording
//! - `bootstrap`: socket binding, session RTC state initialization, transport payload construction
//! - `packet_loop`: async UDP packet loop, session output pumping, incoming packet routing
//! - `packet_mode`: adapter-local str0m frame-versus-RTP mode switch for the forwarding path
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
#[cfg(any(test, feature = "internal-benchmarks"))]
mod ice;
mod local_forwarding;
mod media_registry;
mod negotiated_capabilities;
mod packet_loop;
mod packet_mode;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod parse_diagnostic;
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
pub(crate) use forwarded_packet::ForwardedPacket;
#[cfg(test)]
pub(crate) use forwarded_packet::sample_forwarded_packet;
pub(crate) use negotiated_capabilities::client_rtp_capabilities_from_answer;
pub(crate) use state::TransportSessionHealth;
