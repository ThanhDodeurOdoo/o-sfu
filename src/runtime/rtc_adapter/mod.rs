//! Runtime transport adapter for the `rtc` WebRTC backend.
//!
//! Internal modules:
//! - `api`: runtime adapter facade and worker lifecycle
//! - `commands`: worker mailbox contract and debug-only test commands
//! - `worker`: command dispatch and worker-local state mutations
//! - `state`: pure state types and session scheduling
//! - `media_registry`: media handle tracking and mid registry
//! - `demux`: IP hash-indexed demux and media route entries
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
mod dtls;
mod ice;
mod media_registry;
mod negotiated_capabilities;
mod packet_loop;
mod parse_diagnostic;
mod sdp;
mod state;
#[cfg(test)]
mod tests;
mod validation;
mod worker;

pub(crate) use api::RtcTransportAdapter;
pub(crate) use negotiated_capabilities::client_rtp_capabilities_from_answer;
