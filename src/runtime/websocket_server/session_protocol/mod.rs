//! Authentication ends before this module starts. From that point onward, this node
//! translates framed WebSocket traffic into protocol state transitions, request
//! bookkeeping, and channel-facing business operations without leaking raw socket
//! concerns into the room (/channel) layer
//!
//! ```text
//! session_protocol
//! |- controller      -> facade between the socket loop and protocol subflows
//! |- frame_codec     -> frame and envelope encoding/decoding
//! |- flow_state      -> unified session-scoped negotiation and queued-change state
//! |- track_projection-> server track state projected into protocol payloads
//! `- post_auth       -> steady-state authenticated signaling orchestration
//!
mod controller;
mod flow_state;
pub(super) mod frame_codec;
mod post_auth;
mod track_projection;

pub(super) use controller::{SessionProtocol, SessionProtocolOutcome};
