//! Authentication ends before this module starts. From that point onward, this node
//! translates framed WebSocket traffic into protocol state transitions, request
//! bookkeeping, and room-facing business operations without leaking raw socket
//! concerns into the room (/room) layer
//!
//! ```text
//! session_protocol
//! |- controller      -> facade between the socket loop and protocol subflows
//! |- frame_codec     -> frame and envelope encoding/decoding
//!
mod controller;
pub(super) mod frame_codec;

pub(super) use controller::{SessionProtocol, SessionProtocolOutcome};
