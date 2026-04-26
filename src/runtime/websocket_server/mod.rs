//! This node turns an upgraded socket into an authenticated (rtc) user. It owns the
//! transport-specific mechanics of the WebSocket connection and then delegate the
//! business meaning of authenticated messages to `session_protocol`
//!
//! ```text
//! websocket_server
//! |- controller       -> upgrade boundary and outer user lifecycle
//! |- handshake        -> first-frame authentication and room admission
//! |- session_loop     -> steady-state reader/writer loop after admission
//! |- session_protocol -> authenticated signaling flow
//! `- io               -> socket writer boundary and close helpers
//! ```
//!
//! The rest of the runtime should traet this module as the sole owner of WebSocket
//! mechanics such as close codes, ping/pong liveness, and reader/writer management.
mod controller;
mod handshake;
pub(crate) mod io;
mod session_loop;
mod session_protocol;
#[cfg(test)]
mod tests;

pub(crate) use controller::{close_writer, upgrade};
pub(crate) use handshake::decode_auth_payload_text;
pub(crate) use io::{
    ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES, WsWriter, decode_client_batch,
};
