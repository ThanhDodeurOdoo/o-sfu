//! This node turns an upgraded socket into an authenticated RTC user. It handles
//! WebSocket mechanics then delegates authenticated messages to
//! [`crate::application::user_session::User`].
//!
//! ```text
//! websocket_server
//! |- admission        -> pre-auth WebSocket concurrency budget
//! |- controller       -> upgrade boundary
//! |- handshake        -> first-frame authentication
//! |- session_loop     -> active socket lifecycle after authentication
//! `- io               -> socket writer boundary and close helpers
//! ```
//!
//! The rest of the runtime should keep WebSocket close codes, ping/pong liveness
//! and reader/writer management behind this module.
mod admission;
mod controller;
mod handshake;
pub(crate) mod io;
mod session_loop;
#[cfg(test)]
mod tests;

pub use handshake::decode_auth_payload_text;
pub use io::{
    ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
    MAX_CLIENT_FRAME_BYTES, decode_client_batch,
};
pub(crate) use io::{WsReader, WsWriter};

pub(crate) use self::{admission::PreAuthWebSocketAdmission, controller::upgrade};
