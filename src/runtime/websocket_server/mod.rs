#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod admission;
mod controller;
mod handshake;
pub(crate) mod io;
mod session;

pub use handshake::decode_auth_payload_text;
pub use io::{
    ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
    MAX_CLIENT_FRAME_BYTES, decode_client_batch,
};
pub(crate) use io::{WsReader, WsWriter};

pub(crate) use self::{admission::PreAuthWebSocketAdmission, controller::upgrade};
