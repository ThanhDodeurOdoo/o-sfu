mod controller;
mod handshake;
mod io;
mod session_loop;
mod session_protocol;
#[cfg(test)]
mod tests;

pub(crate) use controller::{close_writer, upgrade};
pub(crate) use io::WsWriter;
#[cfg(test)]
pub(crate) use session_protocol::SessionProtocolMode;

#[doc(hidden)]
#[must_use]
pub(crate) fn fuzz_decode_native_client_batch(payload: &str) -> bool {
    session_protocol::frame_codec::decode_client_batch(payload).is_ok()
}
