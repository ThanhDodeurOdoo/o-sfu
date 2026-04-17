// TODO: needs documentation:
mod controller;
mod handshake;
mod io;
mod session_loop;
mod session_protocol;
#[cfg(test)]
mod tests;

pub(crate) use controller::{close_writer, upgrade};
pub(crate) use io::WsWriter;
