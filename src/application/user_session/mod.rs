use std::sync::Arc;

use o_sfu_protocol::wire::ServerEnvelope;

mod client_input;
mod lifecycle;
mod media;
mod remote_sources;
mod room_events;

use media::ServerMediaNegotiation;

pub type UserOutput = Vec<ServerEnvelope>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    ProtocolViolation,
    Kicked,
    InternalError,
}

pub struct User {
    remote_address: Arc<str>,
    media: ServerMediaNegotiation,
    cleanup_finished: bool,
}
