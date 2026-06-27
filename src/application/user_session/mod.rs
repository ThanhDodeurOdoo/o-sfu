use std::sync::Arc;

use o_sfu_protocol::wire::ServerEnvelope;

use crate::core::prelude::MediaSession;

mod client_input;
mod lifecycle;
mod media;
mod remote_sources;
mod room_events;

use media::ServerNegotiation;

pub type UserOutput = Vec<ServerEnvelope>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    ProtocolViolation,
    Kicked,
    InternalError,
}

pub struct User {
    remote_address: Arc<str>,
    session: MediaSession,
    negotiation: ServerNegotiation,
    cleanup_finished: bool,
}
