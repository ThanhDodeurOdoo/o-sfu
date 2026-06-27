use std::sync::Arc;

use o_sfu_protocol::wire::ServerEnvelope;

use crate::core::prelude::MediaSession;

mod client_input;
mod lifecycle;
mod media;
mod remote_sources;
mod room_events;

use media::NegotiationRequests;

pub type UserOutput = Vec<ServerEnvelope>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    ProtocolViolation,
    Kicked,
    InternalError,
}

#[derive(Debug)]
pub struct User {
    remote_address: Arc<str>,
    session: MediaSession,
    requests: NegotiationRequests,
    cleanup_finished: bool,
}
