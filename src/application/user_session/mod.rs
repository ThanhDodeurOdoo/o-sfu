use std::sync::Arc;

use o_sfu_protocol::wire::ServerEnvelope;

use crate::core::prelude::MediaSession;

mod client_input;
mod lifecycle;
mod media;
mod room_events;
mod track_snapshot;

use media::NegotiationRequests;
use track_snapshot::TrackSnapshot;

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
    tracks: TrackSnapshot,
    cleanup_finished: bool,
}
