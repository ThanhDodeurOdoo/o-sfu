// TODO: needs documentation:
mod controller;
pub(super) mod frame_codec;
mod negotiation;
mod post_auth;
mod request_state;
mod track_projection;

pub(super) use controller::{SessionProtocol, SessionProtocolOutcome};
