//! this module owns the cold-path compatibility flow for one authenticated
//! websocket connection. it accepts Odoo protocol intent, translates stream
//! labels through [`crate::application::stream_catalog`] and calls the media
//! facade with generic source intents
//!
//! business-layer changes to publication shape should enter core as
//! [`crate::core::prelude::SourcePublishIntent`] and
//! [`crate::core::prelude::SourceSubscriptionIntent`] values. `User` sequences those
//! intents around negotiation, request tracking and user-info fanout, while the
//! pure connection-local state lives under `state/` and ordered websocket
//! output lives in `output`
//!
//! `User` is the post-auth websocket session facade. it keeps the
//! connection-scoped signaling state needed to answer one browser, including
//! pending request ids, staged renegotiation decisions and compatibility track
//! snapshots

use std::sync::Arc;

use o_sfu_protocol::wire::UserId;

use crate::{
    core::prelude::SfuCore,
    runtime::{ConnectionId, room::Room},
};

mod compat;
mod lifecycle;
mod negotiation;
mod output;
mod projection;
mod publish;
mod room_events;
mod state;
mod subscribe;

pub use output::{UserOutput, UserSignal};
use state::UserState;

/// User-loop exit reason derived from media endpoint health.
///
/// This is a best-effort transport observation. It is not an authoritative room
/// membership check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDisconnectReason {
    /// The media transport reached a terminal disconnected state.
    TransportDisconnected,
}

/// User-session failure category reported to the websocket runtime.
///
/// # Error handling
///
/// These errors are already translated out of core and room outcomes. The
/// websocket edge maps them to close codes, so callers should not inspect log
/// text to decide whether a socket stays usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserError {
    /// The browser sent a message that cannot be accepted for this session.
    ProtocolViolation,
    /// The room no longer owns this exact user connection.
    Kicked,
    /// A server-side media or transport operation failed.
    InternalError,
}

/// Post-auth application session for one websocket connection.
///
/// `User` owns connection-local negotiation state, local compatibility track
/// bindings for the connected browser and cleanup completion. It does not own
/// room membership, media publications or transport resources. Those stay
/// behind [`Room`] and [`crate::core::prelude::MediaSession`], which keeps this boundary
/// focused on translating Odoo websocket intent into core media intent.
///
/// # Concurrency
///
/// Methods are cold-path orchestration calls. They may await room snapshots,
/// media transactions and transport effects. The room and core layers remain
/// responsible for not holding their state locks across transport work.
///
/// # Lifecycle
///
/// The websocket handshake constructs a `User` only after room admission. The
/// steady-state loop must call [`User::close`] before dropping it so staged
/// publishes that never reached room commit are rolled back explicitly.
#[derive(Debug)]
pub struct User {
    /// Compatibility-facing identity for room state and websocket payloads.
    id: UserId,
    /// Runtime-local identity that separates replacement sockets for one user.
    connection_id: ConnectionId,
    /// Log context for negotiation and media failures.
    ///
    /// The address is not part of authentication or room identity.
    remote_address: Arc<str>,
    /// Authoritative room facade for membership, snapshots and fanout.
    room: Arc<Room>,
    /// Process media facade used to build borrow-based session handles.
    sfu_core: SfuCore,
    /// Connection-local request sequencing and compatibility wire state.
    state: UserState,
    /// Whether async staged-publish cleanup has completed for this connection.
    cleanup_finished: bool,
}
