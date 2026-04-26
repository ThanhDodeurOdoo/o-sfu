//! Post-auth signaling for one admitted user.
//!
//! This node is where authenticated client intent becomes room-level work. It keeps the
//! outer user loop small by centralizing envelope dispatch, renegotiation sequencing,
//! and staged publish transitions behind one user-scoped protocol owner.
//!
//! ```text
//! post_auth
//! |- controller        -> user-scoped protocol owner
//! |- envelope_dispatch -> routes one client envelope to the correct business flow
//! |- negotiation_flow  -> offer/answer and renegotiation sequencing
//! |- publish_flow      -> staged publish commit and cleanup
//! ```
//!
//! Read this node as the bridge between the authenticated protocol surface and the
//! `room` runtime.
mod controller;
mod envelope_dispatch;
mod negotiation_flow;
mod publish_flow;

pub(in crate::runtime::websocket_server) use controller::{
    PostAuthProtocolOutcome, PostAuthSessionProtocol,
};
