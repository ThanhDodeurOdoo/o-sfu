//! This module is the synchronous sibling of `websocket_server`: it exposes the server's
//! endpoints, translates authenticated HTTP requsts into runtime
//! operations, and then delegates room-level work to `room` ownership instead of
//! keeping business logic inside route handlers.
//!
//! ```text
//! http_server
//! |- controller -> Axum app construction plus route-level parsing/response shaping
//! `- services   -> auth-aware room/disconnect helpers behind the route handlers
//! ```
//!
//! Read this node before the WebSocket path when you need the server's non-streaming
//! control plane.

pub(crate) mod contract;
mod controller;
mod services;
#[cfg(test)]
mod tests;

pub(crate) use controller::{app, serve_http};
pub(crate) use services::request_base_url;
