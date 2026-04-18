//! This module is the synchronous sibling of `websocket_server`: it exposes the server's
//! endpoints, translates authenticated HTTP requsts into runtime
//! operations, and then delegates room-level work to `channel` ownership instead of
//! keeping business logic inside route handlers.
//!
//! ```text
//! http_server
//! `- controller -> Axum app construction plus channel and disconnect endpoints
//! ```
//!
//! Read this node before the WebSocket path when you need the server's non-streaming
//! control plane.

pub(crate) mod contract;
mod controller;
#[cfg(test)]
mod tests;

pub(crate) use controller::{app, serve_http};
