//! This module is the synchronous sibling of `websocket_server`: it exposes the server's
//! endpoints, translates authenticated HTTP requests into application
//! room intents, and then renders the resulting public HTTP contract.
//!
//! ```text
//! http_server
//! |- controller -> Axum app construction plus route-level parsing/response shaping
//! `- services   -> auth-aware edge parsing helpers behind the route handlers
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
