//! This module is the synchronous sibling of `websocket_server`: it exposes the server's
//! endpoints, translates authenticated HTTP requests into application
//! room intents, and then renders the resulting public HTTP contract.
//!
//! ```text
//! http_server
//! |- controller -> Axum app construction plus route-level parsing/response shaping
//! ```
//!
//! Read this node before the WebSocket path when you need the server's non-streaming
//! control plane.

pub(crate) mod contract;
mod controller;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use controller::app;
pub(crate) use controller::{serve_http, serve_http_on};
