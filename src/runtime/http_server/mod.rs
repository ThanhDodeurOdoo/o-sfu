//! This module is the synchronous sibling of `websocket_server`: it exposes the server's
//! endpoints, translates authenticated HTTP requests into application
//! room intents, and then renders the resulting public HTTP contract.
//!
//! ```text
//! http_server
//! |- controller -> Axum app construction plus route wiring and response shaping
//! |- extractors -> typed request extraction and verified route inputs
//! ```
//!
//! Read this node before the WebSocket path when you need the server's non-streaming
//! control plane.

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
pub(crate) mod contract;
mod controller;
mod extractors;

#[cfg(test)]
pub(crate) use controller::app;
pub(crate) use controller::{serve_http, serve_http_on};
