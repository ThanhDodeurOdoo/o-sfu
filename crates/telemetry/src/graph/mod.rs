//! Grafana node graph formatting for diagnostics snapshots.
//!
//! This module is the last formatting step before the
//! `/internal/diagnostics/node-graph/...` routes return JSON to the
//! `o-sfu-telemetry` Grafana dashboard. The input is an already assembled
//! diagnostics room detail, so these formatters do not query live runtime
//! state, hold locks, or decide whether a route is valid.
//!
//! Graph formatting is a cold diagnostics path. Allocating strings and JSON
//! values is acceptable here because the endpoint runs on demand after the room
//! snapshot has already been collected.

mod common;
mod room;
mod user;

pub use self::{room::build_graph, user::build_user_graph};
