//! Worker-local route control for declared media.
//!
//! This module exists because consumer-route mutation touches several pieces of
//! state that must stay consistent:
//!
//! - `media_route_index` records which consumer transports depend on a source
//! - remote-source registrations track cross-worker packet gates and keyframes
//! - `route_control` keeps the effective local, relay and server-owned gates
//!
//! `lifecycle.rs` owns media declaration and teardown against `RtcSessionState`.
//! Once a producer or consumer handle exists, this module contain the routing-side
//! bookkeeping that validates sources, registers or removes consumer routes and
//! recomputes packet-gate state.
//!
//! Small ownership graph:
//!
//! ```text
//! lifecycle.rs
//!   |-- declare/remove str0m media
//!   |-- register/remove media handles
//!   `-- call control/ when route ownership changes
//!
//! control/
//!   |-- validate source ownership (local vs remote)
//!   |-- mutate media_route_index
//!   |-- refresh route_control packet gates
//!   `-- keep remote-source packet gates executable
//!
//! keyframe.rs
//!   `-- reads the same source-ownership rules for feedback routing
//! ```
//!
//! The `respond_*` functions at the top are command-adapter entry points for the
//! worker dispatcher. The lower worker functions keep the ownership checks close
//! to the state they protect.

mod remote_source;
mod responses;
mod routes;
mod selected_rid;

pub(in crate::runtime::rtc_engine::worker) use responses::{
    respond_add_relay_target, respond_remove_relay_target, respond_request_consumer_keyframe,
    respond_set_consumer_active, respond_set_consumer_packet_gate,
    respond_set_consumer_packet_gates, respond_set_producer_active,
    respond_set_relay_target_active, respond_set_remote_source_packet_gate,
};
pub(super) use routes::{
    ConsumerRouteRegistration, consumer_payload_type, ensure_existing_route_source,
    ensure_route_source_registered, owned_local_producer_mid, packet_gate_rid,
    register_consumer_route, remove_consumer_route,
};
pub(in crate::runtime::rtc_engine::worker) use routes::{
    refresh_source_packet_gate, remove_source_route,
};
#[cfg(test)]
pub(in crate::runtime::rtc_engine::worker::handlers::media) use selected_rid::observe_source_rid_readiness;
pub(in crate::runtime::rtc_engine) use selected_rid::{
    apply_source_rid_readiness, drain_due_rid_keyframe_refreshes,
};
