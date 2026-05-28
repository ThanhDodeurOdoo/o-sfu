//! Worker-local route control for declared media.
//!
//! This module exists because consumer-route mutation touches several pieces of
//! state that must stay consistent:
//!
//! - `media_route_index` records which consumer transports depend on a source
//! - remote-source registrations track cross-worker packet gates and keyframes
//! - route control keeps the effective local, relay and source policy gates
//!
//! [`lifecycle`](super::lifecycle) handles media declaration and teardown against
//! [`RtcSessionState`](crate::engine::media_transport::rtc::state::RtcSessionState).
//! Once a producer or consumer handle exists, this module
//! contains the routing-side bookkeeping that validates sources, registers or
//! removes consumer routes and recomputes packet-gate state.
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
//!   |-- refresh route-control packet gates
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

pub(in crate::engine::media_transport::rtc::worker) use responses::{
    apply_route_control_request, respond_set_consumer_packet_gates,
};
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use routes::worker_set_consumer_packet_gates_for_benchmark;
pub(super) use routes::{
    ConsumerRouteRegistration, consumer_payload_type, ensure_existing_route_source,
    ensure_route_source_registered, owned_local_producer_mid, packet_gate_rid,
    register_consumer_route, remove_consumer_route,
};
pub(in crate::engine::media_transport::rtc::worker) use routes::{
    refresh_source_packet_gate, remove_source_route,
};
#[cfg(test)]
pub(in crate::engine::media_transport::rtc::worker::handlers::media) use selected_rid::observe_source_rid_readiness;
pub(in crate::engine::media_transport::rtc) use selected_rid::{
    apply_source_rid_readiness, drain_due_rid_keyframe_refreshes,
};
