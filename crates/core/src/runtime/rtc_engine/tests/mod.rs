#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

mod bootstrap_state_tests;
mod fixtures;
mod forwarding_planner_tests;
mod lifecycle_tests;
mod media_flow_tests;
mod negotiation_tests;
mod parsing;
mod relay_registry_tests;
mod route_control_state_tests;
mod shared_payload_tests;
