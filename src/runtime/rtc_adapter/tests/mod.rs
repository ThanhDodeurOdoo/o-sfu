#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

mod bootstrap_state_tests;
mod fixtures;
mod lifecycle_tests;
mod media_flow_tests;
mod negotiation_tests;
mod transport_connect_tests;
mod validation_tests;
