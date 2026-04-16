#![allow(
    clippy::panic,
    reason = "test assertions use panic for clear failure messages"
)]

mod auth_tests;
mod fixtures;
mod protocol_core_harness_tests;
mod protocol_negotiation_tests;
mod protocol_resilience_tests;
mod session_lifecycle_tests;
