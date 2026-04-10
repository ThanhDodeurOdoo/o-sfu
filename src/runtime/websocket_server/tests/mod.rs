#![allow(
    clippy::panic,
    reason = "test assertions use panic for clear failure messages"
)]

mod auth_tests;
mod bootstrap_tests;
mod channel_event_tests;
mod fixtures;
mod media_bootstrap_tests;
mod protocol_resilience_tests;
mod session_lifecycle_tests;
mod transport_request_tests;
