#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

#[path = "api/mod.rs"]
pub(super) mod api;
#[cfg(test)]
#[path = "fixtures.rs"]
mod fixtures;
#[cfg(test)]
#[path = "manager_tests.rs"]
mod manager_tests;
#[cfg(test)]
#[path = "membership_tests.rs"]
mod membership_tests;
#[cfg(test)]
#[path = "outbound_tests.rs"]
mod outbound_tests;
#[cfg(test)]
#[path = "producer_tests.rs"]
mod producer_tests;
#[cfg(test)]
#[path = "tracing.rs"]
pub(crate) mod tracing;
