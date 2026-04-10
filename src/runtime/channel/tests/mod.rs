#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

mod consumer_tests;
mod fixtures;
mod manager_tests;
mod membership_tests;
mod producer_tests;
mod topology_tests;
