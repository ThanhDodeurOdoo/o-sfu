#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

pub(super) mod api;
#[cfg(test)]
mod consumer_tests;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod manager_tests;
#[cfg(test)]
mod membership_tests;
#[cfg(test)]
mod producer_tests;
#[cfg(test)]
mod recording_tests;
#[cfg(test)]
mod router_state_tests;
#[cfg(test)]
mod topology_tests;
