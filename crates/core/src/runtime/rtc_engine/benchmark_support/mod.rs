//! feature-gated packet-loop benchmark fixtures
//!
//! the types in this module build deterministic scenarios for Callgrind
//! benchmarks without creating a second packet-loop model
//! each measured method calls the same RTC-engine helpers used by the worker
//! packet loop

mod fanout;
mod relay;

pub use fanout::{FanoutBenchTopology, ROUTE_PLANNING_TURNS};
pub use relay::{RELAY_MAILBOX_ATTEMPTS, RelayPressureBenchFixture};
