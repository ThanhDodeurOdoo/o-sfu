//! feature-gated packet-loop benchmark fixtures
//!
//! the types in this module build deterministic scenarios for Callgrind
//! benchmarks without creating a second packet-loop model
//! each measured method calls the same RTC-engine helpers used by the worker
//! packet loop

mod fanout;
mod ingress;
mod relay;
mod sinks;
mod video;

pub use fanout::{FanoutBenchTopology, ROUTE_PLANNING_TURNS};
pub use ingress::{INGRESS_DEMUX_ATTEMPTS, IngressRoutingBenchFixture};
pub use relay::{RELAY_MAILBOX_ATTEMPTS, RelayPressureBenchFixture};
pub use sinks::{PACKET_SINK_FANOUT_TURNS, PacketSinkFanoutBenchFixture};
pub use video::{
    KEYFRAME_COALESCING_REQUESTS, KeyframeCoalescingBenchFixture, RidReadinessBenchFixture,
    SELECTED_RID_DESTINATIONS,
};
