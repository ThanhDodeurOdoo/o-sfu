//! feature-gated packet-loop benchmark fixtures
//!
//! the types in this module build deterministic scenarios for Callgrind
//! benchmarks without creating a second packet-loop model
//! each measured method calls the same RTC-engine helpers used by the worker
//! packet loop

mod active_speaker;
mod consumer_gates;
mod fanout;
mod ingress;
mod local_rewrite;
mod observation;
mod relay;
mod scheduler;
mod sinks;
mod video;
#[cfg(feature = "internal-benchmarks")]
mod worker;

pub use active_speaker::ActiveSpeakerBenchFixture;
pub use consumer_gates::ConsumerGateBatchBenchFixture;
pub use fanout::{FanoutBenchTopology, ROUTE_PLANNING_TURNS};
pub use ingress::{INGRESS_DEMUX_ATTEMPTS, IngressRoutingBenchFixture};
pub use local_rewrite::LocalRewriteBenchFixture;
pub use observation::IncomingObservationBenchFixture;
pub use relay::{RELAY_MAILBOX_ATTEMPTS, RelayPressureBenchFixture};
pub use scheduler::SchedulerBenchFixture;
pub use sinks::{PACKET_SINK_FANOUT_TURNS, PacketSinkFanoutBenchFixture};
pub use video::{
    KEYFRAME_COALESCING_REQUESTS, KeyframeCoalescingBenchFixture, RidReadinessBenchFixture,
    SELECTED_RID_DESTINATIONS,
};
#[cfg(feature = "internal-benchmarks")]
pub use worker::{
    WORKER_COMMAND_ROUNDTRIPS, WORKER_PACKET_COMMAND_MIX_PACKETS, WorkerLoopBenchFixture,
    WorkerPacketCommandMixBenchFixture,
};

pub use super::routing_miss::packet_fingerprint_for_benchmark as routing_miss_packet_fingerprint;
