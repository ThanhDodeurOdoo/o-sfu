//! Runtime RTC transport shard for the `rtc` WebRTC backend.
//!
//! Internal modules:
//! - `api`: runtime shard facade and worker lifecycle
//! - `bitrate`: worker-local incoming bitrate counters and cold snapshot assembly
//! - `commands`: production worker mailbox contract
//! - `worker`: command dispatch and worker-local state mutations
//! - `state`: pure state types and user scheduling
//! - `media_registry`: media handle tracking and mid registry
//! - `demux`: IP hash-indexed demux and media route entries
//! - `forwarded_packet`: shard-local forwarded RTP packet model and local send edges
//! - `forwarding_destination`: named packet-forwarding destinations for local RTC, recording, intra-node relay, and inter-node relay sends
//! - `forwarding_planner`: shard-local destination planning over forwarded packets
//! - `local_forwarding`: destination-local send boundary for packet fan-out
//! - `relay_registry`: source-media-scoped relay targets for inter-worker mailboxes and future inter-node forwarding
//! - `route_control`: transport-native packet gates, active-speaker packet state, and keyframe coalescing
//! - `routing_miss`: recent-miss cache and source-aware bounded-pressure control for unknown-source recovery
//! - `sdp_simulcast`: RTC-edge SDP RID/simulcast offer and answer helpers
//! - `shared_payload`: shard-local payload ownership boundary for forwarding and recording
//! - `bootstrap`: socket binding and user RTC state initialization for the real offer/answer path
//! - `test_support`: runtime-owned RTC shard test helpers, route-inspection DTOs, and debug worker handlers that should not live on production module paths
//! - `packet_loop/`: packet-loop driver, ingress routing, keyframe control, event observation, user draining, and forward flushing
//! - `worker/media/`: media lifecycle plus one control owner for source validation, route ownership, and gate synchronization
//! - `negotiated_capabilities`: answer-side RTP capability projection for native signaling

mod api;
mod bitrate;
mod bootstrap;
mod commands;
mod demux;
mod forwarded_packet;
mod forwarding_destination;
mod forwarding_planner;
mod local_forwarding;
mod local_send_rewrite;
mod media_registry;
mod negotiated_capabilities;
mod packet_loop;
mod relay_registry;
mod route_control;
mod routing_miss;
mod sdp_simulcast;
mod shared_payload;
mod state;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
#[cfg(test)]
mod tests;
mod worker;

pub use api::{RtcTransportShard, WorkerHandleSlot};
pub use commands::RelayCleanup;
pub use demux::RemoteAddrDemux;
#[cfg(any(test, feature = "testing-transport"))]
pub use forwarded_packet::ForwardedPacket;
pub use negotiated_capabilities::client_rtp_capabilities_from_answer;
pub use relay_registry::RelayTargetRegistry;

pub use crate::transport::TransportSessionHealth;
