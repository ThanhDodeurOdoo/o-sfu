//! Transport-native route control for source packet forwarding.
//!
//! Route control sits between worker-local media routes and the packet loop. It
//! tracks packet-level active-speaker state and applies already-projected packet
//! gates. It does not know room layout, receiver budgets, or Odoo-facing source
//! identity.

mod active_speaker;
mod packet_gate;

pub use active_speaker::SourceAudioPolicyState;
pub use packet_gate::PacketLayerGate;
pub(super) use packet_gate::{aggregate_packet_gates, intersect_packet_gates};
