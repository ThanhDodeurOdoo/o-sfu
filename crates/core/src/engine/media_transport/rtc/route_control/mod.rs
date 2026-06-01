//! Transport-native route control for source packet forwarding.
//!
//! Route control sits between worker-local media routes and the packet loop. It
//! tracks packet-level active-speaker state and applies already-projected packet
//! gates. It does not know room layout, receiver budgets, or Odoo-facing source
//! identity.

mod active_speaker;
mod packet_gate;
mod state;

pub(super) use packet_gate::{
    PacketLayerGate, PacketLayerMetadata, PacketOperatingPointGate, PacketRouteDecision,
    aggregate_packet_gates,
};
pub(super) use state::RouteControlState;
