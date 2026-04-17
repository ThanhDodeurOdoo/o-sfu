//! Packet-loop runtime driver and hot-path helpers for the RTC adapter.

mod buffers;
mod event_observation;
mod forward_flush;
mod ingress_routing;
mod keyframe_requests;
mod loop_driver;
mod session_drain;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use event_observation::{transport_health_from_event, transport_ice_state};
pub(crate) use loop_driver::{PacketLoopConfig, run_packet_loop};
