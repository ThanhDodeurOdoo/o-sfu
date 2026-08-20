//! Recording packet-sink extension point.
#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;

pub use crate::engine::packet_sink_registry::PacketSink as MediaPacketSink;
