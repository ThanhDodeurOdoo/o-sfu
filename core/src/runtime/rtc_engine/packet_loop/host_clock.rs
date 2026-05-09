//! Host-side clock translation for the packet loop.
//!
//! The async worker owns real `Instant` reads. Synchronous packet-loop helpers
//! receive the translated [`PacketLoopTime`] value instead.

use std::time::Instant;

use super::time::PacketLoopTime;

#[derive(Debug, Clone, Copy)]
pub(super) struct PacketLoopClock {
    origin: Instant,
}

impl PacketLoopClock {
    pub(super) fn new(origin: Instant) -> Self {
        Self { origin }
    }

    pub(super) fn now(self) -> PacketLoopTime {
        self.to_packet_time(Instant::now())
    }

    pub(super) fn to_packet_time(self, instant: Instant) -> PacketLoopTime {
        instant.checked_duration_since(self.origin).map_or(
            PacketLoopTime::ZERO,
            PacketLoopTime::from_duration_saturating,
        )
    }
}
