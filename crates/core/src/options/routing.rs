use std::num::{NonZeroU64, NonZeroUsize};

/// Same-room router cap and packet-loop health threshold.
///
/// A room's first join selects the first healthy worker in its cyclic search
/// order or the least-delayed worker when none qualify. With more than one local
/// router allowed, later joins prefer the least-delayed healthy assigned worker.
/// If none qualifies, another router may be allocated on an unused healthy
/// worker up to `max_local_routers` and the worker count. Missing delay samples
/// and values at or above `packet_loop_delay_threshold_ms` are unhealthy. If no
/// healthy placement exists, the least-delayed assigned worker is reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomWorkerPolicy {
    max_local_routers: usize,
    packet_loop_delay_threshold_ms: u64,
}

impl RoomWorkerPolicy {
    pub const DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS: u64 = 20;

    #[must_use]
    pub const fn strict_single_router() -> Self {
        Self {
            max_local_routers: 1,
            packet_loop_delay_threshold_ms: Self::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS,
        }
    }

    #[must_use]
    pub const fn new(
        max_local_routers: NonZeroUsize,
        packet_loop_delay_threshold_ms: NonZeroU64,
    ) -> Self {
        Self {
            max_local_routers: max_local_routers.get(),
            packet_loop_delay_threshold_ms: packet_loop_delay_threshold_ms.get(),
        }
    }

    #[must_use]
    pub const fn max_local_routers(self) -> usize {
        self.max_local_routers
    }

    #[must_use]
    pub const fn packet_loop_delay_threshold_ms(self) -> u64 {
        self.packet_loop_delay_threshold_ms
    }
}

impl Default for RoomWorkerPolicy {
    fn default() -> Self {
        Self::strict_single_router()
    }
}
