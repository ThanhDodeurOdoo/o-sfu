mod load;

#[cfg(test)]
pub(in crate::runtime::room) use load::LoadPressureReason;

use crate::{Bitrate, RoomShardingPolicy, RoomSpilloverMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::runtime::room) struct TopologyPressureSnapshot {
    pub receiver_count: usize,
    pub active_consumer_count: usize,
    pub pending_consumer_count: usize,
    pub max_source_fanout: usize,
    pub egress_bitrate: Bitrate,
    pub packet_loop_lag_ms: u64,
    pub command_backlog_depth: usize,
    pub relay_mailbox_depth: usize,
    pub worker_pressure_score: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HomePlacementInput {
    pub connection_seed: u64,
    pub reserved_router_count: usize,
    pub pressure: TopologyPressureSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CleanupInput {
    pub reserved_router_count: usize,
    pub occupied_router_count: usize,
    pub pressure: TopologyPressureSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HomePlacementDecision {
    router_index: usize,
}

impl HomePlacementDecision {
    #[must_use]
    pub const fn new(router_index: usize) -> Self {
        Self { router_index }
    }

    #[must_use]
    pub const fn router_index(self) -> usize {
        self.router_index
    }
}

#[derive(Debug, Clone)]
pub(super) enum PlacementPolicy {
    Strict,
    Bounded,
    LoadTriggered(load::LoadTriggeredPlacementPolicy),
}

impl PlacementPolicy {
    #[must_use]
    pub(super) fn new(policy: RoomShardingPolicy) -> Self {
        match policy.spillover() {
            RoomSpilloverMode::StrictSingleRouter => Self::Strict,
            RoomSpilloverMode::BoundedLocalSpillover => Self::Bounded,
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                Self::LoadTriggered(load::LoadTriggeredPlacementPolicy::new(policy))
            }
        }
    }

    pub(super) fn choose_home_router(
        &mut self,
        input: HomePlacementInput,
    ) -> HomePlacementDecision {
        match self {
            Self::Strict => HomePlacementDecision::new(0),
            Self::Bounded => {
                let router_count = input.reserved_router_count.max(1);
                let router_index =
                    usize::try_from(input.connection_seed).unwrap_or(0) % router_count;
                HomePlacementDecision::new(router_index)
            }
            Self::LoadTriggered(policy) => policy.choose_home_router(input),
        }
    }

    pub(super) fn active_router_count_to_keep_after_cleanup(
        &mut self,
        input: CleanupInput,
    ) -> usize {
        match self {
            Self::Strict => 1,
            Self::Bounded => input.occupied_router_count.max(1),
            Self::LoadTriggered(policy) => policy.active_router_count_to_keep_after_cleanup(input),
        }
    }

    #[cfg(test)]
    pub(super) fn active_router_count_for_test(&self) -> usize {
        match self {
            Self::Strict | Self::Bounded => 1,
            Self::LoadTriggered(policy) => policy.active_router_count_for_test(),
        }
    }

    #[cfg(test)]
    pub(super) fn last_load_pressure_reason_for_test(&self) -> Option<LoadPressureReason> {
        match self {
            Self::Strict | Self::Bounded => None,
            Self::LoadTriggered(policy) => policy.last_pressure_reason_for_test(),
        }
    }
}
