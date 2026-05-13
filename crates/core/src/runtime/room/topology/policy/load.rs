use super::{CleanupInput, HomePlacementDecision, HomePlacementInput, TopologyPressureSnapshot};
use crate::LocalSpilloverPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::room) enum LoadPressureReason {
    ReceiverCount,
    ActiveConsumerCount,
    SourceFanout,
    EgressBitrate,
    PacketLoopLag,
    CommandBacklog,
    RelayMailboxDepth,
    WorkerPressure,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room::topology) struct LoadTriggeredPlacementPolicy {
    policy: LocalSpilloverPolicy,
    active_router_count: usize,
    pressure_window: usize,
    idle_window: usize,
    last_pressure_reason: Option<LoadPressureReason>,
}

impl LoadTriggeredPlacementPolicy {
    #[must_use]
    pub(super) const fn new(policy: LocalSpilloverPolicy) -> Self {
        Self {
            policy,
            active_router_count: 1,
            pressure_window: 0,
            idle_window: 0,
            last_pressure_reason: None,
        }
    }

    pub(super) fn choose_home_router(
        &mut self,
        input: HomePlacementInput,
    ) -> HomePlacementDecision {
        let reserved_router_count = input.reserved_router_count.max(1);
        let pressure_reason =
            self.observe_pressure(input.pressure, self.active_router_count.max(1));
        if pressure_reason.is_some()
            && self.pressure_window >= self.policy.activation_window()
            && self.active_router_count < reserved_router_count
        {
            self.active_router_count = self
                .active_router_count
                .saturating_add(1)
                .min(reserved_router_count);
            self.pressure_window = 0;
        }
        let router_index =
            usize::try_from(input.connection_seed).unwrap_or(0) % self.active_router_count.max(1);
        HomePlacementDecision::new(router_index)
    }

    pub(super) fn active_router_count_to_keep_after_cleanup(
        &mut self,
        input: CleanupInput,
    ) -> usize {
        let reserved_router_count = input.reserved_router_count.max(1);
        let occupied_router_count = input.occupied_router_count.max(1);
        if self
            .observe_pressure(input.pressure, occupied_router_count)
            .is_some()
        {
            return self.active_router_count.min(reserved_router_count).max(1);
        }
        self.idle_window = self.idle_window.saturating_add(1);
        if self.idle_window >= self.policy.cooldown_window() {
            self.active_router_count = self
                .active_router_count
                .min(occupied_router_count)
                .min(reserved_router_count)
                .max(1);
            self.idle_window = 0;
        }
        self.active_router_count.min(reserved_router_count).max(1)
    }

    fn observe_pressure(
        &mut self,
        pressure: TopologyPressureSnapshot,
        active_router_count: usize,
    ) -> Option<LoadPressureReason> {
        let reason = pressure_reason(self.policy, pressure, active_router_count);
        if let Some(reason) = reason {
            self.pressure_window = self.pressure_window.saturating_add(1);
            self.idle_window = 0;
            self.last_pressure_reason = Some(reason);
        } else {
            self.pressure_window = 0;
        }
        reason
    }

    #[cfg(test)]
    pub(super) const fn active_router_count_for_test(&self) -> usize {
        self.active_router_count
    }

    #[cfg(test)]
    pub(super) const fn last_pressure_reason_for_test(&self) -> Option<LoadPressureReason> {
        self.last_pressure_reason
    }
}

fn pressure_reason(
    policy: LocalSpilloverPolicy,
    pressure: TopologyPressureSnapshot,
    active_router_count: usize,
) -> Option<LoadPressureReason> {
    if pressure.receiver_count >= policy.min_receiver_count() {
        return Some(LoadPressureReason::ReceiverCount);
    }
    let consumer_count = pressure
        .active_consumer_count
        .saturating_add(pressure.pending_consumer_count);
    if consumers_per_router(consumer_count, active_router_count)
        > policy.max_active_consumers_per_router()
    {
        return Some(LoadPressureReason::ActiveConsumerCount);
    }
    if pressure.max_source_fanout > policy.max_fanout_per_source() {
        return Some(LoadPressureReason::SourceFanout);
    }
    if policy.egress_bitrate_threshold_bps() > 0
        && pressure.egress_bitrate_bps >= policy.egress_bitrate_threshold_bps()
    {
        return Some(LoadPressureReason::EgressBitrate);
    }
    if policy.packet_loop_lag_threshold_ms() > 0
        && pressure.packet_loop_lag_ms >= policy.packet_loop_lag_threshold_ms()
    {
        return Some(LoadPressureReason::PacketLoopLag);
    }
    if policy.command_backlog_threshold() > 0
        && pressure.command_backlog_depth >= policy.command_backlog_threshold()
    {
        return Some(LoadPressureReason::CommandBacklog);
    }
    if policy.relay_mailbox_depth_threshold() > 0
        && pressure.relay_mailbox_depth >= policy.relay_mailbox_depth_threshold()
    {
        return Some(LoadPressureReason::RelayMailboxDepth);
    }
    (policy.worker_pressure_threshold() > 0
        && pressure.worker_pressure_score >= policy.worker_pressure_threshold())
    .then_some(LoadPressureReason::WorkerPressure)
}

fn consumers_per_router(consumer_count: usize, active_router_count: usize) -> usize {
    if consumer_count == 0 {
        return 0;
    }
    let router_count = active_router_count.max(1);
    consumer_count.div_ceil(router_count)
}
