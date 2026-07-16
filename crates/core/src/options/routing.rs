use crate::Bitrate;

/// same-room placement policy for local router spillover
///
/// server startup chooses this topology model before any room exists
/// it describes how many process-local router placements a room may use and
/// which spillover mode interprets that limit
///
/// [`RoomWorkerPolicy`] belongs to room placement, not to the RTP packet
/// loop
/// the room manager reads it at join time to decide whether a user connection
/// can be placed on a spillover router
///
/// # Invariants
///
/// `max_local_routers()` never returns zero
/// the runtime config layer still
/// validates operator-facing worker limits because those depend on process
/// topology
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomWorkerPolicy {
    max_local_routers: usize,
    spillover: RoomSpilloverMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSpilloverPolicy {
    parts: LocalSpilloverPolicyParts,
}

/// validated construction input for [`LocalSpilloverPolicy`]
///
/// count and window fields must be greater than zero
/// transport-observed pressure thresholds may be zero when the caller disables
/// that signal
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSpilloverPolicyParts {
    pub min_receiver_count: usize,
    pub max_active_consumers_per_router: usize,
    pub max_fanout_per_source: usize,
    pub egress_bitrate_threshold: Bitrate,
    pub packet_loop_lag_threshold_ms: u64,
    pub command_backlog_threshold: usize,
    pub relay_mailbox_depth_threshold: usize,
    pub worker_pressure_threshold: u8,
    pub activation_window: usize,
}

/// invalid load-triggered local spillover policy input
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LocalSpilloverPolicyError {
    #[error("minimum receiver count must be greater than zero")]
    MinReceiverCountZero,
    #[error("maximum active consumers per router must be greater than zero")]
    MaxActiveConsumersPerRouterZero,
    #[error("maximum fan-out per source must be greater than zero")]
    MaxFanoutPerSourceZero,
    #[error("worker pressure threshold must be less than or equal to 100")]
    WorkerPressureThresholdTooHigh,
    #[error("activation window must be greater than zero")]
    ActivationWindowZero,
}

/// how a room interprets its local router placement cap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomSpilloverMode {
    /// keep all users, producers and consumers on the room's primary router
    ///
    /// this preserves the historical topology shape even when the process has
    /// multiple RTC media workers
    StrictSingleRouter,
    /// allow the room runtime to add local placements up to the cap
    ///
    /// placement is bounded by `max_local_routers`
    /// it is not an adaptive load-triggered policy
    BoundedLocalSpillover,
    /// keep capable room workers in use and add local capacity only after
    /// measured pressure crosses the configured policy thresholds
    LoadTriggeredLocalSpillover(LocalSpilloverPolicy),
}

impl RoomWorkerPolicy {
    /// use this unless the runtime has explicitly opted into same-room
    /// spillover
    /// it keeps the room topology identical to the historical single-router
    /// model with any positive media-worker count
    #[must_use]
    pub const fn strict_single_router() -> Self {
        Self {
            max_local_routers: 1,
            spillover: RoomSpilloverMode::StrictSingleRouter,
        }
    }

    /// `max_local_routers` is an upper bound for one room
    /// the runtime config
    /// layer must keep it less than or equal to the RTC media worker count so
    /// every placed router has a worker placement
    /// [`Self::max_local_routers`] normalizes zero to one
    ///
    /// this constructor only records the policy consumed by room creation and
    /// topology state
    #[must_use]
    pub const fn bounded_local_spillover(max_local_routers: usize) -> Self {
        Self {
            max_local_routers,
            spillover: RoomSpilloverMode::BoundedLocalSpillover,
        }
    }

    /// `max_local_routers` is still only an upper bound
    /// rooms start on their
    /// primary placement and attach additional local placements when the
    /// provided load policy reports sustained pressure
    #[must_use]
    pub const fn load_triggered_local_spillover(
        max_local_routers: usize,
        policy: LocalSpilloverPolicy,
    ) -> Self {
        Self {
            max_local_routers,
            spillover: RoomSpilloverMode::LoadTriggeredLocalSpillover(policy),
        }
    }

    /// return the non-zero local router cap for one room
    ///
    /// the cap is the number of room-local router placements the runtime may
    /// reserve, not a count of currently attached routers
    /// spillover routers can stay detached until a user is placed on them
    #[must_use]
    pub const fn max_local_routers(self) -> usize {
        if self.max_local_routers == 0 {
            1
        } else {
            self.max_local_routers
        }
    }

    /// return the spillover mode that interprets this policy
    ///
    /// callers should branch on this value instead of treating
    /// `max_local_routers() == 1` as the only strict-mode signal
    #[must_use]
    pub const fn spillover(self) -> RoomSpilloverMode {
        self.spillover
    }

    /// return how many known local placements may receive home sessions
    ///
    /// strict mode always uses the primary placement
    /// bounded spillover uses the configured cap limited by known placements
    #[must_use]
    pub fn allowed_local_router_count(self, reserved_local_routers: usize) -> usize {
        match self.spillover {
            RoomSpilloverMode::BoundedLocalSpillover => {
                self.max_local_routers().min(reserved_local_routers).max(1)
            }
            RoomSpilloverMode::StrictSingleRouter
            | RoomSpilloverMode::LoadTriggeredLocalSpillover(_) => 1,
        }
    }
}

impl Default for RoomWorkerPolicy {
    fn default() -> Self {
        Self::strict_single_router()
    }
}

impl LocalSpilloverPolicy {
    pub const DEFAULT_MIN_RECEIVER_COUNT: usize = 16;
    pub const DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER: usize = 64;
    pub const DEFAULT_MAX_FANOUT_PER_SOURCE: usize = 48;
    pub const DEFAULT_EGRESS_BITRATE_THRESHOLD: Bitrate = Bitrate::from_mbps(750);
    pub const DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS: u64 = 20;
    pub const DEFAULT_COMMAND_BACKLOG_THRESHOLD: usize = 128;
    pub const DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD: usize = 128;
    pub const DEFAULT_WORKER_PRESSURE_THRESHOLD: u8 = 80;
    pub const DEFAULT_ACTIVATION_WINDOW: usize = 2;

    /// build a load-triggered spillover policy after validating its invariants
    ///
    /// use this for operator or caller-provided values
    /// zero is accepted only for optional transport-pressure thresholds where it
    /// disables that signal
    ///
    /// # Errors
    ///
    /// returns [`LocalSpilloverPolicyError`] when a required count or window is
    /// zero or when the worker pressure threshold is above the 0 to 100 score
    /// range
    pub fn try_new(parts: LocalSpilloverPolicyParts) -> Result<Self, LocalSpilloverPolicyError> {
        if parts.min_receiver_count == 0 {
            return Err(LocalSpilloverPolicyError::MinReceiverCountZero);
        }
        if parts.max_active_consumers_per_router == 0 {
            return Err(LocalSpilloverPolicyError::MaxActiveConsumersPerRouterZero);
        }
        if parts.max_fanout_per_source == 0 {
            return Err(LocalSpilloverPolicyError::MaxFanoutPerSourceZero);
        }
        if parts.worker_pressure_threshold > 100 {
            return Err(LocalSpilloverPolicyError::WorkerPressureThresholdTooHigh);
        }
        if parts.activation_window == 0 {
            return Err(LocalSpilloverPolicyError::ActivationWindowZero);
        }
        Ok(Self { parts })
    }

    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            parts: LocalSpilloverPolicyParts::conservative(),
        }
    }

    #[must_use]
    pub const fn parts(self) -> LocalSpilloverPolicyParts {
        self.parts
    }
}

impl LocalSpilloverPolicyParts {
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            min_receiver_count: LocalSpilloverPolicy::DEFAULT_MIN_RECEIVER_COUNT,
            max_active_consumers_per_router:
                LocalSpilloverPolicy::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER,
            max_fanout_per_source: LocalSpilloverPolicy::DEFAULT_MAX_FANOUT_PER_SOURCE,
            egress_bitrate_threshold: LocalSpilloverPolicy::DEFAULT_EGRESS_BITRATE_THRESHOLD,
            packet_loop_lag_threshold_ms:
                LocalSpilloverPolicy::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS,
            command_backlog_threshold: LocalSpilloverPolicy::DEFAULT_COMMAND_BACKLOG_THRESHOLD,
            relay_mailbox_depth_threshold:
                LocalSpilloverPolicy::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD,
            worker_pressure_threshold: LocalSpilloverPolicy::DEFAULT_WORKER_PRESSURE_THRESHOLD,
            activation_window: LocalSpilloverPolicy::DEFAULT_ACTIVATION_WINDOW,
        }
    }
}

impl Default for LocalSpilloverPolicy {
    fn default() -> Self {
        Self::conservative()
    }
}

impl Default for LocalSpilloverPolicyParts {
    fn default() -> Self {
        Self::conservative()
    }
}

#[cfg(test)]
#[path = "TESTS/routing.rs"]
mod tests;
