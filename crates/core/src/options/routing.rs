use crate::Bitrate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingOptions {
    pub media_worker_count: usize,
    /// Room-local routing policy used by the room runtime.
    ///
    /// This is a cold-path control-plane setting. It decides how many local
    /// router placements a room may use. It does not participate in packet
    /// forwarding and it does not change the transport worker count after
    /// startup.
    pub room_worker_policy: RoomWorkerPolicy,
}

/// Same-room placement policy for local router spillover.
///
/// The policy is part of the public core configuration surface because server
/// startup has to choose the room topology model before any room exists. It
/// describes how many process-local router placements a room may use and which
/// spillover mode should interpret that limit.
///
/// [`RoomWorkerPolicy`] belongs to room placement, not to the RTP packet
/// loop. The room manager reads it at join time to decide whether a user
/// connection can be placed on a spillover router.
///
/// # Invariants
///
/// `max_local_routers()` never returns zero. The runtime config layer still
/// validates operator-facing worker limits because those depend on process
/// topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomWorkerPolicy {
    max_local_routers: usize,
    spillover: RoomSpilloverMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSpilloverPolicy {
    parts: LocalSpilloverPolicyParts,
}

/// Validated construction input for [`LocalSpilloverPolicy`].
///
/// Count and window fields must be greater than zero. Transport-observed
/// pressure thresholds may be zero when the caller disables that
/// signal.
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
    pub cooldown_window: usize,
}

/// Invalid load-triggered local spillover policy input.
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
    #[error("cooldown window must be greater than zero")]
    CooldownWindowZero,
}

/// How a room interprets its local router placement cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomSpilloverMode {
    /// Keep all users, producers and consumers on the room's primary router.
    ///
    /// This is the default deployment mode. It preserves the historical
    /// topology shape even when the process has multiple RTC media workers.
    StrictSingleRouter,
    /// Allow the room runtime to add local placements up to the cap.
    ///
    /// Placement is bounded by `max_local_routers`. It is not an adaptive
    /// load-triggered policy.
    BoundedLocalSpillover,
    /// Keep capable room workers in use and add local capacity only after
    /// measured pressure crosses the configured policy thresholds.
    LoadTriggeredLocalSpillover(LocalSpilloverPolicy),
}

impl RoutingOptions {
    #[must_use]
    pub const fn new(media_worker_count: usize) -> Self {
        Self {
            media_worker_count,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
        }
    }
}

impl RoomWorkerPolicy {
    /// Build the default policy that keeps every room on one local router.
    ///
    /// Use this unless the runtime has explicitly opted into same-room
    /// spillover. It keeps the room topology identical to the historical
    /// single-router model and is safe with any positive media-worker count.
    #[must_use]
    pub const fn strict_single_router() -> Self {
        Self {
            max_local_routers: 1,
            spillover: RoomSpilloverMode::StrictSingleRouter,
        }
    }

    /// Build a policy that may place one room across several local routers.
    ///
    /// `max_local_routers` is an upper bound for one room. The runtime config
    /// layer must keep it less than or equal to the RTC media worker count so
    /// every placed router has a worker placement. If a caller passes zero,
    /// [`Self::max_local_routers`] normalizes it to one.
    ///
    /// This constructor does not allocate routers. It only records the policy
    /// consumed by room creation and topology state.
    #[must_use]
    pub const fn bounded_local_spillover(max_local_routers: usize) -> Self {
        Self {
            max_local_routers,
            spillover: RoomSpilloverMode::BoundedLocalSpillover,
        }
    }

    /// Build the production same-room spillover policy.
    ///
    /// `max_local_routers` is still only an upper bound. Rooms start on their
    /// primary placement and attach additional local placements when the
    /// provided load policy reports sustained pressure.
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

    /// Return the non-zero local router cap for one room.
    ///
    /// The cap is the number of room-local router placements the runtime may
    /// reserve, not a count of currently attached routers. Spillover routers
    /// can stay detached until a user is placed on them.
    #[must_use]
    pub const fn max_local_routers(self) -> usize {
        if self.max_local_routers == 0 {
            1
        } else {
            self.max_local_routers
        }
    }

    /// Return the spillover mode that interprets this policy.
    ///
    /// Callers should branch on this value instead of treating
    /// `max_local_routers() == 1` as the only strict-mode signal. That keeps
    /// the policy open to other modes that may also use one router at a time.
    #[must_use]
    pub const fn spillover(self) -> RoomSpilloverMode {
        self.spillover
    }

    /// Return how many known local placements may receive home sessions.
    ///
    /// Strict mode always uses the primary placement. Bounded spillover uses the
    /// configured cap, limited by how many placements currently exist.
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
    pub const DEFAULT_COOLDOWN_WINDOW: usize = 4;

    /// Build a load-triggered spillover policy after validating its invariants.
    ///
    /// Use this for operator or caller-provided values. The only zero values
    /// accepted here are optional transport-pressure thresholds where zero means
    /// the corresponding signal is disabled.
    ///
    /// # Errors
    ///
    /// Returns [`LocalSpilloverPolicyError`] when a required count or window is
    /// zero or when the worker pressure threshold is above the 0 to 100 score
    /// range.
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
        if parts.cooldown_window == 0 {
            return Err(LocalSpilloverPolicyError::CooldownWindowZero);
        }
        Ok(Self { parts })
    }

    /// Build the default conservative threshold set.
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
    /// Build the default conservative threshold input set.
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
            cooldown_window: LocalSpilloverPolicy::DEFAULT_COOLDOWN_WINDOW,
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
mod tests {
    use super::{
        Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts,
    };

    #[test]
    fn local_spillover_policy_rejects_invalid_required_fields() {
        let valid_parts = LocalSpilloverPolicyParts::conservative();
        let cases = [
            (
                LocalSpilloverPolicyParts {
                    min_receiver_count: 0,
                    ..valid_parts
                },
                LocalSpilloverPolicyError::MinReceiverCountZero,
            ),
            (
                LocalSpilloverPolicyParts {
                    max_active_consumers_per_router: 0,
                    ..valid_parts
                },
                LocalSpilloverPolicyError::MaxActiveConsumersPerRouterZero,
            ),
            (
                LocalSpilloverPolicyParts {
                    max_fanout_per_source: 0,
                    ..valid_parts
                },
                LocalSpilloverPolicyError::MaxFanoutPerSourceZero,
            ),
            (
                LocalSpilloverPolicyParts {
                    activation_window: 0,
                    ..valid_parts
                },
                LocalSpilloverPolicyError::ActivationWindowZero,
            ),
            (
                LocalSpilloverPolicyParts {
                    cooldown_window: 0,
                    ..valid_parts
                },
                LocalSpilloverPolicyError::CooldownWindowZero,
            ),
        ];

        for (parts, error) in cases {
            assert_eq!(LocalSpilloverPolicy::try_new(parts).err(), Some(error));
        }
    }

    #[test]
    fn local_spillover_policy_rejects_worker_pressure_above_score_range() {
        let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            worker_pressure_threshold: 101,
            ..LocalSpilloverPolicyParts::conservative()
        });

        assert_eq!(
            policy.err(),
            Some(LocalSpilloverPolicyError::WorkerPressureThresholdTooHigh)
        );
    }

    #[test]
    fn local_spillover_policy_accepts_disabled_transport_pressure_signals() {
        let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
            egress_bitrate_threshold: Bitrate::zero(),
            packet_loop_lag_threshold_ms: 0,
            command_backlog_threshold: 0,
            relay_mailbox_depth_threshold: 0,
            worker_pressure_threshold: 0,
            ..LocalSpilloverPolicyParts::conservative()
        });

        assert!(policy.is_ok());
        let Ok(policy) = policy else {
            return;
        };
        let parts = policy.parts();
        assert_eq!(parts.egress_bitrate_threshold, Bitrate::zero());
        assert_eq!(parts.packet_loop_lag_threshold_ms, 0);
        assert_eq!(parts.command_backlog_threshold, 0);
        assert_eq!(parts.relay_mailbox_depth_threshold, 0);
        assert_eq!(parts.worker_pressure_threshold, 0);
    }
}
