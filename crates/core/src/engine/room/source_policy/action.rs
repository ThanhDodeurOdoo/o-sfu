use super::super::media_graph::{ConsumerRouteTarget, ConsumerRouteTransportRef};
use crate::engine::{
    ConnectionId, UserId,
    media_transport::{ReceiverBweTargetUpdate, SourcePacketGate},
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId,
        ReceiverVideoBudgetDiagnostics, SourceSelector,
    },
};

#[derive(Debug)]
pub(super) struct ReceiverVideoPolicyPlan {
    pub(super) state_packet_updates: Vec<ConsumerPacketSelectionUpdate>,
    pub(super) transport_packet_updates: Vec<TransportPacketSelectionUpdate>,
    pub(super) receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
}

#[derive(Debug)]
pub(in crate::engine::room) struct TransportPacketSelectionUpdate {
    pub(in crate::engine::room) update: ConsumerPacketSelectionUpdate,
    pub(in crate::engine::room) target: ConsumerRouteTarget,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetSolverOutcomes {
    bits: u8,
}

impl BudgetSolverOutcomes {
    const DEGRADED: u8 = 1 << 0;
    const PAUSED: u8 = 1 << 1;
    const RESUMED: u8 = 1 << 2;
    const PROTECTED_OVER_BUDGET: u8 = 1 << 3;

    pub const fn degraded() -> Self {
        Self {
            bits: Self::DEGRADED,
        }
    }

    pub const fn paused() -> Self {
        Self { bits: Self::PAUSED }
    }

    pub const fn resumed() -> Self {
        Self {
            bits: Self::RESUMED,
        }
    }

    pub const fn with_protected_over_budget(mut self) -> Self {
        self.bits |= Self::PROTECTED_OVER_BUDGET;
        self
    }

    pub const fn is_degraded(self) -> bool {
        self.bits & Self::DEGRADED != 0
    }

    pub const fn is_paused(self) -> bool {
        self.bits & Self::PAUSED != 0
    }

    pub const fn is_resumed(self) -> bool {
        self.bits & Self::RESUMED != 0
    }

    pub const fn is_protected_over_budget(self) -> bool {
        self.bits & Self::PROTECTED_OVER_BUDGET != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPacketSelectionUpdate {
    pub transport_ref: ConsumerRouteTransportRef,
    pub source_id: PublishedSourceId,
    pub selector: SourceSelector,
    pub policy_pause_reason: Option<PolicyPauseReason>,
    pub budget: ReceiverVideoBudgetDiagnostics,
    pub outcomes: BudgetSolverOutcomes,
    pub pressure_observations: u8,
    pub upgrade_observations: u8,
    pub packet_gate: Option<SourcePacketGate>,
    pub route_activity_changed: bool,
    pub request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub fn route_activity(
        transport_ref: ConsumerRouteTransportRef,
        source_id: PublishedSourceId,
        current_selection: ConsumerSourceSelection,
        policy_pause_reason: Option<PolicyPauseReason>,
    ) -> Option<Self> {
        (policy_pause_reason != current_selection.policy_pause_reason()).then(|| Self {
            transport_ref,
            source_id,
            selector: current_selection.selector(),
            policy_pause_reason,
            budget: current_selection.budget(),
            outcomes: BudgetSolverOutcomes::default(),
            pressure_observations: current_selection.pressure_observations(),
            upgrade_observations: current_selection.upgrade_observations(),
            packet_gate: None,
            route_activity_changed: true,
            request_keyframe: false,
        })
    }

    pub(super) const fn requires_media_transport_effect(&self) -> bool {
        self.packet_gate.is_some() || self.route_activity_changed || self.request_keyframe
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeaturedUserUpdate {
    pub user_id: UserId,
    pub connection_id: ConnectionId,
    pub featured: Option<bool>,
}

impl FeaturedUserUpdate {
    #[must_use]
    pub fn new(user_id: UserId, connection_id: ConnectionId, featured: Option<bool>) -> Self {
        Self {
            user_id,
            connection_id,
            featured,
        }
    }
}
