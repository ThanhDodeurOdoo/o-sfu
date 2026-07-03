use super::super::media_graph::ConsumerRouteTransportRef;
use crate::engine::{
    ConnectionId, UserId,
    media_transport::{
        ConsumerActivity, ConsumerRouteControl, SourcePacketGate, TransportConsumerRoute,
    },
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId,
        ReceiverVideoBudgetDiagnostics, SourceSelector,
    },
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct BudgetSolverOutcomes {
    bits: u8,
}

impl BudgetSolverOutcomes {
    const DEGRADED: u8 = 1 << 0;
    const PAUSED: u8 = 1 << 1;
    const RESUMED: u8 = 1 << 2;
    const PROTECTED_OVER_BUDGET: u8 = 1 << 3;

    pub(super) const fn degraded() -> Self {
        Self {
            bits: Self::DEGRADED,
        }
    }

    pub(super) const fn paused() -> Self {
        Self { bits: Self::PAUSED }
    }

    pub(super) const fn resumed() -> Self {
        Self {
            bits: Self::RESUMED,
        }
    }

    pub(super) const fn with_protected_over_budget(mut self) -> Self {
        self.bits |= Self::PROTECTED_OVER_BUDGET;
        self
    }

    pub(super) const fn is_degraded(self) -> bool {
        self.bits & Self::DEGRADED != 0
    }

    pub(super) const fn is_paused(self) -> bool {
        self.bits & Self::PAUSED != 0
    }

    pub(super) const fn is_resumed(self) -> bool {
        self.bits & Self::RESUMED != 0
    }

    pub(super) const fn is_protected_over_budget(self) -> bool {
        self.bits & Self::PROTECTED_OVER_BUDGET != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::room) struct ConsumerPacketSelectionUpdate {
    pub(super) transport_ref: ConsumerRouteTransportRef,
    pub(super) source_id: PublishedSourceId,
    pub(super) selector: SourceSelector,
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    pub(super) budget: ReceiverVideoBudgetDiagnostics,
    pub(super) outcomes: BudgetSolverOutcomes,
    pub(super) pressure_observations: u8,
    pub(super) upgrade_observations: u8,
    pub(super) packet_gate: Option<SourcePacketGate>,
    pub(super) route_activity_changed: bool,
    pub(super) request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub(super) fn route_activity(
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

    pub(in crate::engine::room) fn route_control(
        &self,
        route: TransportConsumerRoute,
    ) -> ConsumerRouteControl {
        let mut control = ConsumerRouteControl::new(route).request_keyframe(self.request_keyframe);
        if self.route_activity_changed {
            control = control.activity(ConsumerActivity::from_active(self.route_active()));
        }
        if let Some(packet_gate) = &self.packet_gate {
            control = control.packet_gate(packet_gate.clone());
        }
        control
    }

    pub(in crate::engine::room) const fn route_active(&self) -> bool {
        self.policy_pause_reason.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FeaturedUserUpdate {
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) featured: Option<bool>,
}

impl FeaturedUserUpdate {
    #[must_use]
    pub(super) fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        featured: Option<bool>,
    ) -> Self {
        Self {
            user_id,
            connection_id,
            featured,
        }
    }
}
