//! Source policy action vocabulary.
//!
//! Policy planners emit source-domain updates, not transport calls. This file
//! holds the shared effect-bound update types consumed by the post-lock source
//! policy executor.

use super::super::media::ConsumerRouteTransportRef;
use crate::runtime::{
    UserId,
    media_transport::SourcePacketGate,
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId,
        ReceiverVideoBudgetDiagnostics, SourceSelector,
    },
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime::room) struct BudgetSolverOutcomes {
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

/// One receiver-side source selection that is ready for the effect boundary.
///
/// The update carries the transport handles and connection ids observed while
/// planning. Commit revalidates them after async transport work so stale
/// replacement or cleanup events cannot write selector state onto a newer route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct ConsumerPacketSelectionUpdate {
    pub(super) route: ConsumerRouteTransportRef,
    pub(super) source_id: PublishedSourceId,
    pub(super) selector: SourceSelector,
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    pub(super) budget: ReceiverVideoBudgetDiagnostics,
    pub(super) outcomes: BudgetSolverOutcomes,
    pub(super) pressure_observations: u8,
    pub(super) upgrade_observations: u8,
    pub(super) packet_gate: Option<SourcePacketGate>,
    pub(super) route_activity_update: bool,
    pub(super) request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub fn route_activity(
        route: ConsumerRouteTransportRef,
        source_id: PublishedSourceId,
        current_selection: ConsumerSourceSelection,
        policy_pause_reason: Option<PolicyPauseReason>,
    ) -> Option<Self> {
        let route_activity_update = policy_pause_reason != current_selection.policy_pause_reason();
        route_activity_update.then(|| Self {
            route,
            source_id,
            selector: current_selection.selector(),
            policy_pause_reason,
            budget: current_selection.budget(),
            outcomes: BudgetSolverOutcomes::default(),
            pressure_observations: current_selection.pressure_observations(),
            upgrade_observations: current_selection.upgrade_observations(),
            packet_gate: None,
            route_activity_update,
            request_keyframe: false,
        })
    }

    pub fn route(&self) -> &ConsumerRouteTransportRef {
        &self.route
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub const fn selector(&self) -> SourceSelector {
        self.selector
    }

    pub const fn policy_pause_reason(&self) -> Option<PolicyPauseReason> {
        self.policy_pause_reason
    }

    pub const fn route_active(&self) -> bool {
        self.policy_pause_reason.is_none()
    }

    pub const fn budget(&self) -> ReceiverVideoBudgetDiagnostics {
        self.budget
    }

    pub const fn outcomes(&self) -> BudgetSolverOutcomes {
        self.outcomes
    }

    pub const fn pressure_observations(&self) -> u8 {
        self.pressure_observations
    }

    pub const fn upgrade_observations(&self) -> u8 {
        self.upgrade_observations
    }

    pub fn packet_gate(&self) -> Option<&SourcePacketGate> {
        self.packet_gate.as_ref()
    }

    pub const fn route_activity_update(&self) -> bool {
        self.route_activity_update
    }

    pub const fn request_keyframe(&self) -> bool {
        self.request_keyframe
    }
}

/// Server-owned featured state derived from active-speaker observations.
///
/// This lives beside the video route actions because current featured
/// projection and quality floor both derive from the same transport
/// active-speaker snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct FeaturedUserUpdate {
    user_id: UserId,
    featured: Option<bool>,
}

impl FeaturedUserUpdate {
    #[must_use]
    pub fn new(user_id: UserId, featured: Option<bool>) -> Self {
        Self { user_id, featured }
    }

    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    pub const fn featured(&self) -> Option<bool> {
        self.featured
    }
}
