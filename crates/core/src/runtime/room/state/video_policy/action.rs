//! Video policy action vocabulary.
//!
//! The budget planner emits semantic actions, not transport calls. `Send`
//! resolves to a source selector and active consumer route; `Pause` keeps the
//! subscription intact while withholding RTP delivery for a policy-owned reason.

use super::{input::ReceiverVideoRouteInput, projection::source_packet_gate_for_selector};
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::{SourcePacketGate, TransportMediaId},
    source_model::{
        PolicyPauseReason, PublishedSourceId, ReceiverVideoBudgetDiagnostics, SourceSelector,
    },
};

/// Semantic decision for one receiver/source video route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::room) enum VideoRouteAction {
    /// Forward the source with the selected source-domain quality constraint.
    Send(SourceSelector),
    /// Withhold the route for a server-owned policy reason.
    Pause(PolicyPauseReason),
}

/// One route action plus the route identity needed for stale-update checks.
#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteAction<'a> {
    route: &'a ReceiverVideoRouteInput<'a>,
    action: VideoRouteAction,
    budget: ReceiverVideoBudgetDiagnostics,
    outcomes: BudgetSolverOutcomes,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

impl<'a> ReceiverVideoRouteAction<'a> {
    #[must_use]
    pub fn new(
        route: &'a ReceiverVideoRouteInput<'a>,
        action: VideoRouteAction,
        budget: ReceiverVideoBudgetDiagnostics,
        outcomes: BudgetSolverOutcomes,
        pressure_observations: u8,
        upgrade_observations: u8,
        request_keyframe: bool,
    ) -> Self {
        Self {
            route,
            action,
            budget,
            outcomes,
            pressure_observations,
            upgrade_observations,
            request_keyframe,
        }
    }

    pub fn into_selection_update(self) -> Option<ConsumerPacketSelectionUpdate> {
        let current_selection = self.route.current_selection();
        let (selector, policy_pause_reason, request_keyframe) = match self.action {
            VideoRouteAction::Send(selector) => (
                selector,
                None,
                self.request_keyframe || !current_selection.policy_allows_delivery(),
            ),
            VideoRouteAction::Pause(reason) => (current_selection.selector(), Some(reason), false),
        };
        let packet_gate = if selector == current_selection.selector() {
            None
        } else {
            Some(source_packet_gate_for_selector(self.route.source(), selector).ok()?)
        };
        let route_activity_update = policy_pause_reason != current_selection.policy_pause_reason();
        if packet_gate.is_none()
            && !route_activity_update
            && self.budget == current_selection.budget()
            && self.pressure_observations == current_selection.pressure_observations()
            && self.upgrade_observations == current_selection.upgrade_observations()
        {
            return None;
        }
        Some(ConsumerPacketSelectionUpdate {
            consumer_user_id: self.route.consumer_user_id().clone(),
            consumer_connection_id: self.route.consumer_connection_id(),
            source_user_id: self.route.source_user_id().clone(),
            source_connection_id: self.route.source_connection_id(),
            source_transport_media_id: self.route.source_transport_media_id(),
            consumer_transport_media_id: self.route.consumer_transport_media_id(),
            source_id: self.route.source_id(),
            selector,
            policy_pause_reason,
            budget: self.budget,
            outcomes: self.outcomes,
            pressure_observations: self.pressure_observations,
            upgrade_observations: self.upgrade_observations,
            packet_gate,
            route_activity_update,
            request_keyframe,
        })
    }
}

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
    consumer_user_id: UserId,
    consumer_connection_id: ConnectionId,
    source_user_id: UserId,
    source_connection_id: ConnectionId,
    source_transport_media_id: TransportMediaId,
    consumer_transport_media_id: TransportMediaId,
    source_id: PublishedSourceId,
    selector: SourceSelector,
    policy_pause_reason: Option<PolicyPauseReason>,
    budget: ReceiverVideoBudgetDiagnostics,
    outcomes: BudgetSolverOutcomes,
    pressure_observations: u8,
    upgrade_observations: u8,
    packet_gate: Option<SourcePacketGate>,
    route_activity_update: bool,
    request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub fn consumer_user_id(&self) -> &UserId {
        &self.consumer_user_id
    }

    pub const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub fn source_user_id(&self) -> &UserId {
        &self.source_user_id
    }

    pub const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    pub const fn consumer_transport_media_id(&self) -> TransportMediaId {
        self.consumer_transport_media_id
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
