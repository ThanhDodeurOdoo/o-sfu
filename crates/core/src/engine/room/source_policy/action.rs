use super::super::media_graph::SubscriptionKey;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteBudgetOutcome {
    Degraded,
    Paused,
    Resumed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::room) struct ConsumerPacketSelectionUpdate {
    pub(super) key: SubscriptionKey,
    pub(super) source_id: PublishedSourceId,
    pub(in crate::engine::room) route: TransportConsumerRoute,
    pub(super) selector: SourceSelector,
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    pub(super) budget: ReceiverVideoBudgetDiagnostics,
    pub(super) outcome: Option<RouteBudgetOutcome>,
    pub(super) pressure_observations: u8,
    pub(super) upgrade_observations: u8,
    pub(super) packet_gate: Option<SourcePacketGate>,
    pub(super) route_activity_changed: bool,
    pub(super) request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub(in crate::engine::room) fn route_activity(
        key: SubscriptionKey,
        source_id: PublishedSourceId,
        route: TransportConsumerRoute,
        current_selection: ConsumerSourceSelection,
        policy_pause_reason: Option<PolicyPauseReason>,
    ) -> Option<Self> {
        (policy_pause_reason != current_selection.policy_pause_reason()).then(|| Self {
            key,
            source_id,
            route,
            selector: current_selection.selector(),
            policy_pause_reason,
            budget: current_selection.budget(),
            outcome: None,
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

    pub(super) const fn requires_follow_up(&self) -> bool {
        self.pressure_observations > 0 || self.upgrade_observations > 0
    }

    pub(in crate::engine::room) fn route_control(&self) -> ConsumerRouteControl {
        let mut control =
            ConsumerRouteControl::new(self.route.clone()).request_keyframe(self.request_keyframe);
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
