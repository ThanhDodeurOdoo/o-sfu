//! Video policy action vocabulary.
//!
//! The budget planner emits semantic actions, not transport calls. `Send`
//! resolves to a source selector and active consumer route; `Pause` keeps the
//! subscription intact while withholding RTP delivery for a policy-owned reason.

use super::{input::ReceiverVideoRouteInput, projection::source_packet_gate_for_selector};
use crate::runtime::{
    ConnectionId, UserId,
    source_model::{PolicyPauseReason, PublishedSourceId, SourceSelector},
    transport_adapter::{SourcePacketGate, TransportMediaId},
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
    route: ReceiverVideoRouteInput<'a>,
    action: VideoRouteAction,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

impl<'a> ReceiverVideoRouteAction<'a> {
    #[must_use]
    pub(in crate::runtime::room) fn new(
        route: ReceiverVideoRouteInput<'a>,
        action: VideoRouteAction,
        pressure_observations: u8,
        upgrade_observations: u8,
        request_keyframe: bool,
    ) -> Self {
        Self {
            route,
            action,
            pressure_observations,
            upgrade_observations,
            request_keyframe,
        }
    }

    pub(in crate::runtime::room) fn into_selection_update(
        self,
    ) -> Option<ConsumerPacketSelectionUpdate> {
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
            pressure_observations: self.pressure_observations,
            upgrade_observations: self.upgrade_observations,
            packet_gate,
            route_activity_update,
            request_keyframe,
        })
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
    pressure_observations: u8,
    upgrade_observations: u8,
    packet_gate: Option<SourcePacketGate>,
    route_activity_update: bool,
    request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub(in crate::runtime::room) fn consumer_user_id(&self) -> &UserId {
        &self.consumer_user_id
    }

    pub(in crate::runtime::room) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::room) fn source_user_id(&self) -> &UserId {
        &self.source_user_id
    }

    pub(in crate::runtime::room) const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub(in crate::runtime::room) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    pub(in crate::runtime::room) const fn consumer_transport_media_id(&self) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    pub(in crate::runtime::room) const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub(in crate::runtime::room) const fn selector(&self) -> SourceSelector {
        self.selector
    }

    pub(in crate::runtime::room) const fn policy_pause_reason(&self) -> Option<PolicyPauseReason> {
        self.policy_pause_reason
    }

    pub(in crate::runtime::room) const fn route_active(&self) -> bool {
        self.policy_pause_reason.is_none()
    }

    pub(in crate::runtime::room) const fn pressure_observations(&self) -> u8 {
        self.pressure_observations
    }

    pub(in crate::runtime::room) const fn upgrade_observations(&self) -> u8 {
        self.upgrade_observations
    }

    pub(in crate::runtime::room) fn packet_gate(&self) -> Option<&SourcePacketGate> {
        self.packet_gate.as_ref()
    }

    pub(in crate::runtime::room) const fn route_activity_update(&self) -> bool {
        self.route_activity_update
    }

    pub(in crate::runtime::room) const fn request_keyframe(&self) -> bool {
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
    pub(in crate::runtime::room) fn new(user_id: UserId, featured: Option<bool>) -> Self {
        Self { user_id, featured }
    }

    pub(in crate::runtime::room) fn user_id(&self) -> &UserId {
        &self.user_id
    }

    pub(in crate::runtime::room) const fn featured(&self) -> Option<bool> {
        self.featured
    }
}
