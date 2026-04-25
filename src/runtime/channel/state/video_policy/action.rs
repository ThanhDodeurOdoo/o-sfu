//! Video policy action vocabulary.
//!
//! The budget planner emits semantic actions, not transport calls. Today the
//! live action is `Send(SourceSelector)`, which preserves the existing RID
//! selection behavior. The paused-route action is intentionally present as the
//! future policy vocabulary for overload work, but it is not emitted until the
//! budget solver starts withholding routes.

use o_sfu_protocol::shared::SessionId;

use super::{input::ReceiverVideoRouteInput, projection::source_packet_gate_for_selector};
use crate::runtime::{
    ConnectionId,
    source_model::{PolicyPauseReason, PublishedSourceId, SourceSelector},
    transport_adapter::{SourcePacketGate, TransportMediaId},
};

/// Semantic decision for one receiver/source video route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) enum VideoRouteAction {
    /// Forward the source with the selected source-domain quality constraint.
    Send(SourceSelector),
    /// Withhold the route for a server-owned policy reason.
    #[allow(
        dead_code,
        reason = "route-pause decisions are introduced by the later receiver budget solver task"
    )]
    Pause(PolicyPauseReason),
}

/// One route action plus the route identity needed for stale-update checks.
#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct ReceiverVideoRouteAction<'a> {
    route: ReceiverVideoRouteInput<'a>,
    action: VideoRouteAction,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

impl<'a> ReceiverVideoRouteAction<'a> {
    #[must_use]
    pub(in crate::runtime::channel) fn new(
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

    pub(in crate::runtime::channel) fn into_selection_update(
        self,
    ) -> Option<ConsumerPacketSelectionUpdate> {
        let VideoRouteAction::Send(selector) = self.action else {
            return None;
        };
        let packet_gate = if selector == self.route.current_selection().selector() {
            None
        } else {
            Some(source_packet_gate_for_selector(self.route.source(), selector).ok()?)
        };
        if packet_gate.is_none()
            && self.pressure_observations == self.route.current_selection().pressure_observations()
            && self.upgrade_observations == self.route.current_selection().upgrade_observations()
        {
            return None;
        }
        Some(ConsumerPacketSelectionUpdate {
            consumer_session_id: self.route.consumer_session_id().clone(),
            consumer_connection_id: self.route.consumer_connection_id(),
            source_session_id: self.route.source_session_id().clone(),
            source_connection_id: self.route.source_connection_id(),
            source_transport_media_id: self.route.source_transport_media_id(),
            consumer_transport_media_id: self.route.consumer_transport_media_id(),
            source_id: self.route.source_id(),
            selector,
            pressure_observations: self.pressure_observations,
            upgrade_observations: self.upgrade_observations,
            packet_gate,
            request_keyframe: self.request_keyframe,
        })
    }
}

/// One receiver-side source selection that is ready for the effect boundary.
///
/// The update carries the transport handles and connection ids observed while
/// planning. Commit revalidates them after async transport work so stale
/// replacement or cleanup events cannot write selector state onto a newer route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ConsumerPacketSelectionUpdate {
    consumer_session_id: SessionId,
    consumer_connection_id: ConnectionId,
    source_session_id: SessionId,
    source_connection_id: ConnectionId,
    source_transport_media_id: TransportMediaId,
    consumer_transport_media_id: TransportMediaId,
    source_id: PublishedSourceId,
    selector: SourceSelector,
    pressure_observations: u8,
    upgrade_observations: u8,
    packet_gate: Option<SourcePacketGate>,
    request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub(in crate::runtime::channel) fn consumer_session_id(&self) -> &SessionId {
        &self.consumer_session_id
    }

    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::channel) fn source_session_id(&self) -> &SessionId {
        &self.source_session_id
    }

    pub(in crate::runtime::channel) const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub(in crate::runtime::channel) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    pub(in crate::runtime::channel) const fn consumer_transport_media_id(
        &self,
    ) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    pub(in crate::runtime::channel) const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub(in crate::runtime::channel) const fn selector(&self) -> SourceSelector {
        self.selector
    }

    pub(in crate::runtime::channel) const fn pressure_observations(&self) -> u8 {
        self.pressure_observations
    }

    pub(in crate::runtime::channel) const fn upgrade_observations(&self) -> u8 {
        self.upgrade_observations
    }

    pub(in crate::runtime::channel) fn packet_gate(&self) -> Option<&SourcePacketGate> {
        self.packet_gate.as_ref()
    }

    pub(in crate::runtime::channel) const fn request_keyframe(&self) -> bool {
        self.request_keyframe
    }
}

/// Server-owned featured state derived from active-speaker observations.
///
/// This lives beside the video route actions because current featured
/// projection and quality floor both derive from the same transport
/// active-speaker snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct FeaturedSessionUpdate {
    session_id: SessionId,
    featured: Option<bool>,
}

impl FeaturedSessionUpdate {
    #[must_use]
    pub(in crate::runtime::channel) fn new(session_id: SessionId, featured: Option<bool>) -> Self {
        Self {
            session_id,
            featured,
        }
    }

    pub(in crate::runtime::channel) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub(in crate::runtime::channel) const fn featured(&self) -> Option<bool> {
        self.featured
    }
}
