//! producer-side entities tracked by the pure router

use super::{MediaKind, ProducerId, ProducerRouteState, TransportId};

/// media source attached to a receive transport inside the pure router
///
/// a producer stores only router topology and source-side route state
/// transport handles, RTP parameters and packet forwarding state stay in the
/// runtime and transport layers so the router can remain a pure state machine
///
/// the producer route state is authoritative for the source shadow seen by all
/// consumers of this producer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producer {
    id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    route_state: ProducerRouteState,
}

impl Producer {
    /// build an active producer for a receive transport
    ///
    /// callers that restore paused state from a snapshot should apply it before
    /// inserting the producer
    #[must_use]
    pub fn new(id: ProducerId, transport_id: TransportId, media_kind: MediaKind) -> Self {
        Self {
            id,
            transport_id,
            media_kind,
            route_state: ProducerRouteState::Active,
        }
    }

    #[must_use]
    pub fn id(&self) -> ProducerId {
        self.id
    }

    #[must_use]
    pub fn transport_id(&self) -> TransportId {
        self.transport_id
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    /// producer route state as a compatibility paused flag
    ///
    /// router mutation APIs use [`ProducerRouteState`] directly
    #[must_use]
    pub fn paused(&self) -> bool {
        self.route_state.is_paused()
    }

    #[must_use]
    pub fn route_state(&self) -> ProducerRouteState {
        self.route_state
    }

    /// staged copy with a different source route state
    ///
    /// consumer shadowing is a router transition and must go through
    /// [`Router::set_producer_route_state`](super::Router::set_producer_route_state)
    #[must_use]
    pub fn with_route_state(mut self, route_state: ProducerRouteState) -> Self {
        self.route_state = route_state;
        self
    }

    pub(super) fn set_route_state(&mut self, route_state: ProducerRouteState) {
        self.route_state = route_state;
    }
}
