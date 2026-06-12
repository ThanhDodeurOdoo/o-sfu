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
    #[must_use]
    pub(super) fn new(id: ProducerId, transport_id: TransportId, media_kind: MediaKind) -> Self {
        Self {
            id,
            transport_id,
            media_kind,
            route_state: ProducerRouteState::Active,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
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

    #[must_use]
    pub fn route_state(&self) -> ProducerRouteState {
        self.route_state
    }

    pub(super) fn set_route_state(&mut self, route_state: ProducerRouteState) {
        self.route_state = route_state;
    }
}
