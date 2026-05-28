//! Producer-side entities tracked by the pure router.

use super::{MediaKind, ProducerId, ProducerRouteState, TransportId};

/// Media source attached to a receive transport inside the pure router.
///
/// A producer stores only router topology and source-side route state.
/// Transport handles, RTP parameters and packet forwarding state stay in the
/// runtime and transport layers so the router can remain a pure state machine.
///
/// The producer route state is authoritative for the source shadow seen by all
/// consumers of this producer. Consumer-local route state is stored on each
/// consumer instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producer {
    id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    route_state: ProducerRouteState,
}

impl Producer {
    /// Builds an active producer for a receive transport.
    ///
    /// Callers that restore paused state from an existing room snapshot should
    /// use [`Producer::with_route_state`] before inserting the producer.
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

    /// Returns the producer route state as a compatibility paused flag.
    ///
    /// Router mutation APIs use [`ProducerRouteState`] directly. This view is
    /// kept for callers that need to compare with legacy client state.
    #[must_use]
    pub fn paused(&self) -> bool {
        self.route_state.is_paused()
    }

    /// Returns the source-side route state shadowed onto dependent consumers.
    #[must_use]
    pub fn route_state(&self) -> ProducerRouteState {
        self.route_state
    }

    /// Returns a copy of this producer with a different source route state.
    ///
    /// This builder only changes the producer. Shadowing to consumers is a
    /// router transition and must go through
    /// [`Router::set_producer_route_state`](super::Router::set_producer_route_state).
    #[must_use]
    pub fn with_route_state(mut self, route_state: ProducerRouteState) -> Self {
        self.route_state = route_state;
        self
    }

    pub(super) fn set_route_state(&mut self, route_state: ProducerRouteState) {
        self.route_state = route_state;
    }
}
