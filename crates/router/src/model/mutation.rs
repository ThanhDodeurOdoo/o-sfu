use super::{
    ConsumerCapability, ConsumerId, ConsumerRouteState, MediaKind, ProducerId, Router, RouterError,
    SessionId, TransportDirection, TransportId,
};

/// producer input accepted by a receive-transport handle.
///
/// The transport id is intentionally not caller-supplied. It comes from the
/// handle so producers cannot be attached to a send transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSpec {
    id: ProducerId,
    media_kind: MediaKind,
}

impl ProducerSpec {
    #[must_use]
    pub const fn new(id: ProducerId, media_kind: MediaKind) -> Self {
        Self { id, media_kind }
    }

    pub(super) const fn id(self) -> ProducerId {
        self.id
    }

    pub(super) const fn media_kind(self) -> MediaKind {
        self.media_kind
    }
}

/// consumer input accepted by a send-transport handle.
///
/// The transport id and media kind are intentionally absent. The handle supplies
/// the send transport and the router derives media kind from the source
/// producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSpec {
    id: ConsumerId,
    producer_id: ProducerId,
    capability: ConsumerCapability,
    route_state: ConsumerRouteState,
}

impl ConsumerSpec {
    #[must_use]
    pub const fn new(
        id: ConsumerId,
        producer_id: ProducerId,
        capability: ConsumerCapability,
    ) -> Self {
        Self {
            id,
            producer_id,
            capability,
            route_state: ConsumerRouteState::Active,
        }
    }

    #[must_use]
    pub const fn with_route_state(mut self, route_state: ConsumerRouteState) -> Self {
        self.route_state = route_state;
        self
    }

    pub(super) const fn id(self) -> ConsumerId {
        self.id
    }

    pub(super) const fn producer_id(self) -> ProducerId {
        self.producer_id
    }

    pub(super) const fn capability(self) -> ConsumerCapability {
        self.capability
    }

    pub(super) const fn route_state(self) -> ConsumerRouteState {
        self.route_state
    }
}

/// short-lived mutation scope for one live session.
///
/// The handle borrows the router mutably, so no other router mutation can
/// interleave between session lookup and transport creation.
pub struct SessionHandle<'a> {
    router: &'a mut Router,
    session_id: SessionId,
}

impl<'a> SessionHandle<'a> {
    pub(super) const fn new(router: &'a mut Router, session_id: SessionId) -> Self {
        Self { router, session_id }
    }

    /// open a receive transport owned by this session
    ///
    /// # Errors
    ///
    /// returns [`RouterError::DuplicateTransport`] when the transport id is
    /// already live
    pub fn open_receive_transport(
        self,
        transport_id: TransportId,
    ) -> Result<ReceiveTransportHandle<'a>, RouterError> {
        self.router
            .insert_transport(transport_id, self.session_id, TransportDirection::Receive)?;
        Ok(ReceiveTransportHandle::new(self.router, transport_id))
    }

    /// open a send transport owned by this session
    ///
    /// # Errors
    ///
    /// returns [`RouterError::DuplicateTransport`] when the transport id is
    /// already live
    pub fn open_send_transport(
        self,
        transport_id: TransportId,
    ) -> Result<SendTransportHandle<'a>, RouterError> {
        self.router
            .insert_transport(transport_id, self.session_id, TransportDirection::Send)?;
        Ok(SendTransportHandle::new(self.router, transport_id))
    }
}

/// short-lived mutation scope for one live receive transport.
///
/// The handle proves the transport direction before publishing, so producer
/// attachment only checks id uniqueness.
pub struct ReceiveTransportHandle<'a> {
    router: &'a mut Router,
    transport_id: TransportId,
}

impl<'a> ReceiveTransportHandle<'a> {
    pub(super) const fn new(router: &'a mut Router, transport_id: TransportId) -> Self {
        Self {
            router,
            transport_id,
        }
    }

    /// publish a producer on this receive transport
    ///
    /// # Errors
    ///
    /// returns [`RouterError::DuplicateProducer`] when the producer id is
    /// already live
    pub fn publish(self, spec: ProducerSpec) -> Result<ProducerId, RouterError> {
        self.router.insert_producer(self.transport_id, spec)
    }
}

/// short-lived mutation scope for one live send transport.
///
/// The handle proves the transport direction before consumer attachment, so
/// consuming only checks producer existence, capability and id uniqueness.
pub struct SendTransportHandle<'a> {
    router: &'a mut Router,
    transport_id: TransportId,
}

impl<'a> SendTransportHandle<'a> {
    pub(super) const fn new(router: &'a mut Router, transport_id: TransportId) -> Self {
        Self {
            router,
            transport_id,
        }
    }

    /// consume an existing producer through this send transport
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingProducer`] when the source producer does
    /// not exist, [`RouterError::IncompatibleCapabilities`] when capability
    /// negotiation rejected the route or [`RouterError::DuplicateConsumer`]
    /// when the consumer id is already live
    pub fn consume(self, spec: ConsumerSpec) -> Result<ConsumerId, RouterError> {
        self.router.insert_consumer(self.transport_id, spec)
    }
}
