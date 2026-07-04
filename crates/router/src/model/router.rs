//! pure router state machine plus local dependency indexes

use super::{
    Consumer, ConsumerId, ConsumerRouteState, ConsumerSpec, Producer, ProducerId,
    ProducerRouteState, ProducerSpec, ReceiveTransportHandle, RouterError, RouterId,
    SendTransportHandle, Session, SessionHandle, SessionId, Transport, TransportDirection,
    TransportId, proof_storage::BTreeMap, relation_index::RouterIndexes,
};

/// pure routing state for one router instance
///
/// the router is synchronous and in-memory
/// it does not perform media I/O or signaling negotiation
/// it only accepts typed router-domain entities and keeps the topology internally
/// consistent
///
/// the primary maps store live entities
/// reverse indexes mirror dependency edges needed for teardown and
/// producer route-state propagation
/// every mutation that changes a primary entity must keep the matching reverse
/// relation in the same transition
#[derive(Debug, Clone)]
pub struct Router {
    pub(super) id: RouterId,
    pub(super) sessions: BTreeMap<SessionId, Session>,
    pub(super) transports: BTreeMap<TransportId, Transport>,
    pub(super) producers: BTreeMap<ProducerId, Producer>,
    pub(super) consumers: BTreeMap<ConsumerId, Consumer>,
    pub(super) indexes: RouterIndexes,
}

impl Router {
    #[must_use]
    pub fn new(id: RouterId) -> Self {
        Self {
            id,
            sessions: BTreeMap::new(),
            transports: BTreeMap::new(),
            producers: BTreeMap::new(),
            consumers: BTreeMap::new(),
            indexes: RouterIndexes::new(),
        }
    }

    #[must_use]
    pub fn id(&self) -> RouterId {
        self.id
    }

    /// admit a new session to the router
    ///
    /// this is the first state transition for a participant
    /// a joined session has no transports or media yet, but later transitions
    /// require the session to exist first
    ///
    /// # Errors
    ///
    /// returns [`RouterError::DuplicateSession`] when the session already exists
    pub fn join(&mut self, session: Session) -> Result<(), RouterError> {
        let session_id = session.id();
        if self.sessions.contains_key(&session_id) {
            return Err(RouterError::DuplicateSession(session_id));
        }
        self.sessions.insert(session_id, session);
        Ok(())
    }

    /// start a scoped mutation flow for an existing session
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingSession`] when the session does not exist
    pub fn session(&mut self, session_id: SessionId) -> Result<SessionHandle<'_>, RouterError> {
        if !self.sessions.contains_key(&session_id) {
            return Err(RouterError::MissingSession(session_id));
        }
        Ok(SessionHandle::new(self, session_id))
    }

    /// start a scoped mutation flow for an existing receive transport
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingTransport`] when the transport does not exist
    /// or [`RouterError::ProducerRequiresReceiveTransport`] when the transport is
    /// not a receive transport
    pub fn receive_transport(
        &mut self,
        transport_id: TransportId,
    ) -> Result<ReceiveTransportHandle<'_>, RouterError> {
        self.ensure_transport_direction(transport_id, TransportDirection::Receive)?;
        Ok(ReceiveTransportHandle::new(self, transport_id))
    }

    /// start a scoped mutation flow for an existing send transport
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingTransport`] when the transport does not exist
    /// or [`RouterError::ConsumerRequiresSendTransport`] when the transport is
    /// not a send transport
    pub fn send_transport(
        &mut self,
        transport_id: TransportId,
    ) -> Result<SendTransportHandle<'_>, RouterError> {
        self.ensure_transport_direction(transport_id, TransportDirection::Send)?;
        Ok(SendTransportHandle::new(self, transport_id))
    }

    pub(super) fn insert_transport(
        &mut self,
        transport_id: TransportId,
        session_id: SessionId,
        direction: TransportDirection,
    ) -> Result<(), RouterError> {
        if self.transports.contains_key(&transport_id) {
            return Err(RouterError::DuplicateTransport(transport_id));
        }
        self.transports.insert(
            transport_id,
            Transport::new(transport_id, session_id, direction),
        );
        self.indexes.add_transport(session_id, transport_id);
        Ok(())
    }

    pub(super) fn insert_producer(
        &mut self,
        transport_id: TransportId,
        spec: ProducerSpec,
    ) -> Result<ProducerId, RouterError> {
        let producer_id = spec.id();
        if self.producers.contains_key(&producer_id) {
            return Err(RouterError::DuplicateProducer(producer_id));
        }
        let media_kind = spec.media_kind();
        self.producers.insert(
            producer_id,
            Producer::new(producer_id, transport_id, media_kind),
        );
        self.indexes.add_producer(transport_id, producer_id);
        Ok(producer_id)
    }

    pub(super) fn insert_consumer(
        &mut self,
        transport_id: TransportId,
        spec: ConsumerSpec,
    ) -> Result<ConsumerId, RouterError> {
        let producer_id = spec.producer_id();
        let Some(producer) = self.producers.get(&producer_id) else {
            return Err(RouterError::MissingProducer(producer_id));
        };
        if !spec.capability().is_compatible() {
            return Err(RouterError::IncompatibleCapabilities { producer_id });
        }
        let consumer_id = spec.id();
        if self.consumers.contains_key(&consumer_id) {
            return Err(RouterError::DuplicateConsumer(consumer_id));
        }
        let consumer = Consumer::new(
            consumer_id,
            producer_id,
            transport_id,
            producer.media_kind(),
            spec.route_state(),
            producer.route_state(),
        );
        self.consumers.insert(consumer_id, consumer);
        self.indexes
            .add_consumer(transport_id, producer_id, consumer_id);
        Ok(consumer_id)
    }

    fn ensure_transport_direction(
        &self,
        transport_id: TransportId,
        direction: TransportDirection,
    ) -> Result<(), RouterError> {
        let Some(&transport) = self.transports.get(&transport_id) else {
            return Err(RouterError::MissingTransport(transport_id));
        };
        if transport.direction() == direction {
            return Ok(());
        }
        match direction {
            TransportDirection::Send => {
                Err(RouterError::ConsumerRequiresSendTransport(transport_id))
            }
            TransportDirection::Receive => {
                Err(RouterError::ProducerRequiresReceiveTransport(transport_id))
            }
        }
    }

    /// set the source-side route state for a producer
    ///
    /// this is the authoritative producer pause transition in the pure router
    /// it mutates the producer and then updates the producer shadow stored on
    /// each dependent consumer
    /// the consumer-local route state is left unchanged
    ///
    /// the update cost is proportional to the number of consumers attached to
    /// the producer because the router uses the reverse producer-consumer index
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingProducer`] when the producer does not exist
    pub fn set_producer_route_state(
        &mut self,
        producer_id: ProducerId,
        route_state: ProducerRouteState,
    ) -> Result<(), RouterError> {
        let Some(producer) = self.producers.get_mut(&producer_id) else {
            return Err(RouterError::MissingProducer(producer_id));
        };
        producer.set_route_state(route_state);

        let consumer_ids = self.indexes.producer_consumers_snapshot(producer_id);
        for consumer_id in consumer_ids {
            if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                consumer.set_producer_route_state(route_state);
            }
        }

        Ok(())
    }

    /// set the receiver-local route state for a consumer
    ///
    /// this only changes the consumer's own route state
    /// it does not change the producer route state or any other consumer that
    /// depends on the same producer
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingConsumer`] when the consumer does not exist
    pub fn set_consumer_route_state(
        &mut self,
        consumer_id: ConsumerId,
        route_state: ConsumerRouteState,
    ) -> Result<(), RouterError> {
        let Some(consumer) = self.consumers.get_mut(&consumer_id) else {
            return Err(RouterError::MissingConsumer(consumer_id));
        };
        consumer.set_route_state(route_state);
        Ok(())
    }

    /// remove one producer and every consumer that depends on it
    ///
    /// unrelated transports and unrelated consumers stay live
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingProducer`] when the producer does not exist
    pub fn remove_producer(&mut self, producer_id: ProducerId) -> Result<(), RouterError> {
        if !self.producers.contains_key(&producer_id) {
            return Err(RouterError::MissingProducer(producer_id));
        }
        self.detach_producer(producer_id);
        Ok(())
    }

    /// remove one receiver-side route without touching its producer
    ///
    /// this is the narrowest teardown path
    /// it only removes the consumer primary
    /// record plus the two reverse relations that mention it
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingConsumer`] when the consumer does not exist
    pub fn remove_consumer(&mut self, consumer_id: ConsumerId) -> Result<(), RouterError> {
        if !self.consumers.contains_key(&consumer_id) {
            return Err(RouterError::MissingConsumer(consumer_id));
        }
        self.detach_consumer(consumer_id);
        Ok(())
    }

    /// remove a session and cascade all dependent transports plus media entities
    ///
    /// this is the authoritative teardown path for session-owned router state
    /// reverse indexes guarantee that cleanup only walks the transports,
    /// producers and consumers that belong to the removed session
    ///
    /// # Errors
    ///
    /// returns [`RouterError::MissingSession`] when the session does not exist
    pub fn remove_session(&mut self, session_id: SessionId) -> Result<(), RouterError> {
        let Some(mut session) = self.sessions.remove(&session_id) else {
            return Err(RouterError::MissingSession(session_id));
        };
        session.close();

        let transport_ids = self.indexes.take_session_transports(session_id);
        for transport_id in transport_ids {
            self.remove_transport(transport_id);
        }

        Ok(())
    }

    fn remove_transport(&mut self, transport_id: TransportId) {
        let Some(transport) = self.transports.remove(&transport_id) else {
            return;
        };

        self.indexes
            .remove_transport_from_session(transport.session_id(), transport_id);

        let consumer_ids = self.indexes.take_transport_consumers(transport_id);
        for consumer_id in consumer_ids {
            self.detach_consumer(consumer_id);
        }

        let producer_ids = self.indexes.take_transport_producers(transport_id);
        for producer_id in producer_ids {
            self.detach_producer(producer_id);
        }
    }

    fn detach_producer(&mut self, producer_id: ProducerId) {
        let Some(producer) = self.producers.remove(&producer_id) else {
            return;
        };
        let transport_id = producer.transport_id();

        self.indexes
            .remove_producer_from_transport(transport_id, producer_id);

        let consumer_ids = self.indexes.take_producer_consumers(producer_id);
        for consumer_id in consumer_ids {
            self.detach_consumer(consumer_id);
        }
    }

    fn detach_consumer(&mut self, consumer_id: ConsumerId) {
        let Some(consumer) = self.consumers.remove(&consumer_id) else {
            return;
        };

        self.indexes
            .remove_consumer(consumer.transport_id(), consumer.producer_id(), consumer_id);
    }
}
