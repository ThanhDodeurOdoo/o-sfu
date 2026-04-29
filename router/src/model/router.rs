//! Pure router state machine plus the reverse indexes that keep teardown local.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    Consumer, ConsumerCapability, ConsumerId, ConsumerRouteState, NoopRouterObserver, Producer,
    ProducerId, ProducerRouteState, RouterError, RouterEvent, RouterId, RouterObserver, Session,
    SessionId, Transport, TransportDirection, TransportId,
};

/// Pure routing state for one router instance.
///
/// The main maps hold the persistent entities.
///
/// And their associated reverse indexes to keep dependency cleanup proportional
/// to the removed entity instead of requiring scans across the full router state.
#[derive(Debug, Clone)]
pub struct Router<O: RouterObserver = NoopRouterObserver> {
    pub(super) id: RouterId,
    /// Live sessions that currently belong to this router.
    pub(super) sessions: BTreeMap<SessionId, Session>,
    pub(super) transports: BTreeMap<TransportId, Transport>,
    pub(super) producers: BTreeMap<ProducerId, Producer>,
    pub(super) consumers: BTreeMap<ConsumerId, Consumer>,
    // Reverse indexes
    pub(super) session_transports: BTreeMap<SessionId, BTreeSet<TransportId>>,
    pub(super) transport_producers: BTreeMap<TransportId, BTreeSet<ProducerId>>,
    pub(super) transport_consumers: BTreeMap<TransportId, BTreeSet<ConsumerId>>,
    pub(super) producer_consumers: BTreeMap<ProducerId, BTreeSet<ConsumerId>>,
    observer: O,
}

impl Router<NoopRouterObserver> {
    #[must_use]
    pub fn new(id: RouterId) -> Self {
        Self::new_with_observer(id, NoopRouterObserver)
    }
}

impl<O: RouterObserver> Router<O> {
    #[must_use]
    pub fn new_with_observer(id: RouterId, observer: O) -> Self {
        Self {
            id,
            sessions: BTreeMap::new(),
            transports: BTreeMap::new(),
            producers: BTreeMap::new(),
            consumers: BTreeMap::new(),
            session_transports: BTreeMap::new(),
            transport_producers: BTreeMap::new(),
            transport_consumers: BTreeMap::new(),
            producer_consumers: BTreeMap::new(),
            observer,
        }
    }

    #[must_use]
    pub fn id(&self) -> RouterId {
        self.id
    }

    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn sessions(&self) -> impl Iterator<Item = &Session> {
        self.sessions.values()
    }

    /// Admit a new session to the router.
    ///
    /// This is the first state transition for a participant. A joined session
    /// has no transports or media yet, but later transitions require the
    /// session to exist first.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::DuplicateSession`] when the session already exists.
    pub fn join_session(&mut self, session: Session) -> Result<(), RouterError> {
        let session_id = session.id();
        if self.sessions.contains_key(&session_id) {
            return Err(RouterError::DuplicateSession(session_id));
        }
        self.sessions.insert(session_id, session);
        self.observer
            .on_event(RouterEvent::SessionJoined { session_id });
        Ok(())
    }

    /// Register a transport under an existing session.
    ///
    /// The router only records ownership and direction here. Transport-specific
    /// protocol or WebRTC state stays outside the router core.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the owning session does not exist
    /// or [`RouterError::DuplicateTransport`] when the transport already exists.
    pub fn open_transport(&mut self, transport: Transport) -> Result<(), RouterError> {
        let transport_id = transport.id();
        let session_id = transport.session_id();
        if !self.sessions.contains_key(&session_id) {
            return Err(RouterError::MissingSession(session_id));
        }
        if self.transports.contains_key(&transport_id) {
            return Err(RouterError::DuplicateTransport(transport_id));
        }
        self.transports.insert(transport_id, transport);
        self.session_transports
            .entry(session_id)
            .or_default()
            .insert(transport_id);
        Ok(())
    }

    /// Register a producer on a receive transport.
    ///
    /// Producers are source-side entities. The router enforces that they only
    /// live on receive transports so downstream state stays structurally valid.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the owning transport does not exist
    /// [`RouterError::ProducerRequiresReceiveTransport`] when the transport does not accept
    /// producers or [`RouterError::DuplicateProducer`] when the producer already exists.
    pub fn add_producer(&mut self, producer: Producer) -> Result<(), RouterError> {
        let producer_id = producer.id();
        let transport_id = producer.transport_id();
        let media_kind = producer.media_kind();
        let Some(transport) = self.transports.get(&transport_id) else {
            return Err(RouterError::MissingTransport(transport_id));
        };
        let session_id = transport.session_id();
        if transport.direction() != TransportDirection::Receive {
            return Err(RouterError::ProducerRequiresReceiveTransport(transport_id));
        }
        if self.producers.contains_key(&producer_id) {
            return Err(RouterError::DuplicateProducer(producer_id));
        }
        self.producers.insert(producer_id, producer);
        self.transport_producers
            .entry(transport_id)
            .or_default()
            .insert(producer_id);
        self.observer.on_event(RouterEvent::ProducerAdded {
            session_id,
            transport_id,
            producer_id,
            media_kind,
        });
        Ok(())
    }

    /// Register a consumer on a send transport.
    ///
    /// The `capability` parameter is the result of external capability
    /// negotiation such as [`crate::can_consume`]. The router
    /// treats it as an opaque compatibility gate. When
    /// [`ConsumerCapability::Incompatible`], the consumer is rejected without
    /// inspecting RTP parameters. This keeps the full RTP capability matching
    /// logic outside the router while still letting the router enforce the
    /// structural gate.
    ///
    /// The consumer's producer shadow is copied from the current producer route
    /// state before insertion. After this transition, source-side route changes
    /// must go through [`Router::set_producer_route_state`] so the shadow stays
    /// coherent on every dependent consumer.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the consumer transport does not exist,
    /// [`RouterError::MissingProducer`] when the target producer does not exist,
    /// [`RouterError::ConsumerRequiresSendTransport`] when the transport does not accept
    /// consumers, [`RouterError::IncompatibleCapabilities`] when the external capability
    /// negotiation determined that the consumer cannot consume the producer,
    /// [`RouterError::ConsumerMediaKindMismatch`] when the consumer metadata does not
    /// match its source producer,
    /// or [`RouterError::DuplicateConsumer`] when the consumer already exists.
    pub fn add_consumer(
        &mut self,
        mut consumer: Consumer,
        capability: ConsumerCapability,
    ) -> Result<(), RouterError> {
        let consumer_id = consumer.id();
        let transport_id = consumer.transport_id();
        let producer_id = consumer.producer_id();
        let Some(transport) = self.transports.get(&transport_id) else {
            return Err(RouterError::MissingTransport(transport_id));
        };
        if transport.direction() != TransportDirection::Send {
            return Err(RouterError::ConsumerRequiresSendTransport(transport_id));
        }
        let Some(producer) = self.producers.get(&producer_id) else {
            return Err(RouterError::MissingProducer(producer_id));
        };
        if !capability.is_compatible() {
            return Err(RouterError::IncompatibleCapabilities { producer_id });
        }
        if consumer.media_kind() != producer.media_kind() {
            return Err(RouterError::ConsumerMediaKindMismatch {
                producer_id,
                expected: producer.media_kind(),
                actual: consumer.media_kind(),
            });
        }
        if self.consumers.contains_key(&consumer_id) {
            return Err(RouterError::DuplicateConsumer(consumer_id));
        }
        consumer.set_producer_route_state(producer.route_state());
        self.consumers.insert(consumer_id, consumer);
        self.transport_consumers
            .entry(transport_id)
            .or_default()
            .insert(consumer_id);
        self.producer_consumers
            .entry(producer_id)
            .or_default()
            .insert(consumer_id);
        Ok(())
    }

    /// Set the source-side route state for a producer.
    ///
    /// This is the authoritative producer pause transition in the pure router.
    /// It mutates the producer and then updates the producer shadow stored on
    /// each dependent consumer. The consumer-local route state is deliberately
    /// left unchanged.
    ///
    /// The update cost is proportional to the number of consumers attached to
    /// the producer because the router uses the reverse producer-consumer index.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingProducer`] when the producer does not exist.
    pub fn set_producer_route_state(
        &mut self,
        producer_id: ProducerId,
        route_state: ProducerRouteState,
    ) -> Result<(), RouterError> {
        let Some(producer) = self.producers.get_mut(&producer_id) else {
            return Err(RouterError::MissingProducer(producer_id));
        };
        producer.set_route_state(route_state);

        let consumer_ids: Vec<_> = self
            .producer_consumers
            .get(&producer_id)
            .map_or_else(Vec::new, |ids| ids.iter().copied().collect());
        for consumer_id in consumer_ids {
            if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                consumer.set_producer_route_state(route_state);
            }
        }

        Ok(())
    }

    /// Set the receiver-local route state for a consumer.
    ///
    /// This only changes the consumer's own route state. It does not change the
    /// producer route state or any other consumer that depends on the same
    /// producer.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingConsumer`] when the consumer does not exist.
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

    /// # Errors
    ///
    /// Returns [`RouterError::MissingProducer`] when the producer does not exist.
    pub fn remove_producer(&mut self, producer_id: ProducerId) -> Result<(), RouterError> {
        let Some(producer) = self.producers.get(&producer_id).copied() else {
            return Err(RouterError::MissingProducer(producer_id));
        };
        let session_id = self
            .transports
            .get(&producer.transport_id())
            .map_or(SessionId(0), Transport::session_id);
        self.detach_producer(producer_id, session_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingConsumer`] when the consumer does not exist.
    pub fn remove_consumer(&mut self, consumer_id: ConsumerId) -> Result<(), RouterError> {
        if !self.consumers.contains_key(&consumer_id) {
            return Err(RouterError::MissingConsumer(consumer_id));
        }
        self.detach_consumer(consumer_id);
        Ok(())
    }

    /// Remove a session and cascade all dependent transports and media entities.
    ///
    /// This is the authoritative teardown path for session-owned router state.
    /// Reverse indexes guarantee that cleanup only walks the transports,
    /// producers and consumers that actually belong to the removed session.
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the session does not exist.
    pub fn remove_session(&mut self, session_id: SessionId) -> Result<(), RouterError> {
        let Some(mut session) = self.sessions.remove(&session_id) else {
            return Err(RouterError::MissingSession(session_id));
        };
        session.close();

        let transport_ids = Self::take_ids(&mut self.session_transports, &session_id);
        for transport_id in transport_ids {
            self.remove_transport(transport_id);
        }
        self.observer
            .on_event(RouterEvent::SessionLeft { session_id });

        Ok(())
    }

    fn remove_transport(&mut self, transport_id: TransportId) {
        let Some(transport) = self.transports.remove(&transport_id) else {
            return;
        };

        Self::remove_index_member(
            &mut self.session_transports,
            transport.session_id(),
            &transport_id,
        );

        let consumer_ids = Self::take_ids(&mut self.transport_consumers, &transport_id);
        for consumer_id in consumer_ids {
            self.detach_consumer(consumer_id);
        }

        let producer_ids = Self::take_ids(&mut self.transport_producers, &transport_id);
        for producer_id in producer_ids {
            self.detach_producer(producer_id, transport.session_id());
        }
    }

    fn detach_producer(&mut self, producer_id: ProducerId, session_id: SessionId) {
        let Some(producer) = self.producers.remove(&producer_id) else {
            return;
        };
        let transport_id = producer.transport_id();

        Self::remove_index_member(&mut self.transport_producers, transport_id, &producer_id);

        let consumer_ids = Self::take_ids(&mut self.producer_consumers, &producer_id);
        for consumer_id in consumer_ids {
            self.detach_consumer(consumer_id);
        }
        self.observer.on_event(RouterEvent::ProducerRemoved {
            session_id,
            transport_id,
            producer_id,
            media_kind: producer.media_kind(),
        });
    }

    fn detach_consumer(&mut self, consumer_id: ConsumerId) {
        let Some(consumer) = self.consumers.remove(&consumer_id) else {
            return;
        };

        Self::remove_index_member(
            &mut self.transport_consumers,
            consumer.transport_id(),
            &consumer_id,
        );
        Self::remove_index_member(
            &mut self.producer_consumers,
            consumer.producer_id(),
            &consumer_id,
        );
    }

    fn take_ids<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K) -> Vec<V>
    where
        K: Ord,
        V: Ord,
    {
        index
            .remove(key)
            .map_or_else(Vec::new, |ids| ids.into_iter().collect())
    }

    fn remove_index_member<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: K, value: &V)
    where
        K: Copy + Ord,
        V: Ord,
    {
        let should_remove_key = index.get_mut(&key).is_some_and(|values| {
            values.remove(value);
            values.is_empty()
        });

        if should_remove_key {
            index.remove(&key);
        }
    }
}
