use std::collections::{BTreeMap, BTreeSet};

use super::{
    Consumer, ConsumerId, Producer, ProducerId, RouterError, RouterId, Session, SessionId,
    Transport, TransportDirection, TransportId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Router {
    pub(super) id: RouterId,
    pub(super) sessions: BTreeMap<SessionId, Session>,
    pub(super) transports: BTreeMap<TransportId, Transport>,
    pub(super) producers: BTreeMap<ProducerId, Producer>,
    pub(super) consumers: BTreeMap<ConsumerId, Consumer>,
    pub(super) session_transports: BTreeMap<SessionId, BTreeSet<TransportId>>,
    pub(super) transport_producers: BTreeMap<TransportId, BTreeSet<ProducerId>>,
    pub(super) transport_consumers: BTreeMap<TransportId, BTreeSet<ConsumerId>>,
    pub(super) producer_consumers: BTreeMap<ProducerId, BTreeSet<ConsumerId>>,
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
            session_transports: BTreeMap::new(),
            transport_producers: BTreeMap::new(),
            transport_consumers: BTreeMap::new(),
            producer_consumers: BTreeMap::new(),
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

    /// # Errors
    ///
    /// Returns [`RouterError::DuplicateSession`] when the session already exists.
    pub fn join_session(&mut self, session: Session) -> Result<(), RouterError> {
        let session_id = session.id();
        if self.sessions.contains_key(&session_id) {
            return Err(RouterError::DuplicateSession(session_id));
        }
        self.sessions.insert(session_id, session);
        Ok(())
    }

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

    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the owning transport does not exist
    /// [`RouterError::ProducerRequiresReceiveTransport`] when the transport does not accept
    /// producers, or [`RouterError::DuplicateProducer`] when the producer already exists.
    pub fn add_producer(&mut self, producer: Producer) -> Result<(), RouterError> {
        let producer_id = producer.id();
        let transport_id = producer.transport_id();
        let Some(transport) = self.transports.get(&transport_id) else {
            return Err(RouterError::MissingTransport(transport_id));
        };
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
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the consumer transport does not exist,
    /// [`RouterError::MissingProducer`] when the target producer does not exist,
    /// [`RouterError::ConsumerRequiresSendTransport`] when the transport does not accept
    /// consumers, [`RouterError::ConsumerMediaKindMismatch`] when the consumer metadata does not
    /// match its source producer, [`RouterError::ConsumerStreamTypeMismatch`] when the consumer
    /// stream type does not match its source producer,
    /// or [`RouterError::DuplicateConsumer`] when the consumer already exists.
    pub fn add_consumer(&mut self, consumer: Consumer) -> Result<(), RouterError> {
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
        if consumer.media_kind() != producer.media_kind() {
            return Err(RouterError::ConsumerMediaKindMismatch {
                producer_id,
                expected: producer.media_kind(),
                actual: consumer.media_kind(),
            });
        }
        if consumer.stream_type() != producer.stream_type() {
            return Err(RouterError::ConsumerStreamTypeMismatch {
                producer_id,
                expected: producer.stream_type(),
                actual: consumer.stream_type(),
            });
        }
        if self.consumers.contains_key(&consumer_id) {
            return Err(RouterError::DuplicateConsumer(consumer_id));
        }
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

    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the session does not exist.
    pub fn remove_session(&mut self, session_id: SessionId) -> Result<(), RouterError> {
        if self.sessions.remove(&session_id).is_none() {
            return Err(RouterError::MissingSession(session_id));
        }

        let transport_ids = Self::take_ids(&mut self.session_transports, &session_id);
        for transport_id in transport_ids {
            self.remove_transport(transport_id);
        }

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
            self.remove_consumer(consumer_id);
        }

        let producer_ids = Self::take_ids(&mut self.transport_producers, &transport_id);
        for producer_id in producer_ids {
            self.remove_producer(producer_id);
        }
    }

    fn remove_producer(&mut self, producer_id: ProducerId) {
        let Some(producer) = self.producers.remove(&producer_id) else {
            return;
        };

        Self::remove_index_member(
            &mut self.transport_producers,
            producer.transport_id(),
            &producer_id,
        );

        let consumer_ids = Self::take_ids(&mut self.producer_consumers, &producer_id);
        for consumer_id in consumer_ids {
            self.remove_consumer(consumer_id);
        }
    }

    fn remove_consumer(&mut self, consumer_id: ConsumerId) {
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
