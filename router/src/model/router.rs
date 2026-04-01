use std::collections::{BTreeMap, BTreeSet};

use super::{
    Consumer, ConsumerId, Producer, ProducerId, ResourceKind, RouterError, RouterId, Session,
    SessionId, Transport, TransportId,
};

const MAX_SESSIONS: usize = 8;
const MAX_TRANSPORTS: usize = 16;
const MAX_PRODUCERS: usize = 16;
const MAX_CONSUMERS: usize = 32;

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

    /// # Errors
    ///
    /// Returns [`RouterError::DuplicateSession`] when the session already exists,
    /// or [`RouterError::CapacityExceeded`] when the router has no free session slot.
    pub fn join_session(&mut self, session: Session) -> Result<(), RouterError> {
        let session_id = session.id();
        if self.sessions.contains_key(&session_id) {
            return Err(RouterError::DuplicateSession(session_id));
        }
        Self::ensure_capacity(self.sessions.len(), MAX_SESSIONS, ResourceKind::Session)?;
        self.sessions.insert(session_id, session);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the owning session does not exist,
    /// [`RouterError::DuplicateTransport`] when the transport already exists,
    /// or [`RouterError::CapacityExceeded`] when the router has no free transport slot.
    pub fn open_transport(&mut self, transport: Transport) -> Result<(), RouterError> {
        let transport_id = transport.id();
        let session_id = transport.session_id();
        if !self.sessions.contains_key(&session_id) {
            return Err(RouterError::MissingSession(session_id));
        }
        if self.transports.contains_key(&transport_id) {
            return Err(RouterError::DuplicateTransport(transport_id));
        }
        Self::ensure_capacity(
            self.transports.len(),
            MAX_TRANSPORTS,
            ResourceKind::Transport,
        )?;

        self.transports.insert(transport_id, transport);
        self.session_transports
            .entry(session_id)
            .or_default()
            .insert(transport_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the owning transport does not exist,
    /// [`RouterError::DuplicateProducer`] when the producer already exists,
    /// or [`RouterError::CapacityExceeded`] when the router has no free producer slot.
    pub fn add_producer(&mut self, producer: Producer) -> Result<(), RouterError> {
        let producer_id = producer.id();
        let transport_id = producer.transport_id();
        if !self.transports.contains_key(&transport_id) {
            return Err(RouterError::MissingTransport(transport_id));
        }
        if self.producers.contains_key(&producer_id) {
            return Err(RouterError::DuplicateProducer(producer_id));
        }
        Self::ensure_capacity(self.producers.len(), MAX_PRODUCERS, ResourceKind::Producer)?;

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
    /// [`RouterError::DuplicateConsumer`] when the consumer already exists,
    /// or [`RouterError::CapacityExceeded`] when the router has no free consumer slot.
    pub fn add_consumer(&mut self, consumer: Consumer) -> Result<(), RouterError> {
        let consumer_id = consumer.id();
        let transport_id = consumer.transport_id();
        let producer_id = consumer.producer_id();
        if !self.transports.contains_key(&transport_id) {
            return Err(RouterError::MissingTransport(transport_id));
        }
        if !self.producers.contains_key(&producer_id) {
            return Err(RouterError::MissingProducer(producer_id));
        }
        if self.consumers.contains_key(&consumer_id) {
            return Err(RouterError::DuplicateConsumer(consumer_id));
        }
        Self::ensure_capacity(self.consumers.len(), MAX_CONSUMERS, ResourceKind::Consumer)?;

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

    fn ensure_capacity(
        current_len: usize,
        max_len: usize,
        kind: ResourceKind,
    ) -> Result<(), RouterError> {
        if current_len >= max_len {
            return Err(RouterError::CapacityExceeded(kind));
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
