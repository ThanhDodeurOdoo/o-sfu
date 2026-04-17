use super::super::{
    Consumer, ConsumerId, Producer, ProducerId, RouterError, RouterId, Session, SessionId,
    Transport, TransportDirection, TransportId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceKind {
    Session,
    Transport,
    Producer,
    Consumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProofRouterError {
    Router(RouterError),
    CapacityExceeded(ResourceKind),
}

impl From<RouterError> for ProofRouterError {
    fn from(err: RouterError) -> Self {
        Self::Router(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProofMembershipEntry<K, V, const MAX_VALUES: usize> {
    pub(super) key: K,
    pub(super) values: [Option<V>; MAX_VALUES],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProofMembershipIndex<K, V, const MAX_KEYS: usize, const MAX_VALUES: usize> {
    pub(super) entries: [Option<ProofMembershipEntry<K, V, MAX_VALUES>>; MAX_KEYS],
}

impl<K: Copy + Eq, V: Copy + Eq, const MAX_KEYS: usize, const MAX_VALUES: usize>
    ProofMembershipIndex<K, V, MAX_KEYS, MAX_VALUES>
{
    fn new() -> Self {
        Self {
            entries: [None; MAX_KEYS],
        }
    }

    fn insert(
        &mut self,
        key: K,
        value: V,
        value_kind: ResourceKind,
    ) -> Result<(), ProofRouterError> {
        let mut entry_index = 0;
        while let Some(slot) = self.entries.get_mut(entry_index) {
            if let Some(entry) = slot.as_mut()
                && entry.key == key
            {
                let mut value_index = 0;
                while let Some(value_slot) = entry.values.get_mut(value_index) {
                    match *value_slot {
                        Some(current) if current == value => return Ok(()),
                        None => {
                            *value_slot = Some(value);
                            return Ok(());
                        }
                        Some(_) => {}
                    }
                    value_index += 1;
                }
                return Err(ProofRouterError::CapacityExceeded(value_kind));
            }
            entry_index += 1;
        }

        let mut entry_index = 0;
        while let Some(slot) = self.entries.get_mut(entry_index) {
            if slot.is_none() {
                let mut values = [None; MAX_VALUES];
                values[0] = Some(value);
                *slot = Some(ProofMembershipEntry { key, values });
                return Ok(());
            }
            entry_index += 1;
        }

        Err(ProofRouterError::CapacityExceeded(value_kind))
    }

    pub(super) fn contains_key(&self, key: K) -> bool {
        let mut entry_index = 0;
        while let Some(slot) = self.entries.get(entry_index) {
            if slot.is_some_and(|entry| entry.key == key) {
                return true;
            }
            entry_index += 1;
        }
        false
    }

    pub(super) fn contains_member(&self, key: K, value: V) -> bool {
        let mut entry_index = 0;
        while let Some(slot) = self.entries.get(entry_index) {
            if let Some(entry) = *slot
                && entry.key == key
            {
                let mut value_index = 0;
                while let Some(value_slot) = entry.values.get(value_index) {
                    if value_slot.is_some_and(|current| current == value) {
                        return true;
                    }
                    value_index += 1;
                }
                return false;
            }
            entry_index += 1;
        }
        false
    }

    pub(super) fn member_count(&self, key: K) -> usize {
        let mut entry_index = 0;
        while let Some(slot) = self.entries.get(entry_index) {
            if let Some(entry) = *slot
                && entry.key == key
            {
                let mut count = 0;
                let mut value_index = 0;
                while let Some(value_slot) = entry.values.get(value_index) {
                    if value_slot.is_some() {
                        count += 1;
                    }
                    value_index += 1;
                }
                return count;
            }
            entry_index += 1;
        }
        0
    }

    fn remove_member(&mut self, key: K, value: V) {
        let mut entry_index = 0;
        while let Some(slot) = self.entries.get_mut(entry_index) {
            if let Some(entry) = slot.as_mut()
                && entry.key == key
            {
                let mut has_live_member = false;
                let mut value_index = 0;
                while let Some(value_slot) = entry.values.get_mut(value_index) {
                    if value_slot.is_some_and(|current| current == value) {
                        *value_slot = None;
                    }
                    if value_slot.is_some() {
                        has_live_member = true;
                    }
                    value_index += 1;
                }
                if !has_live_member {
                    *slot = None;
                }
                return;
            }
            entry_index += 1;
        }
    }

    fn take_members(&mut self, key: K) -> [Option<V>; MAX_VALUES] {
        let mut entry_index = 0;
        while let Some(slot) = self.entries.get_mut(entry_index) {
            if slot.is_some_and(|entry| entry.key == key) {
                let values = slot
                    .as_ref()
                    .map_or([None; MAX_VALUES], |entry| entry.values);
                *slot = None;
                return values;
            }
            entry_index += 1;
        }
        [None; MAX_VALUES]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProofRouterModel<
    const MAX_SESSIONS: usize,
    const MAX_TRANSPORTS: usize,
    const MAX_PRODUCERS: usize,
    const MAX_CONSUMERS: usize,
> {
    pub(super) id: RouterId,
    pub(super) sessions: [Option<Session>; MAX_SESSIONS],
    pub(super) transports: [Option<Transport>; MAX_TRANSPORTS],
    pub(super) producers: [Option<Producer>; MAX_PRODUCERS],
    pub(super) consumers: [Option<Consumer>; MAX_CONSUMERS],
    pub(super) session_transports:
        ProofMembershipIndex<SessionId, TransportId, MAX_SESSIONS, MAX_TRANSPORTS>,
    pub(super) transport_producers:
        ProofMembershipIndex<TransportId, ProducerId, MAX_TRANSPORTS, MAX_PRODUCERS>,
    pub(super) transport_consumers:
        ProofMembershipIndex<TransportId, ConsumerId, MAX_TRANSPORTS, MAX_CONSUMERS>,
    pub(super) producer_consumers:
        ProofMembershipIndex<ProducerId, ConsumerId, MAX_PRODUCERS, MAX_CONSUMERS>,
}

impl<
    const MAX_SESSIONS: usize,
    const MAX_TRANSPORTS: usize,
    const MAX_PRODUCERS: usize,
    const MAX_CONSUMERS: usize,
> ProofRouterModel<MAX_SESSIONS, MAX_TRANSPORTS, MAX_PRODUCERS, MAX_CONSUMERS>
{
    #[must_use]
    pub(crate) fn new(id: RouterId) -> Self {
        Self {
            id,
            sessions: [None; MAX_SESSIONS],
            transports: [None; MAX_TRANSPORTS],
            producers: [None; MAX_PRODUCERS],
            consumers: [None; MAX_CONSUMERS],
            session_transports: ProofMembershipIndex::new(),
            transport_producers: ProofMembershipIndex::new(),
            transport_consumers: ProofMembershipIndex::new(),
            producer_consumers: ProofMembershipIndex::new(),
        }
    }

    /// # Errors
    ///
    /// Returns [`RouterError::DuplicateSession`] when the session already exists,
    /// or [`ProofRouterError::CapacityExceeded`] when the proof model has no free slot.
    pub(crate) fn join_session(&mut self, session: Session) -> Result<(), ProofRouterError> {
        if self.contains_session(session.id()) {
            return Err(RouterError::DuplicateSession(session.id()).into());
        }
        self.insert_session(session)
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the session does not exist.
    pub(crate) fn update_session_permissions(
        &mut self,
        session_id: SessionId,
        permissions: crate::SessionPermissions,
    ) -> Result<(), ProofRouterError> {
        let Some(session) = self.session_by_id_mut(session_id) else {
            return Err(RouterError::MissingSession(session_id).into());
        };
        session.set_permissions(permissions);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the owning session does not exist,
    /// [`RouterError::DuplicateTransport`] when the transport already exists,
    /// or [`ProofRouterError::CapacityExceeded`] when the proof model has no free slot.
    pub(crate) fn open_transport(&mut self, transport: Transport) -> Result<(), ProofRouterError> {
        if !self.contains_session(transport.session_id()) {
            return Err(RouterError::MissingSession(transport.session_id()).into());
        }
        if self.contains_transport(transport.id()) {
            return Err(RouterError::DuplicateTransport(transport.id()).into());
        }
        self.insert_transport(transport)?;
        self.session_transports.insert(
            transport.session_id(),
            transport.id(),
            ResourceKind::Transport,
        )
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the owning transport does not exist,
    /// [`RouterError::ProducerRequiresReceiveTransport`] when the transport does not accept
    /// producers, [`RouterError::DuplicateProducer`] when the producer already exists,
    /// or [`ProofRouterError::CapacityExceeded`] when the proof model has no free slot.
    pub(crate) fn add_producer(&mut self, producer: Producer) -> Result<(), ProofRouterError> {
        let Some(transport) = self.transport_by_id(producer.transport_id()) else {
            return Err(RouterError::MissingTransport(producer.transport_id()).into());
        };
        if transport.direction() != TransportDirection::Receive {
            return Err(
                RouterError::ProducerRequiresReceiveTransport(producer.transport_id()).into(),
            );
        }
        if self.contains_producer(producer.id()) {
            return Err(RouterError::DuplicateProducer(producer.id()).into());
        }
        self.insert_producer(producer)?;
        self.transport_producers.insert(
            producer.transport_id(),
            producer.id(),
            ResourceKind::Producer,
        )
    }

    /// The `capability` parameter abstracts the external capability negotiation as a semantic
    /// compatibility gate. [`crate::ConsumerCapability::Incompatible`] is rejected with
    /// [`RouterError::IncompatibleCapabilities`].
    ///
    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the consumer transport does not exist,
    /// [`RouterError::MissingProducer`] when the target producer does not exist,
    /// [`RouterError::ConsumerRequiresSendTransport`] when the transport does not accept
    /// consumers, [`RouterError::IncompatibleCapabilities`] when `capability` is
    /// [`crate::ConsumerCapability::Incompatible`],
    /// [`RouterError::ConsumerMediaKindMismatch`] when the consumer metadata does not
    /// match its source producer, [`RouterError::ConsumerStreamTypeMismatch`] when the consumer
    /// stream type does not match its source producer,
    /// [`RouterError::DuplicateConsumer`] when the consumer already exists,
    /// or [`ProofRouterError::CapacityExceeded`] when the proof model has no free slot.
    pub(crate) fn add_consumer(
        &mut self,
        mut consumer: Consumer,
        capability: crate::ConsumerCapability,
    ) -> Result<(), ProofRouterError> {
        let Some(transport) = self.transport_by_id(consumer.transport_id()) else {
            return Err(RouterError::MissingTransport(consumer.transport_id()).into());
        };
        if transport.direction() != TransportDirection::Send {
            return Err(RouterError::ConsumerRequiresSendTransport(consumer.transport_id()).into());
        }
        let Some(producer) = self.producer_by_id(consumer.producer_id()) else {
            return Err(RouterError::MissingProducer(consumer.producer_id()).into());
        };
        if !capability.is_compatible() {
            return Err(RouterError::IncompatibleCapabilities {
                producer_id: consumer.producer_id(),
            }
            .into());
        }
        if consumer.media_kind() != producer.media_kind() {
            return Err(RouterError::ConsumerMediaKindMismatch {
                producer_id: consumer.producer_id(),
                expected: producer.media_kind(),
                actual: consumer.media_kind(),
            }
            .into());
        }
        if consumer.stream_type() != producer.stream_type() {
            return Err(RouterError::ConsumerStreamTypeMismatch {
                producer_id: consumer.producer_id(),
                expected: producer.stream_type(),
                actual: consumer.stream_type(),
            }
            .into());
        }
        if self.contains_consumer(consumer.id()) {
            return Err(RouterError::DuplicateConsumer(consumer.id()).into());
        }
        consumer.set_producer_paused(producer.paused());
        self.insert_consumer(consumer)?;
        self.transport_consumers.insert(
            consumer.transport_id(),
            consumer.id(),
            ResourceKind::Consumer,
        )?;
        self.producer_consumers.insert(
            consumer.producer_id(),
            consumer.id(),
            ResourceKind::Consumer,
        )
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingProducer`] when the producer does not exist.
    pub(crate) fn set_producer_paused(
        &mut self,
        producer_id: ProducerId,
        paused: bool,
    ) -> Result<(), ProofRouterError> {
        let Some(producer) = self.producer_by_id_mut(producer_id) else {
            return Err(RouterError::MissingProducer(producer_id).into());
        };
        producer.set_paused(paused);

        for consumer in &mut self.consumers {
            if let Some(consumer) = consumer.as_mut()
                && consumer.producer_id() == producer_id
            {
                consumer.set_producer_paused(paused);
            }
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingConsumer`] when the consumer does not exist.
    pub(crate) fn set_consumer_paused(
        &mut self,
        consumer_id: ConsumerId,
        paused: bool,
    ) -> Result<(), ProofRouterError> {
        let Some(consumer) = self.consumer_by_id_mut(consumer_id) else {
            return Err(RouterError::MissingConsumer(consumer_id).into());
        };
        consumer.set_paused(paused);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingProducer`] when the producer does not exist.
    pub(crate) fn remove_producer(
        &mut self,
        producer_id: ProducerId,
    ) -> Result<(), ProofRouterError> {
        if !self.contains_producer(producer_id) {
            return Err(RouterError::MissingProducer(producer_id).into());
        }
        self.detach_producer(producer_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingConsumer`] when the consumer does not exist.
    pub(crate) fn remove_consumer(
        &mut self,
        consumer_id: ConsumerId,
    ) -> Result<(), ProofRouterError> {
        if !self.contains_consumer(consumer_id) {
            return Err(RouterError::MissingConsumer(consumer_id).into());
        }
        self.detach_consumer(consumer_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the session does not exist.
    pub(crate) fn remove_session(&mut self, session_id: SessionId) -> Result<(), ProofRouterError> {
        if !self.contains_session(session_id) {
            return Err(RouterError::MissingSession(session_id).into());
        }

        self.clear_session(session_id);
        let transport_ids = self.session_transports.take_members(session_id);
        let mut transport_index = 0;
        while let Some(transport_id) = transport_ids.get(transport_index) {
            if let Some(transport_id) = *transport_id {
                self.remove_transport(transport_id);
            }
            transport_index += 1;
        }

        Ok(())
    }

    fn insert_session(&mut self, session: Session) -> Result<(), ProofRouterError> {
        for slot in &mut self.sessions {
            if slot.is_none() {
                *slot = Some(session);
                return Ok(());
            }
        }
        Err(ProofRouterError::CapacityExceeded(ResourceKind::Session))
    }

    fn insert_transport(&mut self, transport: Transport) -> Result<(), ProofRouterError> {
        for slot in &mut self.transports {
            if slot.is_none() {
                *slot = Some(transport);
                return Ok(());
            }
        }
        Err(ProofRouterError::CapacityExceeded(ResourceKind::Transport))
    }

    fn insert_producer(&mut self, producer: Producer) -> Result<(), ProofRouterError> {
        for slot in &mut self.producers {
            if slot.is_none() {
                *slot = Some(producer);
                return Ok(());
            }
        }
        Err(ProofRouterError::CapacityExceeded(ResourceKind::Producer))
    }

    fn insert_consumer(&mut self, consumer: Consumer) -> Result<(), ProofRouterError> {
        for slot in &mut self.consumers {
            if slot.is_none() {
                *slot = Some(consumer);
                return Ok(());
            }
        }
        Err(ProofRouterError::CapacityExceeded(ResourceKind::Consumer))
    }

    fn remove_transport(&mut self, transport_id: TransportId) {
        let Some(transport) = self.transport_by_id(transport_id) else {
            return;
        };

        self.clear_transport(transport_id);
        self.session_transports
            .remove_member(transport.session_id(), transport_id);

        let consumer_ids = self.transport_consumers.take_members(transport_id);
        let mut consumer_index = 0;
        while let Some(consumer_id) = consumer_ids.get(consumer_index) {
            if let Some(consumer_id) = *consumer_id {
                self.detach_consumer(consumer_id);
            }
            consumer_index += 1;
        }

        let producer_ids = self.transport_producers.take_members(transport_id);
        let mut producer_index = 0;
        while let Some(producer_id) = producer_ids.get(producer_index) {
            if let Some(producer_id) = *producer_id {
                self.detach_producer(producer_id);
            }
            producer_index += 1;
        }
    }

    fn detach_producer(&mut self, producer_id: ProducerId) {
        let Some(producer) = self.producer_by_id(producer_id) else {
            return;
        };

        self.clear_producer(producer_id);
        self.transport_producers
            .remove_member(producer.transport_id(), producer_id);

        let consumer_ids = self.producer_consumers.take_members(producer_id);
        let mut consumer_index = 0;
        while let Some(consumer_id) = consumer_ids.get(consumer_index) {
            if let Some(consumer_id) = *consumer_id {
                self.detach_consumer(consumer_id);
            }
            consumer_index += 1;
        }
    }

    fn detach_consumer(&mut self, consumer_id: ConsumerId) {
        let Some(consumer) = self.consumer_by_id(consumer_id) else {
            return;
        };

        self.clear_consumer(consumer_id);
        self.transport_consumers
            .remove_member(consumer.transport_id(), consumer_id);
        self.producer_consumers
            .remove_member(consumer.producer_id(), consumer_id);
    }

    fn clear_session(&mut self, session_id: SessionId) {
        for slot in &mut self.sessions {
            if slot.is_some_and(|session| session.id() == session_id) {
                *slot = None;
            }
        }
    }

    fn clear_transport(&mut self, transport_id: TransportId) {
        for slot in &mut self.transports {
            if slot.is_some_and(|transport| transport.id() == transport_id) {
                *slot = None;
            }
        }
    }

    fn clear_producer(&mut self, producer_id: ProducerId) {
        for slot in &mut self.producers {
            if slot.is_some_and(|producer| producer.id() == producer_id) {
                *slot = None;
            }
        }
    }

    fn clear_consumer(&mut self, consumer_id: ConsumerId) {
        for slot in &mut self.consumers {
            if slot.is_some_and(|consumer| consumer.id() == consumer_id) {
                *slot = None;
            }
        }
    }

    fn producer_by_id_mut(&mut self, producer_id: ProducerId) -> Option<&mut Producer> {
        self.producers.iter_mut().find_map(|slot| {
            slot.as_mut()
                .filter(|producer| producer.id() == producer_id)
        })
    }

    fn consumer_by_id_mut(&mut self, consumer_id: ConsumerId) -> Option<&mut Consumer> {
        self.consumers.iter_mut().find_map(|slot| {
            slot.as_mut()
                .filter(|consumer| consumer.id() == consumer_id)
        })
    }

    fn session_by_id_mut(&mut self, session_id: SessionId) -> Option<&mut Session> {
        self.sessions
            .iter_mut()
            .find_map(|slot| slot.as_mut().filter(|session| session.id() == session_id))
    }

    pub(super) fn contains_session(&self, session_id: SessionId) -> bool {
        for session in &self.sessions {
            if session.is_some_and(|session| session.id() == session_id) {
                return true;
            }
        }
        false
    }

    pub(super) fn contains_transport(&self, transport_id: TransportId) -> bool {
        for transport in &self.transports {
            if transport.is_some_and(|transport| transport.id() == transport_id) {
                return true;
            }
        }
        false
    }

    pub(super) fn transport_by_id(&self, transport_id: TransportId) -> Option<Transport> {
        for transport in &self.transports {
            if transport.is_some_and(|transport| transport.id() == transport_id) {
                return *transport;
            }
        }
        None
    }

    pub(super) fn contains_producer(&self, producer_id: ProducerId) -> bool {
        for producer in &self.producers {
            if producer.is_some_and(|producer| producer.id() == producer_id) {
                return true;
            }
        }
        false
    }

    pub(super) fn producer_by_id(&self, producer_id: ProducerId) -> Option<Producer> {
        for producer in &self.producers {
            if producer.is_some_and(|producer| producer.id() == producer_id) {
                return *producer;
            }
        }
        None
    }

    pub(super) fn consumer_by_id(&self, consumer_id: ConsumerId) -> Option<Consumer> {
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| consumer.id() == consumer_id) {
                return *consumer;
            }
        }
        None
    }

    fn contains_consumer(&self, consumer_id: ConsumerId) -> bool {
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| consumer.id() == consumer_id) {
                return true;
            }
        }
        false
    }
}
