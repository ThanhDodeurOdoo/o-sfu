use super::super::{
    Consumer, ConsumerId, Producer, ProducerId, RouterError, RouterId, Session, SessionId,
    Transport, TransportId,
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
        self.insert_transport(transport)
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the owning transport does not exist,
    /// [`RouterError::DuplicateProducer`] when the producer already exists,
    /// or [`ProofRouterError::CapacityExceeded`] when the proof model has no free slot.
    pub(crate) fn add_producer(&mut self, producer: Producer) -> Result<(), ProofRouterError> {
        if !self.contains_transport(producer.transport_id()) {
            return Err(RouterError::MissingTransport(producer.transport_id()).into());
        }
        if self.contains_producer(producer.id()) {
            return Err(RouterError::DuplicateProducer(producer.id()).into());
        }
        self.insert_producer(producer)
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingTransport`] when the consumer transport does not exist,
    /// [`RouterError::MissingProducer`] when the target producer does not exist,
    /// [`RouterError::DuplicateConsumer`] when the consumer already exists,
    /// or [`ProofRouterError::CapacityExceeded`] when the proof model has no free slot.
    pub(crate) fn add_consumer(&mut self, consumer: Consumer) -> Result<(), ProofRouterError> {
        if !self.contains_transport(consumer.transport_id()) {
            return Err(RouterError::MissingTransport(consumer.transport_id()).into());
        }
        if !self.contains_producer(consumer.producer_id()) {
            return Err(RouterError::MissingProducer(consumer.producer_id()).into());
        }
        if self.contains_consumer(consumer.id()) {
            return Err(RouterError::DuplicateConsumer(consumer.id()).into());
        }
        self.insert_consumer(consumer)
    }

    /// # Errors
    ///
    /// Returns [`RouterError::MissingSession`] when the session does not exist.
    pub(crate) fn remove_session(&mut self, session_id: SessionId) -> Result<(), ProofRouterError> {
        if !self.contains_session(session_id) {
            return Err(RouterError::MissingSession(session_id).into());
        }

        self.clear_session(session_id);
        let removed_transport_ids = self.clear_transports_for_session(session_id);
        let removed_producer_ids = self.clear_producers_for_transports(&removed_transport_ids);
        self.clear_consumers_for_dependencies(&removed_transport_ids, &removed_producer_ids);

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

    fn clear_session(&mut self, session_id: SessionId) {
        for slot in &mut self.sessions {
            if slot.is_some_and(|session| session.id() == session_id) {
                *slot = None;
            }
        }
    }

    fn clear_transports_for_session(
        &mut self,
        session_id: SessionId,
    ) -> [Option<TransportId>; MAX_TRANSPORTS] {
        let mut removed = [None; MAX_TRANSPORTS];
        for slot in &mut self.transports {
            if slot.is_some_and(|transport| transport.session_id() == session_id) {
                if let Some(transport) = *slot {
                    Self::push_removed_id(&mut removed, transport.id());
                }
                *slot = None;
            }
        }
        removed
    }

    fn clear_producers_for_transports(
        &mut self,
        removed_transport_ids: &[Option<TransportId>; MAX_TRANSPORTS],
    ) -> [Option<ProducerId>; MAX_PRODUCERS] {
        let mut removed = [None; MAX_PRODUCERS];
        for slot in &mut self.producers {
            if slot.is_some_and(|producer| {
                Self::removed_id_contains(removed_transport_ids, producer.transport_id())
            }) {
                if let Some(producer) = *slot {
                    Self::push_removed_id(&mut removed, producer.id());
                }
                *slot = None;
            }
        }
        removed
    }

    fn clear_consumers_for_dependencies(
        &mut self,
        removed_transport_ids: &[Option<TransportId>; MAX_TRANSPORTS],
        removed_producer_ids: &[Option<ProducerId>; MAX_PRODUCERS],
    ) {
        for slot in &mut self.consumers {
            if slot.is_some_and(|consumer| {
                Self::removed_id_contains(removed_transport_ids, consumer.transport_id())
                    || Self::removed_id_contains(removed_producer_ids, consumer.producer_id())
            }) {
                *slot = None;
            }
        }
    }

    fn push_removed_id<T: Copy>(removed_ids: &mut [Option<T>], value: T) {
        for slot in removed_ids {
            if slot.is_none() {
                *slot = Some(value);
                return;
            }
        }
    }

    fn removed_id_contains<T: Copy + Eq>(removed_ids: &[Option<T>], value: T) -> bool {
        for current in removed_ids {
            if current.is_some_and(|current| current == value) {
                return true;
            }
        }
        false
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

    pub(super) fn contains_producer(&self, producer_id: ProducerId) -> bool {
        for producer in &self.producers {
            if producer.is_some_and(|producer| producer.id() == producer_id) {
                return true;
            }
        }
        false
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
