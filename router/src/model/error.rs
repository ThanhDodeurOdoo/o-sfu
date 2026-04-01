use super::{ConsumerId, ProducerId, SessionId, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterError {
    DuplicateSession(SessionId),
    DuplicateTransport(TransportId),
    DuplicateProducer(ProducerId),
    DuplicateConsumer(ConsumerId),
    MissingSession(SessionId),
    MissingTransport(TransportId),
    MissingProducer(ProducerId),
    MissingConsumer(ConsumerId),
}
