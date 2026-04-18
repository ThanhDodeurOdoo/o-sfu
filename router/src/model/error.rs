use super::{ConsumerId, MediaKind, ProducerId, SessionId, StreamType, TransportId};

/// Router mutation errors
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
    ProducerRequiresReceiveTransport(TransportId),
    ConsumerRequiresSendTransport(TransportId),
    ConsumerMediaKindMismatch {
        producer_id: ProducerId,
        expected: MediaKind,
        actual: MediaKind,
    },
    ConsumerStreamTypeMismatch {
        producer_id: ProducerId,
        expected: StreamType,
        actual: StreamType,
    },
    IncompatibleCapabilities {
        producer_id: ProducerId,
    },
}
