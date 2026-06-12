use super::{ConsumerId, ProducerId, SessionId, TransportId};

/// Router mutation errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    #[error("duplicate session {0:?}")]
    DuplicateSession(SessionId),
    #[error("duplicate transport {0:?}")]
    DuplicateTransport(TransportId),
    #[error("duplicate producer {0:?}")]
    DuplicateProducer(ProducerId),
    #[error("duplicate consumer {0:?}")]
    DuplicateConsumer(ConsumerId),
    #[error("missing session {0:?}")]
    MissingSession(SessionId),
    #[error("missing transport {0:?}")]
    MissingTransport(TransportId),
    #[error("missing producer {0:?}")]
    MissingProducer(ProducerId),
    #[error("missing transport {transport_id:?} for producer {producer_id:?}")]
    MissingProducerTransport {
        producer_id: ProducerId,
        transport_id: TransportId,
    },
    #[error("missing consumer {0:?}")]
    MissingConsumer(ConsumerId),
    #[error("producer transport {0:?} is not a receive transport")]
    ProducerRequiresReceiveTransport(TransportId),
    #[error("consumer transport {0:?} is not a send transport")]
    ConsumerRequiresSendTransport(TransportId),
    #[error("consumer capabilities are incompatible with producer {producer_id:?}")]
    IncompatibleCapabilities { producer_id: ProducerId },
}
