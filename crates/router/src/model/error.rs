use o_sfu_model::UserId;

use super::{
    ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId,
    topology::{RoutedConsumerId, RoutedProducerId},
};

/// router mutation errors
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    #[error("primary router {actual:?} does not match {expected:?}")]
    PrimaryRouterMismatch {
        expected: RouterId,
        actual: RouterId,
    },
    #[error("router {router:?} is assigned to media worker {expected:?}, not {actual:?}")]
    MediaWorkerMismatch {
        router: RouterId,
        expected: MediaWorkerId,
        actual: MediaWorkerId,
    },
    #[error("connection {0:?} is already committed")]
    DuplicateConnection(ConnectionId),
    #[error("duplicate producer {0:?}")]
    DuplicateProducer(ProducerId),
    #[error("duplicate consumer {0:?}")]
    DuplicateConsumer(ConsumerId),
    #[error("missing session for {0:?}")]
    MissingSession(UserId),
    #[error("missing router {0:?}")]
    MissingRouter(RouterId),
    #[error("missing producer {0:?}")]
    MissingProducer(RoutedProducerId),
    #[error("missing consumer {0:?}")]
    MissingConsumer(RoutedConsumerId),
}
