use super::{ConsumerId, ProducerId, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Consumer {
    id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
}

impl Consumer {
    #[must_use]
    pub fn new(id: ConsumerId, producer_id: ProducerId, transport_id: TransportId) -> Self {
        Self {
            id,
            producer_id,
            transport_id,
        }
    }

    #[must_use]
    pub fn id(&self) -> ConsumerId {
        self.id
    }

    #[must_use]
    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    #[must_use]
    pub fn transport_id(&self) -> TransportId {
        self.transport_id
    }
}
