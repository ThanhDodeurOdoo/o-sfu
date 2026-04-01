use crate::{ConsumerId, ProducerId, TransportId};

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
}
