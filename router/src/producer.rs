use crate::{ProducerId, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producer {
    id: ProducerId,
    transport_id: TransportId,
}

impl Producer {
    #[must_use]
    pub fn new(id: ProducerId, transport_id: TransportId) -> Self {
        Self { id, transport_id }
    }
}
