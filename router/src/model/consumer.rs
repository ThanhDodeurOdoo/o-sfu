use super::{ConsumerId, MediaKind, ProducerId, StreamType, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Consumer {
    id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    stream_type: StreamType,
}

impl Consumer {
    #[must_use]
    pub fn new(
        id: ConsumerId,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
        stream_type: StreamType,
    ) -> Self {
        Self {
            id,
            producer_id,
            transport_id,
            media_kind,
            stream_type,
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

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub fn stream_type(&self) -> StreamType {
        self.stream_type
    }
}
