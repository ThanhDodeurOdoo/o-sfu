use super::{ConsumerId, MediaKind, ProducerId, StreamType, TransportId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Consumer {
    id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    stream_type: StreamType,
    paused: bool,
    producer_paused: bool,
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
            paused: false,
            producer_paused: false,
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

    #[must_use]
    pub fn paused(&self) -> bool {
        self.paused
    }

    #[must_use]
    pub fn producer_paused(&self) -> bool {
        self.producer_paused
    }

    pub(super) fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub(super) fn set_producer_paused(&mut self, producer_paused: bool) {
        self.producer_paused = producer_paused;
    }
}
