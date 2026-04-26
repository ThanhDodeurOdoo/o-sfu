use super::{ConsumerId, MediaKind, ProducerId, TransportId};

/// Media sink attached to a send transport.
///
/// `producer_id` binds the consumer to its source, `transport_id` selects the
/// downstream transport, `media_kind` mirrors the expected source identity,
/// and `paused` is the local pause selected by the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Consumer {
    id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
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
    ) -> Self {
        Self {
            id,
            producer_id,
            transport_id,
            media_kind,
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
    pub fn paused(&self) -> bool {
        self.paused
    }

    #[must_use]
    pub fn producer_paused(&self) -> bool {
        self.producer_paused
    }

    #[must_use]
    pub fn with_paused(mut self, paused: bool) -> Self {
        self.paused = paused;
        self
    }

    #[must_use]
    pub fn with_producer_paused(mut self, producer_paused: bool) -> Self {
        self.producer_paused = producer_paused;
        self
    }

    pub(super) fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub(super) fn set_producer_paused(&mut self, producer_paused: bool) {
        self.producer_paused = producer_paused;
    }
}
