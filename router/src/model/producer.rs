//! Producer-side entities tracked by the pure router.

use super::{MediaKind, ProducerId, StreamType, TransportId};

/// Media source attached to a receive transport.
///
/// `id` and `transport_id` identify the producer and its owning transport,
/// `media_kind` plus `stream_type` preserve the semantic media identity used by
/// consumers, and `paused` is the source-side mute shadow propagated to
/// dependent consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producer {
    id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    stream_type: StreamType,
    paused: bool,
}

impl Producer {
    #[must_use]
    pub fn new(
        id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
        stream_type: StreamType,
    ) -> Self {
        Self {
            id,
            transport_id,
            media_kind,
            stream_type,
            paused: false,
        }
    }

    #[must_use]
    pub fn id(&self) -> ProducerId {
        self.id
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

    pub(super) fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }
}
