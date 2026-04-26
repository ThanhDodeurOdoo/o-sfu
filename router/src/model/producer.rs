//! Producer-side entities tracked by the pure router.

use super::{MediaKind, ProducerId, TransportId};

/// Media source attached to a receive transport.
///
/// `id` and `transport_id` identify the producer and its owning transport,
/// `media_kind` is the technical media class used by consumers, and `paused`
/// is the source-side mute shadow propagated to dependent consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Producer {
    id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    paused: bool,
}

impl Producer {
    #[must_use]
    pub fn new(id: ProducerId, transport_id: TransportId, media_kind: MediaKind) -> Self {
        Self {
            id,
            transport_id,
            media_kind,
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
    pub fn paused(&self) -> bool {
        self.paused
    }

    #[must_use]
    pub fn with_paused(mut self, paused: bool) -> Self {
        self.paused = paused;
        self
    }

    pub(super) fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }
}
