use std::collections::BTreeMap;

use o_sfu_router::{MediaKind, ProducerId, SessionId, StreamType, TransportId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedProducer {
    pub(crate) transport_id: TransportId,
    pub(crate) media_kind: MediaKind,
    pub(crate) stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingSession {
    session_id: SessionId,
    producers: BTreeMap<ProducerId, TrackedProducer>,
}

impl RecordingSession {
    pub(crate) fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            producers: BTreeMap::new(),
        }
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn add_producer(
        &mut self,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
        stream_type: StreamType,
    ) {
        self.producers.insert(
            producer_id,
            TrackedProducer {
                transport_id,
                media_kind,
                stream_type,
            },
        );
    }

    pub(crate) fn remove_producer(&mut self, producer_id: ProducerId) {
        self.producers.remove(&producer_id);
    }

    pub(crate) fn producer_count(&self) -> usize {
        self.producers.len()
    }
}
