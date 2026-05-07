use std::collections::BTreeMap;

use o_sfu_router::{MediaKind, ProducerId, TransportId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedProducer {
    pub(crate) transport_id: TransportId,
    pub(crate) media_kind: MediaKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct RecordingSession {
    producers: BTreeMap<ProducerId, TrackedProducer>,
}

impl RecordingSession {
    pub(crate) fn add_producer(
        &mut self,
        producer_id: ProducerId,
        transport_id: TransportId,
        media_kind: MediaKind,
    ) {
        self.producers.insert(
            producer_id,
            TrackedProducer {
                transport_id,
                media_kind,
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
