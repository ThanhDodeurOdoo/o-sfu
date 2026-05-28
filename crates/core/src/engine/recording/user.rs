use std::collections::BTreeSet;

use o_sfu_router::ProducerId;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct RecordingSession {
    producers: BTreeSet<ProducerId>,
}

impl RecordingSession {
    pub(crate) fn add_producer(&mut self, producer_id: ProducerId) {
        self.producers.insert(producer_id);
    }

    pub(crate) fn remove_producer(&mut self, producer_id: ProducerId) {
        self.producers.remove(&producer_id);
    }

    pub(crate) fn producer_count(&self) -> usize {
        self.producers.len()
    }
}
