use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::engine::room) struct ProducerRuntimeId(u64);

impl ProducerRuntimeId {
    pub fn allocate(next_producer_id: &mut u64) -> Self {
        let current = *next_producer_id;
        *next_producer_id = next_producer_id.saturating_add(1);
        Self(current)
    }
}

impl Display for ProducerRuntimeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "producer-{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::engine::room) struct ConsumerRuntimeId(u64);

impl ConsumerRuntimeId {
    pub fn allocate(next_consumer_id: &mut u64) -> Self {
        let current = *next_consumer_id;
        *next_consumer_id = next_consumer_id.saturating_add(1);
        Self(current)
    }

    pub fn into_wire_id(self) -> String {
        self.to_string()
    }
}

impl Display for ConsumerRuntimeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "consumer-{}", self.0)
    }
}
