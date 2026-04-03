use super::ProofRouterModel;

impl<
    const MAX_SESSIONS: usize,
    const MAX_TRANSPORTS: usize,
    const MAX_PRODUCERS: usize,
    const MAX_CONSUMERS: usize,
> ProofRouterModel<MAX_SESSIONS, MAX_TRANSPORTS, MAX_PRODUCERS, MAX_CONSUMERS>
{
    #[must_use]
    pub(crate) fn satisfies_invariants(&self) -> bool {
        self.session_ids_are_unique()
            && self.transport_ids_are_unique()
            && self.producer_ids_are_unique()
            && self.consumer_ids_are_unique()
            && self.references_are_valid()
            && self.transport_directions_are_valid()
    }

    fn session_ids_are_unique(&self) -> bool {
        let mut left_index = 0;
        while let Some(left_slot) = self.sessions.get(left_index) {
            if let Some(left) = *left_slot {
                let mut right_index = left_index + 1;
                while let Some(right_slot) = self.sessions.get(right_index) {
                    if right_slot.is_some_and(|right| left.id() == right.id()) {
                        return false;
                    }
                    right_index += 1;
                }
            }
            left_index += 1;
        }
        true
    }

    fn transport_ids_are_unique(&self) -> bool {
        let mut left_index = 0;
        while let Some(left_slot) = self.transports.get(left_index) {
            if let Some(left) = *left_slot {
                let mut right_index = left_index + 1;
                while let Some(right_slot) = self.transports.get(right_index) {
                    if right_slot.is_some_and(|right| left.id() == right.id()) {
                        return false;
                    }
                    right_index += 1;
                }
            }
            left_index += 1;
        }
        true
    }

    fn producer_ids_are_unique(&self) -> bool {
        let mut left_index = 0;
        while let Some(left_slot) = self.producers.get(left_index) {
            if let Some(left) = *left_slot {
                let mut right_index = left_index + 1;
                while let Some(right_slot) = self.producers.get(right_index) {
                    if right_slot.is_some_and(|right| left.id() == right.id()) {
                        return false;
                    }
                    right_index += 1;
                }
            }
            left_index += 1;
        }
        true
    }

    fn consumer_ids_are_unique(&self) -> bool {
        let mut left_index = 0;
        while let Some(left_slot) = self.consumers.get(left_index) {
            if let Some(left) = *left_slot {
                let mut right_index = left_index + 1;
                while let Some(right_slot) = self.consumers.get(right_index) {
                    if right_slot.is_some_and(|right| left.id() == right.id()) {
                        return false;
                    }
                    right_index += 1;
                }
            }
            left_index += 1;
        }
        true
    }

    fn references_are_valid(&self) -> bool {
        for transport in &self.transports {
            if transport.is_some_and(|transport| !self.contains_session(transport.session_id())) {
                return false;
            }
        }
        for producer in &self.producers {
            if producer.is_some_and(|producer| !self.contains_transport(producer.transport_id())) {
                return false;
            }
        }
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                !self.contains_transport(consumer.transport_id())
                    || !self.contains_producer(consumer.producer_id())
            }) {
                return false;
            }
        }
        true
    }

    fn transport_directions_are_valid(&self) -> bool {
        for producer in &self.producers {
            if producer.is_some_and(|producer| {
                self.transport_by_id(producer.transport_id())
                    .is_some_and(|transport| {
                        transport.direction() != crate::TransportDirection::Receive
                    })
            }) {
                return false;
            }
        }
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                self.transport_by_id(consumer.transport_id())
                    .is_some_and(|transport| {
                        transport.direction() != crate::TransportDirection::Send
                    })
            }) {
                return false;
            }
        }
        true
    }
}
