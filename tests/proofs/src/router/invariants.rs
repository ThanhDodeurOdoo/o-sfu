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
            && self.live_sessions_are_active()
            && self.transport_ids_are_unique()
            && self.producer_ids_are_unique()
            && self.consumer_ids_are_unique()
            && self.references_are_valid()
            && self.reverse_indices_are_coherent()
            && self.transport_directions_are_valid()
            && self.consumer_media_matches_producer()
            && self.consumer_pause_shadows_producer()
    }

    fn live_sessions_are_active(&self) -> bool {
        let mut session_index = 0;
        while let Some(session_slot) = self.users.get(session_index) {
            if let Some(user) = session_slot
                && user.state() != o_sfu_router::SessionState::Active
            {
                return false;
            }
            session_index += 1;
        }
        true
    }

    fn session_ids_are_unique(&self) -> bool {
        let mut left_index = 0;
        while let Some(left_slot) = self.users.get(left_index) {
            if let Some(left) = *left_slot {
                let mut right_index = left_index + 1;
                while let Some(right_slot) = self.users.get(right_index) {
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

    fn reverse_indices_are_coherent(&self) -> bool {
        self.session_transport_index_is_coherent()
            && self.transport_producer_index_is_coherent()
            && self.transport_consumer_index_is_coherent()
            && self.producer_consumer_index_is_coherent()
    }

    fn session_transport_index_is_coherent(&self) -> bool {
        let mut entry_index = 0;
        while let Some(entry_slot) = self.session_transports.entries.get(entry_index) {
            if let Some(entry) = entry_slot {
                if !self.contains_session(entry.key)
                    || self.session_transports.member_count(entry.key) == 0
                {
                    return false;
                }
                let mut value_index = 0;
                while let Some(transport_id) = entry.values.get(value_index) {
                    if let Some(transport_id) = transport_id
                        && self
                            .transport_by_id(*transport_id)
                            .is_none_or(|transport| transport.session_id() != entry.key)
                    {
                        return false;
                    }
                    value_index += 1;
                }
            }
            entry_index += 1;
        }

        for transport in &self.transports {
            if transport.is_some_and(|transport| {
                !self
                    .session_transports
                    .contains_member(transport.session_id(), transport.id())
            }) {
                return false;
            }
        }

        true
    }

    fn transport_producer_index_is_coherent(&self) -> bool {
        let mut entry_index = 0;
        while let Some(entry_slot) = self.transport_producers.entries.get(entry_index) {
            if let Some(entry) = entry_slot {
                if !self.contains_transport(entry.key)
                    || self.transport_producers.member_count(entry.key) == 0
                {
                    return false;
                }
                let mut value_index = 0;
                while let Some(producer_id) = entry.values.get(value_index) {
                    if let Some(producer_id) = producer_id
                        && self
                            .producer_by_id(*producer_id)
                            .is_none_or(|producer| producer.transport_id() != entry.key)
                    {
                        return false;
                    }
                    value_index += 1;
                }
            }
            entry_index += 1;
        }

        for producer in &self.producers {
            if producer.is_some_and(|producer| {
                !self
                    .transport_producers
                    .contains_member(producer.transport_id(), producer.id())
            }) {
                return false;
            }
        }

        true
    }

    fn transport_consumer_index_is_coherent(&self) -> bool {
        let mut entry_index = 0;
        while let Some(entry_slot) = self.transport_consumers.entries.get(entry_index) {
            if let Some(entry) = entry_slot {
                if !self.contains_transport(entry.key)
                    || self.transport_consumers.member_count(entry.key) == 0
                {
                    return false;
                }
                let mut value_index = 0;
                while let Some(consumer_id) = entry.values.get(value_index) {
                    if let Some(consumer_id) = consumer_id
                        && self
                            .consumer_by_id(*consumer_id)
                            .is_none_or(|consumer| consumer.transport_id() != entry.key)
                    {
                        return false;
                    }
                    value_index += 1;
                }
            }
            entry_index += 1;
        }

        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                !self
                    .transport_consumers
                    .contains_member(consumer.transport_id(), consumer.id())
            }) {
                return false;
            }
        }

        true
    }

    fn producer_consumer_index_is_coherent(&self) -> bool {
        let mut entry_index = 0;
        while let Some(entry_slot) = self.producer_consumers.entries.get(entry_index) {
            if let Some(entry) = entry_slot {
                if !self.contains_producer(entry.key)
                    || self.producer_consumers.member_count(entry.key) == 0
                {
                    return false;
                }
                let mut value_index = 0;
                while let Some(consumer_id) = entry.values.get(value_index) {
                    if let Some(consumer_id) = consumer_id
                        && self
                            .consumer_by_id(*consumer_id)
                            .is_none_or(|consumer| consumer.producer_id() != entry.key)
                    {
                        return false;
                    }
                    value_index += 1;
                }
            }
            entry_index += 1;
        }

        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                !self
                    .producer_consumers
                    .contains_member(consumer.producer_id(), consumer.id())
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
                        transport.direction() != o_sfu_router::TransportDirection::Receive
                    })
            }) {
                return false;
            }
        }
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                self.transport_by_id(consumer.transport_id())
                    .is_some_and(|transport| {
                        transport.direction() != o_sfu_router::TransportDirection::Send
                    })
            }) {
                return false;
            }
        }
        true
    }

    fn consumer_media_matches_producer(&self) -> bool {
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                self.producer_by_id(consumer.producer_id())
                    .is_some_and(|producer| consumer.media_kind() != producer.media_kind())
            }) {
                return false;
            }
        }
        true
    }

    fn consumer_pause_shadows_producer(&self) -> bool {
        for consumer in &self.consumers {
            if consumer.is_some_and(|consumer| {
                self.producer_by_id(consumer.producer_id())
                    .is_some_and(|producer| consumer.producer_paused() != producer.paused())
            }) {
                return false;
            }
        }
        true
    }
}
