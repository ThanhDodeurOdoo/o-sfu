//! Read-only router state inspection for proofs and subsystem tests.

use super::{
    ConsumerId, ConsumerRouteState, MediaKind, ProducerId, ProducerRouteState, Router, RouterId,
    RouterObserver, SessionId, SessionState, TransportDirection, TransportId,
};

// these macros keep proof-only option handling closure-free under kani while
// preserving compact helper bodies for normal tests because repeated closures
// in these router predicates make the solver formula much larger
macro_rules! option_len {
    ($option:expr) => {{
        #[cfg(not(kani))]
        {
            $option.map_or(0, |value| value.len())
        }
        #[cfg(kani)]
        {
            match $option {
                Some(value) => value.len(),
                None => 0,
            }
        }
    }};
}

macro_rules! option_matches {
    ($option:expr, |$value:ident| $predicate:expr) => {{
        #[cfg(not(kani))]
        {
            $option.is_some_and(|$value| $predicate)
        }
        #[cfg(kani)]
        {
            match $option {
                Some($value) => $predicate,
                None => false,
            }
        }
    }};
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterStateSnapshot {
    pub id: RouterId,
    pub sessions: Vec<(SessionId, SessionState)>,
    pub transports: Vec<(TransportId, SessionId, TransportDirection)>,
    pub producers: Vec<(ProducerId, TransportId, MediaKind, ProducerRouteState)>,
    pub consumers: Vec<(
        ConsumerId,
        ProducerId,
        TransportId,
        MediaKind,
        ConsumerRouteState,
        ProducerRouteState,
    )>,
    pub session_transports: Vec<(SessionId, Vec<TransportId>)>,
    pub transport_producers: Vec<(TransportId, Vec<ProducerId>)>,
    pub transport_consumers: Vec<(TransportId, Vec<ConsumerId>)>,
    pub producer_consumers: Vec<(ProducerId, Vec<ConsumerId>)>,
}

#[must_use]
pub fn router_state_snapshot<O: RouterObserver>(router: &Router<O>) -> RouterStateSnapshot {
    RouterStateSnapshot {
        id: router.id,
        sessions: router
            .sessions
            .values()
            .map(|session| (session.id(), session.state()))
            .collect(),
        transports: router
            .transports
            .values()
            .map(|transport| {
                (
                    transport.id(),
                    transport.session_id(),
                    transport.direction(),
                )
            })
            .collect(),
        producers: router
            .producers
            .values()
            .map(|producer| {
                (
                    producer.id(),
                    producer.transport_id(),
                    producer.media_kind(),
                    producer.route_state(),
                )
            })
            .collect(),
        consumers: router
            .consumers
            .values()
            .map(|consumer| {
                (
                    consumer.id(),
                    consumer.producer_id(),
                    consumer.transport_id(),
                    consumer.media_kind(),
                    consumer.route_state(),
                    consumer.producer_route_state(),
                )
            })
            .collect(),
        session_transports: router
            .session_transports
            .iter()
            .map(|(session_id, transport_ids)| {
                (*session_id, transport_ids.iter().copied().collect())
            })
            .collect(),
        transport_producers: router
            .transport_producers
            .iter()
            .map(|(transport_id, producer_ids)| {
                (*transport_id, producer_ids.iter().copied().collect())
            })
            .collect(),
        transport_consumers: router
            .transport_consumers
            .iter()
            .map(|(transport_id, consumer_ids)| {
                (*transport_id, consumer_ids.iter().copied().collect())
            })
            .collect(),
        producer_consumers: router
            .producer_consumers
            .iter()
            .map(|(producer_id, consumer_ids)| {
                (*producer_id, consumer_ids.iter().copied().collect())
            })
            .collect(),
    }
}

#[must_use]
pub fn router_contains_session<O: RouterObserver>(
    router: &Router<O>,
    session_id: SessionId,
) -> bool {
    router.sessions.contains_key(&session_id)
}

#[must_use]
pub fn router_contains_transport<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
) -> bool {
    router.transports.contains_key(&transport_id)
}

#[must_use]
pub fn router_contains_producer<O: RouterObserver>(
    router: &Router<O>,
    producer_id: ProducerId,
) -> bool {
    router.producers.contains_key(&producer_id)
}

#[must_use]
pub fn router_contains_consumer<O: RouterObserver>(
    router: &Router<O>,
    consumer_id: ConsumerId,
) -> bool {
    router.consumers.contains_key(&consumer_id)
}

#[must_use]
pub fn router_transport_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.transports.len()
}

#[must_use]
pub fn router_producer_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.producers.len()
}

#[must_use]
pub fn router_consumer_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.consumers.len()
}

#[must_use]
pub fn router_session_transport_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.session_transports.len()
}

#[must_use]
pub fn router_transport_producer_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.transport_producers.len()
}

#[must_use]
pub fn router_transport_consumer_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.transport_consumers.len()
}

#[must_use]
pub fn router_producer_consumer_index_count<O: RouterObserver>(router: &Router<O>) -> usize {
    router.producer_consumers.len()
}

#[must_use]
pub fn router_session_transport_count<O: RouterObserver>(
    router: &Router<O>,
    session_id: SessionId,
) -> usize {
    option_len!(router.session_transports.get(&session_id))
}

#[must_use]
pub fn router_transport_producer_count<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
) -> usize {
    option_len!(router.transport_producers.get(&transport_id))
}

#[must_use]
pub fn router_transport_consumer_count<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
) -> usize {
    option_len!(router.transport_consumers.get(&transport_id))
}

#[must_use]
pub fn router_producer_consumer_count<O: RouterObserver>(
    router: &Router<O>,
    producer_id: ProducerId,
) -> usize {
    option_len!(router.producer_consumers.get(&producer_id))
}

#[must_use]
pub fn router_has_session_transport<O: RouterObserver>(
    router: &Router<O>,
    session_id: SessionId,
    transport_id: TransportId,
) -> bool {
    option_matches!(router.session_transports.get(&session_id), |ids| {
        ids.contains(&transport_id)
    })
}

#[must_use]
pub fn router_has_transport_producer<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
    producer_id: ProducerId,
) -> bool {
    option_matches!(router.transport_producers.get(&transport_id), |ids| {
        ids.contains(&producer_id)
    })
}

#[must_use]
pub fn router_has_transport_consumer<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
    consumer_id: ConsumerId,
) -> bool {
    option_matches!(router.transport_consumers.get(&transport_id), |ids| {
        ids.contains(&consumer_id)
    })
}

#[must_use]
pub fn router_has_producer_consumer<O: RouterObserver>(
    router: &Router<O>,
    producer_id: ProducerId,
    consumer_id: ConsumerId,
) -> bool {
    option_matches!(router.producer_consumers.get(&producer_id), |ids| {
        ids.contains(&consumer_id)
    })
}

#[must_use]
pub fn router_transport_matches<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
    session_id: SessionId,
    direction: TransportDirection,
) -> bool {
    option_matches!(router.transports.get(&transport_id), |transport| {
        transport.session_id() == session_id && transport.direction() == direction
    })
}

#[must_use]
pub fn router_producer_origin_matches<O: RouterObserver>(
    router: &Router<O>,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
) -> bool {
    option_matches!(router.producers.get(&producer_id), |producer| {
        producer.transport_id() == transport_id && producer.media_kind() == media_kind
    })
}

#[must_use]
pub fn router_consumer_origin_matches<O: RouterObserver>(
    router: &Router<O>,
    consumer_id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
) -> bool {
    option_matches!(router.consumers.get(&consumer_id), |consumer| {
        consumer.producer_id() == producer_id
            && consumer.transport_id() == transport_id
            && consumer.media_kind() == media_kind
    })
}

#[must_use]
pub fn router_consumer_shadows_producer<O: RouterObserver>(
    router: &Router<O>,
    consumer_id: ConsumerId,
) -> bool {
    option_matches!(router.consumers.get(&consumer_id), |consumer| {
        option_matches!(router.producers.get(&consumer.producer_id()), |producer| {
            consumer.producer_route_state() == producer.route_state()
        })
    })
}

#[must_use]
pub fn router_has_session_transport_index<O: RouterObserver>(
    router: &Router<O>,
    session_id: SessionId,
) -> bool {
    router.session_transports.contains_key(&session_id)
}

#[must_use]
pub fn router_has_transport_producer_index<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
) -> bool {
    router.transport_producers.contains_key(&transport_id)
}

#[must_use]
pub fn router_has_transport_consumer_index<O: RouterObserver>(
    router: &Router<O>,
    transport_id: TransportId,
) -> bool {
    router.transport_consumers.contains_key(&transport_id)
}

#[must_use]
pub fn router_has_producer_consumer_index<O: RouterObserver>(
    router: &Router<O>,
    producer_id: ProducerId,
) -> bool {
    router.producer_consumers.contains_key(&producer_id)
}

#[must_use]
pub fn router_consumer_route_matches<O: RouterObserver>(
    router: &Router<O>,
    consumer_id: ConsumerId,
    route_state: ConsumerRouteState,
    producer_route_state: ProducerRouteState,
) -> bool {
    option_matches!(router.consumers.get(&consumer_id), |consumer| {
        consumer.route_state() == route_state
            && consumer.producer_route_state() == producer_route_state
    })
}

#[must_use]
pub fn router_satisfies_invariants<O: RouterObserver>(router: &Router<O>) -> bool {
    references_are_valid(router)
        && reverse_indices_are_exact(router)
        && transport_directions_are_valid(router)
        && consumer_media_matches_producer(router)
        && consumer_pause_shadows_producer(router)
}

fn references_are_valid<O: RouterObserver>(router: &Router<O>) -> bool {
    for transport in router.transports.values() {
        if !router.sessions.contains_key(&transport.session_id()) {
            return false;
        }
    }

    for producer in router.producers.values() {
        if !router.transports.contains_key(&producer.transport_id()) {
            return false;
        }
    }

    for consumer in router.consumers.values() {
        if !router.transports.contains_key(&consumer.transport_id())
            || !router.producers.contains_key(&consumer.producer_id())
        {
            return false;
        }
    }

    true
}

fn reverse_indices_are_exact<O: RouterObserver>(router: &Router<O>) -> bool {
    session_transport_index_is_exact(router)
        && transport_producer_index_is_exact(router)
        && transport_consumer_index_is_exact(router)
        && producer_consumer_index_is_exact(router)
}

fn session_transport_index_is_exact<O: RouterObserver>(router: &Router<O>) -> bool {
    for (session_id, transport_ids) in &router.session_transports {
        if !router.sessions.contains_key(session_id) || transport_ids.is_empty() {
            return false;
        }

        for transport_id in transport_ids {
            let Some(transport) = router.transports.get(transport_id) else {
                return false;
            };
            if transport.session_id() != *session_id {
                return false;
            }
        }
    }

    for transport in router.transports.values() {
        if !router_has_session_transport(router, transport.session_id(), transport.id()) {
            return false;
        }
    }

    true
}

fn transport_producer_index_is_exact<O: RouterObserver>(router: &Router<O>) -> bool {
    for (transport_id, producer_ids) in &router.transport_producers {
        if !router.transports.contains_key(transport_id) || producer_ids.is_empty() {
            return false;
        }

        for producer_id in producer_ids {
            let Some(producer) = router.producers.get(producer_id) else {
                return false;
            };
            if producer.transport_id() != *transport_id {
                return false;
            }
        }
    }

    for producer in router.producers.values() {
        if !router_has_transport_producer(router, producer.transport_id(), producer.id()) {
            return false;
        }
    }

    true
}

fn transport_consumer_index_is_exact<O: RouterObserver>(router: &Router<O>) -> bool {
    for (transport_id, consumer_ids) in &router.transport_consumers {
        if !router.transports.contains_key(transport_id) || consumer_ids.is_empty() {
            return false;
        }

        for consumer_id in consumer_ids {
            let Some(consumer) = router.consumers.get(consumer_id) else {
                return false;
            };
            if consumer.transport_id() != *transport_id {
                return false;
            }
        }
    }

    for consumer in router.consumers.values() {
        if !router_has_transport_consumer(router, consumer.transport_id(), consumer.id()) {
            return false;
        }
    }

    true
}

fn producer_consumer_index_is_exact<O: RouterObserver>(router: &Router<O>) -> bool {
    for (producer_id, consumer_ids) in &router.producer_consumers {
        if !router.producers.contains_key(producer_id) || consumer_ids.is_empty() {
            return false;
        }

        for consumer_id in consumer_ids {
            let Some(consumer) = router.consumers.get(consumer_id) else {
                return false;
            };
            if consumer.producer_id() != *producer_id {
                return false;
            }
        }
    }

    for consumer in router.consumers.values() {
        if !router_has_producer_consumer(router, consumer.producer_id(), consumer.id()) {
            return false;
        }
    }

    true
}

fn transport_directions_are_valid<O: RouterObserver>(router: &Router<O>) -> bool {
    for producer in router.producers.values() {
        let Some(transport) = router.transports.get(&producer.transport_id()) else {
            return false;
        };
        if transport.direction() != TransportDirection::Receive {
            return false;
        }
    }

    for consumer in router.consumers.values() {
        let Some(transport) = router.transports.get(&consumer.transport_id()) else {
            return false;
        };
        if transport.direction() != TransportDirection::Send {
            return false;
        }
    }

    true
}

fn consumer_media_matches_producer<O: RouterObserver>(router: &Router<O>) -> bool {
    for consumer in router.consumers.values() {
        let Some(producer) = router.producers.get(&consumer.producer_id()) else {
            return false;
        };
        if consumer.media_kind() != producer.media_kind() {
            return false;
        }
    }

    true
}

fn consumer_pause_shadows_producer<O: RouterObserver>(router: &Router<O>) -> bool {
    for consumer in router.consumers.values() {
        let Some(producer) = router.producers.get(&consumer.producer_id()) else {
            return false;
        };
        if consumer.producer_route_state() != producer.route_state() {
            return false;
        }
    }

    true
}
