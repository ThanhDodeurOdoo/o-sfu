//! Read-only router state inspection for proofs and subsystem tests.

use super::{
    ConsumerId, ConsumerRouteState, MediaKind, ProducerId, ProducerRouteState, Router, RouterId,
    RouterObserver, SessionId, SessionState, TransportDirection, TransportId,
};

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
