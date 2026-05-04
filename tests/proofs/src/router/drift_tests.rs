#![allow(
    clippy::panic,
    reason = "the drift-control corpus should fail loudly when the proof model diverges"
)]

use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, ConsumerRouteState, MediaKind, Producer, ProducerId,
    ProducerRouteState, Router, RouterError, RouterId, Session, SessionId, Transport,
    TransportDirection, TransportId,
    test_support::{RouterStateSnapshot, router_state_snapshot},
};

use super::{
    ProofRouterModel,
    model::{ProofMembershipIndex, ProofRouterError},
};

type DriftProofRouter = ProofRouterModel<2, 2, 1, 1>;

#[derive(Clone, Copy)]
enum DriftTransition {
    Join(Session),
    OpenTransport(Transport),
    AddProducer(Producer),
    AddConsumer(Consumer, ConsumerCapability),
    SetProducerRouteState(ProducerId, ProducerRouteState),
    SetConsumerRouteState(ConsumerId, ConsumerRouteState),
    RemoveConsumer(ConsumerId),
    RemoveProducer(ProducerId),
    RemoveSession(SessionId),
}

#[test]
fn proof_router_model_matches_production_router_transition_corpus() {
    let mut production = Router::new(RouterId(1));
    let mut proof = DriftProofRouter::new(RouterId(1));

    let transitions = [
        DriftTransition::Join(Session::new(SessionId(10))),
        DriftTransition::Join(Session::new(SessionId(20))),
        DriftTransition::Join(Session::new(SessionId(10))),
        DriftTransition::OpenTransport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        DriftTransition::OpenTransport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        DriftTransition::OpenTransport(Transport::new(
            TransportId(201),
            SessionId(30),
            TransportDirection::Send,
        )),
        DriftTransition::OpenTransport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        DriftTransition::AddProducer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Video,
        )),
        DriftTransition::AddProducer(Producer::new(
            ProducerId(301),
            TransportId(200),
            MediaKind::Video,
        )),
        DriftTransition::AddConsumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Video,
            ),
            ConsumerCapability::Incompatible,
        ),
        DriftTransition::AddConsumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        DriftTransition::AddConsumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        DriftTransition::SetProducerRouteState(ProducerId(300), ProducerRouteState::Paused),
        DriftTransition::SetConsumerRouteState(ConsumerId(400), ConsumerRouteState::Paused),
        DriftTransition::SetProducerRouteState(ProducerId(300), ProducerRouteState::Active),
        DriftTransition::RemoveConsumer(ConsumerId(400)),
        DriftTransition::RemoveConsumer(ConsumerId(400)),
        DriftTransition::AddConsumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        DriftTransition::RemoveProducer(ProducerId(300)),
        DriftTransition::RemoveProducer(ProducerId(300)),
        DriftTransition::RemoveSession(SessionId(10)),
        DriftTransition::RemoveSession(SessionId(10)),
    ];

    for transition in transitions {
        let production_result = apply_production_transition(&mut production, transition);
        let proof_result = apply_proof_transition(&mut proof, transition);
        assert_matching_result(production_result, proof_result);
        assert_eq!(
            router_state_snapshot(&production),
            proof_state_snapshot(&proof)
        );
    }
}

fn apply_production_transition(
    router: &mut Router,
    transition: DriftTransition,
) -> Result<(), RouterError> {
    match transition {
        DriftTransition::Join(session) => router.join_session(session),
        DriftTransition::OpenTransport(transport) => router.open_transport(transport),
        DriftTransition::AddProducer(producer) => router.add_producer(producer),
        DriftTransition::AddConsumer(consumer, capability) => {
            router.add_consumer(consumer, capability)
        }
        DriftTransition::SetProducerRouteState(producer_id, route_state) => {
            router.set_producer_route_state(producer_id, route_state)
        }
        DriftTransition::SetConsumerRouteState(consumer_id, route_state) => {
            router.set_consumer_route_state(consumer_id, route_state)
        }
        DriftTransition::RemoveConsumer(consumer_id) => router.remove_consumer(consumer_id),
        DriftTransition::RemoveProducer(producer_id) => router.remove_producer(producer_id),
        DriftTransition::RemoveSession(session_id) => router.remove_session(session_id),
    }
}

fn apply_proof_transition(
    router: &mut DriftProofRouter,
    transition: DriftTransition,
) -> Result<(), ProofRouterError> {
    match transition {
        DriftTransition::Join(session) => router.join_session(session),
        DriftTransition::OpenTransport(transport) => router.open_transport(transport),
        DriftTransition::AddProducer(producer) => router.add_producer(producer),
        DriftTransition::AddConsumer(consumer, capability) => {
            router.add_consumer(consumer, capability)
        }
        DriftTransition::SetProducerRouteState(producer_id, route_state) => {
            router.set_producer_route_state(producer_id, route_state)
        }
        DriftTransition::SetConsumerRouteState(consumer_id, route_state) => {
            router.set_consumer_route_state(consumer_id, route_state)
        }
        DriftTransition::RemoveConsumer(consumer_id) => router.remove_consumer(consumer_id),
        DriftTransition::RemoveProducer(producer_id) => router.remove_producer(producer_id),
        DriftTransition::RemoveSession(session_id) => router.remove_session(session_id),
    }
}

fn assert_matching_result(
    production: Result<(), RouterError>,
    proof: Result<(), ProofRouterError>,
) {
    match proof {
        Ok(()) => assert_eq!(production, Ok(())),
        Err(ProofRouterError::Router(error)) => assert_eq!(production, Err(error)),
        Err(ProofRouterError::CapacityExceeded(resource_kind)) => {
            panic!("drift corpus exceeded proof model capacity for {resource_kind:?}");
        }
    }
}

fn proof_state_snapshot(router: &DriftProofRouter) -> RouterStateSnapshot {
    let mut sessions: Vec<_> = router
        .users
        .iter()
        .filter_map(|session| session.map(|session| (session.id(), session.state())))
        .collect();
    sessions.sort_by_key(|(session_id, _)| *session_id);

    let mut transports: Vec<_> = router
        .transports
        .iter()
        .filter_map(|transport| {
            transport.map(|transport| {
                (
                    transport.id(),
                    transport.session_id(),
                    transport.direction(),
                )
            })
        })
        .collect();
    transports.sort_by_key(|(transport_id, _, _)| *transport_id);

    let mut producers: Vec<_> = router
        .producers
        .iter()
        .filter_map(|producer| {
            producer.map(|producer| {
                (
                    producer.id(),
                    producer.transport_id(),
                    producer.media_kind(),
                    producer.route_state(),
                )
            })
        })
        .collect();
    producers.sort_by_key(|(producer_id, _, _, _)| *producer_id);

    let mut consumers: Vec<_> = router
        .consumers
        .iter()
        .filter_map(|consumer| {
            consumer.map(|consumer| {
                (
                    consumer.id(),
                    consumer.producer_id(),
                    consumer.transport_id(),
                    consumer.media_kind(),
                    consumer.route_state(),
                    consumer.producer_route_state(),
                )
            })
        })
        .collect();
    consumers.sort_by_key(|(consumer_id, _, _, _, _, _)| *consumer_id);

    RouterStateSnapshot {
        id: router.id,
        sessions,
        transports,
        producers,
        consumers,
        session_transports: membership_snapshot(&router.session_transports),
        transport_producers: membership_snapshot(&router.transport_producers),
        transport_consumers: membership_snapshot(&router.transport_consumers),
        producer_consumers: membership_snapshot(&router.producer_consumers),
    }
}

fn membership_snapshot<
    K: Copy + Ord,
    V: Copy + Ord,
    const MAX_KEYS: usize,
    const MAX_VALUES: usize,
>(
    index: &ProofMembershipIndex<K, V, MAX_KEYS, MAX_VALUES>,
) -> Vec<(K, Vec<V>)> {
    let mut entries: Vec<_> = index
        .entries
        .iter()
        .filter_map(|entry| {
            entry.map(|entry| {
                let mut values: Vec<_> = entry.values.iter().flatten().copied().collect();
                values.sort();
                (entry.key, values)
            })
        })
        .collect();
    entries.sort_by_key(|(key, _)| *key);
    entries
}
