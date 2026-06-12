#![allow(
    clippy::panic,
    reason = "router tests use panic only for mandatory fixture setup failures"
)]

use std::{cell::RefCell, rc::Rc};

use super::router_invariants::assert_router_is_consistent;
use crate::{
    ConsumerCapability, ConsumerId, ConsumerRouteState, ConsumerSpec, MediaKind, ProducerId,
    ProducerRouteState, ProducerSpec, Router, RouterError, RouterEvent, RouterId, RouterObserver,
    Session, SessionId, SessionState, TransportId, model::test_support::router_state_snapshot,
};

const ROUTER: RouterId = RouterId(1);
const PUBLISHER_SESSION: SessionId = SessionId(10);
const SUBSCRIBER_SESSION: SessionId = SessionId(20);
const SECOND_SUBSCRIBER_SESSION: SessionId = SessionId(30);
const PUBLISHER_RECV_TRANSPORT: TransportId = TransportId(100);
const PUBLISHER_SEND_TRANSPORT: TransportId = TransportId(101);
const SUBSCRIBER_SEND_TRANSPORT: TransportId = TransportId(200);
const SECOND_SUBSCRIBER_SEND_TRANSPORT: TransportId = TransportId(201);
const PRODUCER: ProducerId = ProducerId(300);
const CONSUMER: ConsumerId = ConsumerId(400);
const SECOND_CONSUMER: ConsumerId = ConsumerId(401);

fn join<O: RouterObserver>(router: &mut Router<O>, session_id: SessionId) {
    assert_eq!(router.join(Session::new(session_id)), Ok(()));
}

fn open_receive<O: RouterObserver>(
    router: &mut Router<O>,
    session_id: SessionId,
    transport_id: TransportId,
) {
    assert_eq!(
        router
            .session(session_id)
            .and_then(|session| session.open_receive_transport(transport_id))
            .map(|_| ()),
        Ok(())
    );
}

fn open_send<O: RouterObserver>(
    router: &mut Router<O>,
    session_id: SessionId,
    transport_id: TransportId,
) {
    assert_eq!(
        router
            .session(session_id)
            .and_then(|session| session.open_send_transport(transport_id))
            .map(|_| ()),
        Ok(())
    );
}

fn publish<O: RouterObserver>(
    router: &mut Router<O>,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
) {
    assert_eq!(
        router
            .receive_transport(transport_id)
            .and_then(|transport| transport.publish(ProducerSpec::new(producer_id, media_kind))),
        Ok(producer_id)
    );
}

fn consume<O: RouterObserver>(
    router: &mut Router<O>,
    consumer_id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    capability: ConsumerCapability,
) {
    assert_eq!(
        router.send_transport(transport_id).and_then(|transport| {
            transport.consume(ConsumerSpec::new(consumer_id, producer_id, capability))
        }),
        Ok(consumer_id)
    );
}

fn consume_compatible<O: RouterObserver>(
    router: &mut Router<O>,
    consumer_id: ConsumerId,
    transport_id: TransportId,
) {
    consume(
        router,
        consumer_id,
        PRODUCER,
        transport_id,
        ConsumerCapability::Compatible,
    );
}

fn try_publish<O: RouterObserver>(
    router: &mut Router<O>,
    transport_id: TransportId,
    spec: ProducerSpec,
) -> Result<ProducerId, RouterError> {
    router
        .receive_transport(transport_id)
        .and_then(|transport| transport.publish(spec))
}

fn try_consume<O: RouterObserver>(
    router: &mut Router<O>,
    transport_id: TransportId,
    spec: ConsumerSpec,
) -> Result<ConsumerId, RouterError> {
    router
        .send_transport(transport_id)
        .and_then(|transport| transport.consume(spec))
}

fn prepare_publisher_topology(media_kind: MediaKind) -> Router {
    let mut router = Router::new(ROUTER);
    join(&mut router, PUBLISHER_SESSION);
    join(&mut router, SUBSCRIBER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);
    open_send(&mut router, SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT);
    publish(&mut router, PRODUCER, PUBLISHER_RECV_TRANSPORT, media_kind);
    router
}

fn prepare_publish_subscribe_pair(media_kind: MediaKind) -> Router {
    let mut router = prepare_publisher_topology(media_kind);
    consume_compatible(&mut router, CONSUMER, SUBSCRIBER_SEND_TRANSPORT);
    router
}

fn prepare_two_consumer_flow(media_kind: MediaKind) -> Router {
    let mut router = Router::new(ROUTER);
    join(&mut router, PUBLISHER_SESSION);
    join(&mut router, SUBSCRIBER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);
    open_send(&mut router, PUBLISHER_SESSION, PUBLISHER_SEND_TRANSPORT);
    open_send(&mut router, SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT);
    publish(&mut router, PRODUCER, PUBLISHER_RECV_TRANSPORT, media_kind);
    consume_compatible(&mut router, CONSUMER, PUBLISHER_SEND_TRANSPORT);
    consume_compatible(&mut router, SECOND_CONSUMER, SUBSCRIBER_SEND_TRANSPORT);
    router
}

#[test]
fn router_accepts_a_basic_publish_and_subscribe_flow() {
    let router = prepare_publish_subscribe_pair(MediaKind::Audio);

    assert_router_is_consistent(&router);
}

#[test]
fn router_rejects_orphan_resources() {
    let mut router = Router::new(ROUTER);

    assert_eq!(
        router
            .session(PUBLISHER_SESSION)
            .and_then(|session| session.open_receive_transport(PUBLISHER_RECV_TRANSPORT))
            .map(|_| ()),
        Err(RouterError::MissingSession(PUBLISHER_SESSION))
    );
    assert_eq!(
        try_publish(
            &mut router,
            PUBLISHER_RECV_TRANSPORT,
            ProducerSpec::new(PRODUCER, MediaKind::Audio),
        ),
        Err(RouterError::MissingTransport(PUBLISHER_RECV_TRANSPORT))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_session_cleans_dependent_resources() {
    let mut router = prepare_publish_subscribe_pair(MediaKind::Audio);

    assert_eq!(router.remove_session(PUBLISHER_SESSION), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert_eq!(snapshot.session_count(), 1);
    assert_eq!(snapshot.transport_count(), 1);
    assert_eq!(snapshot.producer_count(), 0);
    assert_eq!(snapshot.consumer_count(), 0);
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_producer_cleans_dependent_consumers_but_keeps_transports() {
    let mut router = prepare_two_consumer_flow(MediaKind::Audio);

    assert_eq!(router.remove_producer(PRODUCER), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert!(!snapshot.contains_producer(PRODUCER));
    assert!(!snapshot.contains_consumer(CONSUMER));
    assert!(!snapshot.contains_consumer(SECOND_CONSUMER));
    assert!(snapshot.contains_transport(PUBLISHER_RECV_TRANSPORT));
    assert!(snapshot.contains_transport(PUBLISHER_SEND_TRANSPORT));
    assert!(snapshot.contains_transport(SUBSCRIBER_SEND_TRANSPORT));
    assert!(
        !snapshot
            .transport_producers()
            .contains_key(PUBLISHER_RECV_TRANSPORT)
    );
    assert!(
        !snapshot
            .transport_consumers()
            .contains_key(PUBLISHER_SEND_TRANSPORT)
    );
    assert!(
        !snapshot
            .transport_consumers()
            .contains_key(SUBSCRIBER_SEND_TRANSPORT)
    );
    assert!(!snapshot.producer_consumers().contains_key(PRODUCER));
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_producer_rejects_missing_owning_transport() {
    let observer = EventCaptureObserver::default();
    let event_log = observer.clone();
    let mut router = Router::new_with_observer(ROUTER, observer);

    join(&mut router, PUBLISHER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);
    publish(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Audio,
    );

    router.transports.remove(&PUBLISHER_RECV_TRANSPORT);

    assert_eq!(
        router.remove_producer(PRODUCER),
        Err(RouterError::MissingProducerTransport {
            producer_id: PRODUCER,
            transport_id: PUBLISHER_RECV_TRANSPORT,
        })
    );
    assert!(router_state_snapshot(&router).contains_producer(PRODUCER));
    assert_eq!(
        event_log.recorded_events(),
        vec![
            RouterEvent::SessionJoined {
                session_id: PUBLISHER_SESSION,
            },
            RouterEvent::ProducerAdded {
                session_id: PUBLISHER_SESSION,
                transport_id: PUBLISHER_RECV_TRANSPORT,
                producer_id: PRODUCER,
                media_kind: MediaKind::Audio,
            },
        ]
    );
}

#[test]
fn removing_a_consumer_preserves_other_routes() {
    let mut router = prepare_two_consumer_flow(MediaKind::Audio);

    assert_eq!(router.remove_consumer(CONSUMER), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert!(!snapshot.contains_consumer(CONSUMER));
    assert!(snapshot.contains_consumer(SECOND_CONSUMER));
    assert!(snapshot.contains_producer(PRODUCER));
    assert!(
        !snapshot
            .transport_consumers()
            .contains_key(PUBLISHER_SEND_TRANSPORT)
    );
    assert!(
        snapshot
            .transport_consumers()
            .contains(SUBSCRIBER_SEND_TRANSPORT, SECOND_CONSUMER)
    );
    assert!(
        snapshot
            .producer_consumers()
            .contains(PRODUCER, SECOND_CONSUMER)
    );
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_session_clears_cross_session_reverse_indices() {
    let mut router = prepare_two_consumer_flow(MediaKind::Audio);

    assert_eq!(router.remove_session(PUBLISHER_SESSION), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.contains_session(SUBSCRIBER_SESSION));
    assert!(snapshot.contains_transport(SUBSCRIBER_SEND_TRANSPORT));
    assert!(!snapshot.contains_transport(PUBLISHER_RECV_TRANSPORT));
    assert!(!snapshot.contains_transport(PUBLISHER_SEND_TRANSPORT));
    assert!(!snapshot.contains_producer(PRODUCER));
    assert!(!snapshot.contains_consumer(CONSUMER));
    assert!(!snapshot.contains_consumer(SECOND_CONSUMER));
    assert!(
        !snapshot
            .session_transports()
            .contains_key(PUBLISHER_SESSION)
    );
    assert!(
        snapshot
            .session_transports()
            .contains(SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT)
    );
    assert!(
        !snapshot
            .transport_consumers()
            .contains_key(SUBSCRIBER_SEND_TRANSPORT)
    );
    assert!(!snapshot.producer_consumers().contains_key(PRODUCER));
    assert_router_is_consistent(&router);
}

#[test]
fn producers_must_use_receive_transports() {
    let mut router = Router::new(ROUTER);

    join(&mut router, PUBLISHER_SESSION);
    open_send(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);

    assert_eq!(
        try_publish(
            &mut router,
            PUBLISHER_RECV_TRANSPORT,
            ProducerSpec::new(PRODUCER, MediaKind::Audio),
        ),
        Err(RouterError::ProducerRequiresReceiveTransport(
            PUBLISHER_RECV_TRANSPORT
        ))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_must_use_send_transports() {
    let mut router = Router::new(ROUTER);
    join(&mut router, PUBLISHER_SESSION);
    join(&mut router, SUBSCRIBER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);
    open_receive(&mut router, SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT);
    publish(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Video,
    );

    assert_eq!(
        try_consume(
            &mut router,
            SUBSCRIBER_SEND_TRANSPORT,
            ConsumerSpec::new(CONSUMER, PRODUCER, ConsumerCapability::Compatible),
        ),
        Err(RouterError::ConsumerRequiresSendTransport(
            SUBSCRIBER_SEND_TRANSPORT
        ))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn failed_consumer_additions_do_not_mutate_router_state() {
    let mut router = prepare_publisher_topology(MediaKind::Audio);

    let consume_rejections = [
        (
            TransportId(999),
            ConsumerSpec::new(CONSUMER, PRODUCER, ConsumerCapability::Compatible),
            RouterError::MissingTransport(TransportId(999)),
        ),
        (
            PUBLISHER_RECV_TRANSPORT,
            ConsumerSpec::new(CONSUMER, PRODUCER, ConsumerCapability::Compatible),
            RouterError::ConsumerRequiresSendTransport(PUBLISHER_RECV_TRANSPORT),
        ),
        (
            SUBSCRIBER_SEND_TRANSPORT,
            ConsumerSpec::new(CONSUMER, ProducerId(999), ConsumerCapability::Compatible),
            RouterError::MissingProducer(ProducerId(999)),
        ),
        (
            SUBSCRIBER_SEND_TRANSPORT,
            ConsumerSpec::new(CONSUMER, PRODUCER, ConsumerCapability::Incompatible),
            RouterError::IncompatibleCapabilities {
                producer_id: PRODUCER,
            },
        ),
    ];

    for (transport_id, spec, expected_error) in consume_rejections {
        let before = router_state_snapshot(&router);

        assert_eq!(
            try_consume(&mut router, transport_id, spec),
            Err(expected_error)
        );
        assert_eq!(router_state_snapshot(&router), before);
        assert_router_is_consistent(&router);
    }
}

#[test]
fn duplicate_ids_do_not_replace_existing_router_state() {
    let mut router = Router::new(ROUTER);
    join(&mut router, PUBLISHER_SESSION);

    let before = router_state_snapshot(&router);
    assert_eq!(
        router.join(Session::new(PUBLISHER_SESSION)),
        Err(RouterError::DuplicateSession(PUBLISHER_SESSION))
    );
    assert_eq!(router_state_snapshot(&router), before);
    assert_router_is_consistent(&router);

    join(&mut router, SUBSCRIBER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);

    let before = router_state_snapshot(&router);
    assert_eq!(
        router
            .session(SUBSCRIBER_SESSION)
            .and_then(|session| session.open_send_transport(PUBLISHER_RECV_TRANSPORT))
            .map(|_| ()),
        Err(RouterError::DuplicateTransport(PUBLISHER_RECV_TRANSPORT))
    );
    assert_eq!(router_state_snapshot(&router), before);
    assert_router_is_consistent(&router);

    publish(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Audio,
    );

    let before = router_state_snapshot(&router);
    assert_eq!(
        try_publish(
            &mut router,
            PUBLISHER_RECV_TRANSPORT,
            ProducerSpec::new(PRODUCER, MediaKind::Video),
        ),
        Err(RouterError::DuplicateProducer(PRODUCER))
    );
    assert_eq!(router_state_snapshot(&router), before);
    assert_router_is_consistent(&router);

    open_send(&mut router, SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT);
    consume_compatible(&mut router, CONSUMER, SUBSCRIBER_SEND_TRANSPORT);
    open_send(
        &mut router,
        SUBSCRIBER_SESSION,
        SECOND_SUBSCRIBER_SEND_TRANSPORT,
    );

    let before = router_state_snapshot(&router);
    assert_eq!(
        try_consume(
            &mut router,
            SECOND_SUBSCRIBER_SEND_TRANSPORT,
            ConsumerSpec::new(CONSUMER, PRODUCER, ConsumerCapability::Compatible),
        ),
        Err(RouterError::DuplicateConsumer(CONSUMER))
    );
    assert_eq!(router_state_snapshot(&router), before);
    assert_router_is_consistent(&router);
}

#[test]
fn new_consumers_inherit_their_producer_pause_state() {
    let mut router = prepare_publisher_topology(MediaKind::Audio);
    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Paused),
        Ok(())
    );

    consume_compatible(&mut router, CONSUMER, SUBSCRIBER_SEND_TRANSPORT);

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.consumer_route_matches(
        CONSUMER,
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn pausing_a_producer_updates_all_dependent_consumers() {
    let mut router = prepare_publisher_topology(MediaKind::Video);
    join(&mut router, SECOND_SUBSCRIBER_SESSION);
    open_send(
        &mut router,
        SECOND_SUBSCRIBER_SESSION,
        SECOND_SUBSCRIBER_SEND_TRANSPORT,
    );
    consume_compatible(&mut router, CONSUMER, SUBSCRIBER_SEND_TRANSPORT);
    consume_compatible(
        &mut router,
        SECOND_CONSUMER,
        SECOND_SUBSCRIBER_SEND_TRANSPORT,
    );

    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Paused),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.producer_route_state_matches(PRODUCER, ProducerRouteState::Paused,));
    assert!(snapshot.consumer_route_matches(
        CONSUMER,
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(snapshot.consumer_route_matches(
        SECOND_CONSUMER,
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn resuming_a_producer_clears_dependent_consumer_pause_shadows() {
    let mut router = prepare_publish_subscribe_pair(MediaKind::Audio);
    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Paused),
        Ok(())
    );

    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Active),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.producer_route_state_matches(PRODUCER, ProducerRouteState::Active,));
    assert!(snapshot.consumer_route_matches(
        CONSUMER,
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn pausing_a_consumer_only_changes_its_local_pause_flag() {
    let mut router = prepare_publish_subscribe_pair(MediaKind::Audio);

    assert_eq!(
        router.set_consumer_route_state(CONSUMER, ConsumerRouteState::Paused),
        Ok(())
    );
    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Paused),
        Ok(())
    );
    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Active),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.consumer_route_matches(
        CONSUMER,
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn joined_sessions_store_only_router_lifecycle_state() {
    let mut router = Router::new(ROUTER);

    assert_eq!(router.join(Session::new(PUBLISHER_SESSION)), Ok(()));

    let Some(session) = router.sessions().next() else {
        panic!("joined session should be visible through the public iterator");
    };
    assert_eq!(session.state(), SessionState::Active);
    assert_router_is_consistent(&router);
}

#[test]
fn explicit_producer_removal_emits_one_removal_event() {
    let observer = EventCaptureObserver::default();
    let event_log = observer.clone();
    let mut router = Router::new_with_observer(ROUTER, observer);

    join(&mut router, PUBLISHER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);
    publish(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Video,
    );

    assert_eq!(router.remove_producer(PRODUCER), Ok(()));

    assert_eq!(
        event_log.recorded_events(),
        vec![
            RouterEvent::SessionJoined {
                session_id: PUBLISHER_SESSION,
            },
            RouterEvent::ProducerAdded {
                session_id: PUBLISHER_SESSION,
                transport_id: PUBLISHER_RECV_TRANSPORT,
                producer_id: PRODUCER,
                media_kind: MediaKind::Video,
            },
            RouterEvent::ProducerRemoved {
                session_id: PUBLISHER_SESSION,
                transport_id: PUBLISHER_RECV_TRANSPORT,
                producer_id: PRODUCER,
                media_kind: MediaKind::Video,
            },
        ]
    );
    assert!(!router_state_snapshot(&router).contains_producer(PRODUCER));
}

#[test]
fn missing_producer_removal_emits_no_removal_event() {
    let observer = EventCaptureObserver::default();
    let event_log = observer.clone();
    let mut router = Router::new_with_observer(ROUTER, observer);

    assert_eq!(
        router.remove_producer(PRODUCER),
        Err(RouterError::MissingProducer(PRODUCER))
    );

    assert!(event_log.recorded_events().is_empty());
}

#[derive(Clone, Default)]
struct EventCaptureObserver {
    events: Rc<RefCell<Vec<RouterEvent>>>,
}

impl EventCaptureObserver {
    fn recorded_events(&self) -> Vec<RouterEvent> {
        self.events.borrow().clone()
    }
}

impl RouterObserver for EventCaptureObserver {
    fn on_event(&mut self, event: RouterEvent) {
        self.events.borrow_mut().push(event);
    }
}

#[test]
fn router_emits_session_and_producer_lifecycle_events() {
    let observer = EventCaptureObserver::default();
    let event_log = observer.clone();
    let mut router = Router::new_with_observer(ROUTER, observer);

    join(&mut router, PUBLISHER_SESSION);
    join(&mut router, SUBSCRIBER_SESSION);
    open_receive(&mut router, PUBLISHER_SESSION, PUBLISHER_RECV_TRANSPORT);
    open_send(&mut router, SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT);
    publish(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Video,
    );
    assert_eq!(router.remove_session(PUBLISHER_SESSION), Ok(()));
    assert_eq!(router.remove_session(SUBSCRIBER_SESSION), Ok(()));

    assert_eq!(
        event_log.recorded_events(),
        vec![
            RouterEvent::SessionJoined {
                session_id: PUBLISHER_SESSION,
            },
            RouterEvent::SessionJoined {
                session_id: SUBSCRIBER_SESSION,
            },
            RouterEvent::ProducerAdded {
                session_id: PUBLISHER_SESSION,
                transport_id: PUBLISHER_RECV_TRANSPORT,
                producer_id: PRODUCER,
                media_kind: MediaKind::Video,
            },
            RouterEvent::ProducerRemoved {
                session_id: PUBLISHER_SESSION,
                transport_id: PUBLISHER_RECV_TRANSPORT,
                producer_id: PRODUCER,
                media_kind: MediaKind::Video,
            },
            RouterEvent::SessionLeft {
                session_id: PUBLISHER_SESSION,
            },
            RouterEvent::SessionLeft {
                session_id: SUBSCRIBER_SESSION,
            },
        ]
    );
}
