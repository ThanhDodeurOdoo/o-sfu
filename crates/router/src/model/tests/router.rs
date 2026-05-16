#![allow(
    clippy::panic,
    reason = "router tests use panic only for mandatory fixture setup failures"
)]

use std::{cell::RefCell, rc::Rc};

use super::router_invariants::assert_router_is_consistent;
use crate::{
    Consumer, ConsumerCapability, ConsumerId, ConsumerRouteState, MediaKind, Producer, ProducerId,
    ProducerRouteState, Router, RouterError, RouterEvent, RouterId, RouterObserver, Session,
    SessionId, SessionState, Transport, TransportDirection, TransportId,
    model::test_support::router_state_snapshot,
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

fn session(id: SessionId) -> Session {
    Session::new(id)
}

fn join_session<O: RouterObserver>(router: &mut Router<O>, session_id: SessionId) {
    assert_eq!(router.join_session(session(session_id)), Ok(()));
}

fn open_transport<O: RouterObserver>(
    router: &mut Router<O>,
    transport_id: TransportId,
    session_id: SessionId,
    direction: TransportDirection,
) {
    assert_eq!(
        router.open_transport(Transport::new(transport_id, session_id, direction)),
        Ok(())
    );
}

fn add_producer<O: RouterObserver>(
    router: &mut Router<O>,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
) {
    assert_eq!(
        router.add_producer(Producer::new(producer_id, transport_id, media_kind)),
        Ok(())
    );
}

fn add_consumer<O: RouterObserver>(
    router: &mut Router<O>,
    consumer_id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    capability: ConsumerCapability,
) {
    assert_eq!(
        router.add_consumer(
            Consumer::new(consumer_id, producer_id, transport_id, media_kind),
            capability,
        ),
        Ok(())
    );
}

fn add_compatible_consumer<O: RouterObserver>(
    router: &mut Router<O>,
    consumer_id: ConsumerId,
    transport_id: TransportId,
    media_kind: MediaKind,
) {
    add_consumer(
        router,
        consumer_id,
        PRODUCER,
        transport_id,
        media_kind,
        ConsumerCapability::Compatible,
    );
}

fn prepare_publish_pair(media_kind: MediaKind) -> Router {
    let mut router = Router::new(ROUTER);
    join_session(&mut router, PUBLISHER_SESSION);
    join_session(&mut router, SUBSCRIBER_SESSION);
    open_transport(
        &mut router,
        PUBLISHER_RECV_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Receive,
    );
    open_transport(
        &mut router,
        SUBSCRIBER_SEND_TRANSPORT,
        SUBSCRIBER_SESSION,
        TransportDirection::Send,
    );
    add_producer(&mut router, PRODUCER, PUBLISHER_RECV_TRANSPORT, media_kind);
    router
}

fn prepare_publish_subscribe_pair(media_kind: MediaKind) -> Router {
    let mut router = prepare_publish_pair(media_kind);
    add_compatible_consumer(&mut router, CONSUMER, SUBSCRIBER_SEND_TRANSPORT, media_kind);
    router
}

fn prepare_two_consumer_flow(media_kind: MediaKind) -> Router {
    let mut router = Router::new(ROUTER);
    join_session(&mut router, PUBLISHER_SESSION);
    join_session(&mut router, SUBSCRIBER_SESSION);
    open_transport(
        &mut router,
        PUBLISHER_RECV_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Receive,
    );
    open_transport(
        &mut router,
        PUBLISHER_SEND_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Send,
    );
    open_transport(
        &mut router,
        SUBSCRIBER_SEND_TRANSPORT,
        SUBSCRIBER_SESSION,
        TransportDirection::Send,
    );
    add_producer(&mut router, PRODUCER, PUBLISHER_RECV_TRANSPORT, media_kind);
    add_compatible_consumer(&mut router, CONSUMER, PUBLISHER_SEND_TRANSPORT, media_kind);
    add_compatible_consumer(
        &mut router,
        SECOND_CONSUMER,
        SUBSCRIBER_SEND_TRANSPORT,
        media_kind,
    );
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
        router.open_transport(Transport::new(
            PUBLISHER_RECV_TRANSPORT,
            PUBLISHER_SESSION,
            TransportDirection::Receive,
        )),
        Err(RouterError::MissingSession(PUBLISHER_SESSION))
    );
    assert_eq!(
        router.add_producer(Producer::new(
            PRODUCER,
            PUBLISHER_RECV_TRANSPORT,
            MediaKind::Audio
        )),
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
    assert!(!snapshot.has_transport_producer_index(PUBLISHER_RECV_TRANSPORT));
    assert!(!snapshot.has_transport_consumer_index(PUBLISHER_SEND_TRANSPORT));
    assert!(!snapshot.has_transport_consumer_index(SUBSCRIBER_SEND_TRANSPORT));
    assert!(!snapshot.has_producer_consumer_index(PRODUCER));
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_producer_rejects_missing_owning_transport() {
    let observer = EventCaptureObserver::default();
    let inspector = observer.clone();
    let mut router = Router::new_with_observer(ROUTER, observer);

    join_session(&mut router, PUBLISHER_SESSION);
    open_transport(
        &mut router,
        PUBLISHER_RECV_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Receive,
    );
    add_producer(
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
        inspector.recorded_events(),
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
    assert!(!snapshot.has_transport_consumer_index(PUBLISHER_SEND_TRANSPORT));
    assert!(snapshot.has_transport_consumer(SUBSCRIBER_SEND_TRANSPORT, SECOND_CONSUMER));
    assert!(snapshot.has_producer_consumer(PRODUCER, SECOND_CONSUMER));
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
    assert!(!snapshot.has_session_transport_index(PUBLISHER_SESSION));
    assert!(snapshot.has_session_transport(SUBSCRIBER_SESSION, SUBSCRIBER_SEND_TRANSPORT));
    assert!(!snapshot.has_transport_consumer_index(SUBSCRIBER_SEND_TRANSPORT));
    assert!(!snapshot.has_producer_consumer_index(PRODUCER));
    assert_router_is_consistent(&router);
}

#[test]
fn producers_must_use_receive_transports() {
    let mut router = Router::new(ROUTER);

    join_session(&mut router, PUBLISHER_SESSION);
    open_transport(
        &mut router,
        PUBLISHER_RECV_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Send,
    );

    assert_eq!(
        router.add_producer(Producer::new(
            PRODUCER,
            PUBLISHER_RECV_TRANSPORT,
            MediaKind::Audio
        )),
        Err(RouterError::ProducerRequiresReceiveTransport(
            PUBLISHER_RECV_TRANSPORT
        ))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_must_use_send_transports() {
    let mut router = Router::new(ROUTER);
    join_session(&mut router, PUBLISHER_SESSION);
    join_session(&mut router, SUBSCRIBER_SESSION);
    open_transport(
        &mut router,
        PUBLISHER_RECV_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Receive,
    );
    open_transport(
        &mut router,
        SUBSCRIBER_SEND_TRANSPORT,
        SUBSCRIBER_SESSION,
        TransportDirection::Receive,
    );
    add_producer(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Video,
    );

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                CONSUMER,
                PRODUCER,
                SUBSCRIBER_SEND_TRANSPORT,
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(RouterError::ConsumerRequiresSendTransport(
            SUBSCRIBER_SEND_TRANSPORT
        ))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_must_match_their_producer_media_kind() {
    let mut router = prepare_publish_pair(MediaKind::Audio);

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                CONSUMER,
                PRODUCER,
                SUBSCRIBER_SEND_TRANSPORT,
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(RouterError::ConsumerMediaKindMismatch {
            producer_id: PRODUCER,
            expected: MediaKind::Audio,
            actual: MediaKind::Video,
        })
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_are_rejected_when_capabilities_are_incompatible() {
    let mut router = prepare_publish_pair(MediaKind::Audio);

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                CONSUMER,
                PRODUCER,
                SUBSCRIBER_SEND_TRANSPORT,
                MediaKind::Audio,
            ),
            ConsumerCapability::Incompatible,
        ),
        Err(RouterError::IncompatibleCapabilities {
            producer_id: PRODUCER,
        })
    );
    assert_router_is_consistent(&router);
}

#[test]
fn new_consumers_inherit_their_producer_pause_state() {
    let mut router = prepare_publish_pair(MediaKind::Audio);
    assert_eq!(
        router.set_producer_route_state(PRODUCER, ProducerRouteState::Paused),
        Ok(())
    );

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                CONSUMER,
                PRODUCER,
                SUBSCRIBER_SEND_TRANSPORT,
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

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
    let mut router = prepare_publish_pair(MediaKind::Video);
    join_session(&mut router, SECOND_SUBSCRIBER_SESSION);
    open_transport(
        &mut router,
        SECOND_SUBSCRIBER_SEND_TRANSPORT,
        SECOND_SUBSCRIBER_SESSION,
        TransportDirection::Send,
    );
    add_compatible_consumer(
        &mut router,
        CONSUMER,
        SUBSCRIBER_SEND_TRANSPORT,
        MediaKind::Video,
    );
    add_compatible_consumer(
        &mut router,
        SECOND_CONSUMER,
        SECOND_SUBSCRIBER_SEND_TRANSPORT,
        MediaKind::Video,
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

    assert_eq!(router.join_session(Session::new(PUBLISHER_SESSION)), Ok(()));

    let session = router.sessions().next();
    assert!(session.is_some());
    let Some(session) = session else {
        panic!("joined session should be visible through the public iterator");
    };
    assert_eq!(session.state(), SessionState::Active);
    assert_router_is_consistent(&router);
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
    let inspector = observer.clone();
    let mut router = Router::new_with_observer(ROUTER, observer);

    join_session(&mut router, PUBLISHER_SESSION);
    join_session(&mut router, SUBSCRIBER_SESSION);
    open_transport(
        &mut router,
        PUBLISHER_RECV_TRANSPORT,
        PUBLISHER_SESSION,
        TransportDirection::Receive,
    );
    open_transport(
        &mut router,
        SUBSCRIBER_SEND_TRANSPORT,
        SUBSCRIBER_SESSION,
        TransportDirection::Send,
    );
    add_producer(
        &mut router,
        PRODUCER,
        PUBLISHER_RECV_TRANSPORT,
        MediaKind::Video,
    );
    assert_eq!(router.remove_session(PUBLISHER_SESSION), Ok(()));
    assert_eq!(router.remove_session(SUBSCRIBER_SESSION), Ok(()));

    assert_eq!(
        inspector.recorded_events(),
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
