use std::{cell::RefCell, rc::Rc};

use super::router_invariants::assert_router_is_consistent;
use crate::{
    Consumer, ConsumerCapability, ConsumerId, ConsumerRouteState, MediaKind, Producer, ProducerId,
    ProducerRouteState, Router, RouterError, RouterEvent, RouterId, RouterObserver, Session,
    SessionId, SessionState, Transport, TransportDirection, TransportId,
    model::test_support::router_state_snapshot,
};

fn session(id: SessionId) -> Session {
    Session::new(id)
}

#[test]
fn router_accepts_a_basic_publish_and_subscribe_flow() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_router_is_consistent(&router);
}

#[test]
fn router_rejects_orphan_resources() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Err(RouterError::MissingSession(SessionId(10)))
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Err(RouterError::MissingTransport(TransportId(100)))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_session_cleans_dependent_resources() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_session(SessionId(10)), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert_eq!(snapshot.session_count(), 1);
    assert_eq!(snapshot.transport_count(), 1);
    assert_eq!(snapshot.producer_count(), 0);
    assert_eq!(snapshot.consumer_count(), 0);
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_producer_cleans_dependent_consumers_but_keeps_transports() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(101),
            SessionId(10),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(101),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(401),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_producer(ProducerId(300)), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert!(!snapshot.contains_producer(ProducerId(300)));
    assert!(!snapshot.contains_consumer(ConsumerId(400)));
    assert!(!snapshot.contains_consumer(ConsumerId(401)));
    assert!(snapshot.contains_transport(TransportId(100)));
    assert!(snapshot.contains_transport(TransportId(101)));
    assert!(snapshot.contains_transport(TransportId(200)));
    assert!(!snapshot.has_transport_producer_index(TransportId(100)));
    assert!(!snapshot.has_transport_consumer_index(TransportId(101)));
    assert!(!snapshot.has_transport_consumer_index(TransportId(200)));
    assert!(!snapshot.has_producer_consumer_index(ProducerId(300)));
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_producer_rejects_missing_owning_transport() {
    let observer = EventCaptureObserver::default();
    let inspector = observer.clone();
    let mut router = Router::new_with_observer(RouterId(1), observer);

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );

    router.transports.remove(&TransportId(100));

    assert_eq!(
        router.remove_producer(ProducerId(300)),
        Err(RouterError::MissingProducerTransport {
            producer_id: ProducerId(300),
            transport_id: TransportId(100),
        })
    );
    assert!(router_state_snapshot(&router).contains_producer(ProducerId(300)));
    assert_eq!(
        inspector.recorded_events(),
        vec![
            RouterEvent::SessionJoined {
                session_id: SessionId(10),
            },
            RouterEvent::ProducerAdded {
                session_id: SessionId(10),
                transport_id: TransportId(100),
                producer_id: ProducerId(300),
                media_kind: MediaKind::Audio,
            },
        ]
    );
}

#[test]
fn removing_a_consumer_preserves_other_routes() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(101),
            SessionId(10),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(101),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(401),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_consumer(ConsumerId(400)), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert!(!snapshot.contains_consumer(ConsumerId(400)));
    assert!(snapshot.contains_consumer(ConsumerId(401)));
    assert!(snapshot.contains_producer(ProducerId(300)));
    assert!(!snapshot.has_transport_consumer_index(TransportId(101)));
    assert!(snapshot.has_transport_consumer(TransportId(200), ConsumerId(401)));
    assert!(snapshot.has_producer_consumer(ProducerId(300), ConsumerId(401)));
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_session_clears_cross_session_reverse_indices() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(101),
            SessionId(10),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(101),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(401),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_session(SessionId(10)), Ok(()));
    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.contains_session(SessionId(20)));
    assert!(snapshot.contains_transport(TransportId(200)));
    assert!(!snapshot.contains_transport(TransportId(100)));
    assert!(!snapshot.contains_transport(TransportId(101)));
    assert!(!snapshot.contains_producer(ProducerId(300)));
    assert!(!snapshot.contains_consumer(ConsumerId(400)));
    assert!(!snapshot.contains_consumer(ConsumerId(401)));
    assert!(!snapshot.has_session_transport_index(SessionId(10)));
    assert!(snapshot.has_session_transport(SessionId(20), TransportId(200)));
    assert!(!snapshot.has_transport_consumer_index(TransportId(200)));
    assert!(!snapshot.has_producer_consumer_index(ProducerId(300)));
    assert_router_is_consistent(&router);
}

#[test]
fn producers_must_use_receive_transports() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Send,
        )),
        Ok(())
    );

    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Err(RouterError::ProducerRequiresReceiveTransport(TransportId(
            100
        )))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_must_use_send_transports() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Video,
        )),
        Ok(())
    );

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(RouterError::ConsumerRequiresSendTransport(TransportId(200)))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_must_match_their_producer_media_kind() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(RouterError::ConsumerMediaKindMismatch {
            producer_id: ProducerId(300),
            expected: MediaKind::Audio,
            actual: MediaKind::Video,
        })
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_are_rejected_when_capabilities_are_incompatible() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Incompatible,
        ),
        Err(RouterError::IncompatibleCapabilities {
            producer_id: ProducerId(300),
        })
    );
    assert_router_is_consistent(&router);
}

#[test]
fn new_consumers_inherit_their_producer_pause_state() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.set_producer_route_state(ProducerId(300), ProducerRouteState::Paused),
        Ok(())
    );

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.consumer_route_matches(
        ConsumerId(400),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn pausing_a_producer_updates_all_dependent_consumers() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(30))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(201),
            SessionId(30),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Video,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(401),
                ProducerId(300),
                TransportId(201),
                MediaKind::Video,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(
        router.set_producer_route_state(ProducerId(300), ProducerRouteState::Paused),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.producer_route_state_matches(ProducerId(300), ProducerRouteState::Paused,));
    assert!(snapshot.consumer_route_matches(
        ConsumerId(400),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(snapshot.consumer_route_matches(
        ConsumerId(401),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn resuming_a_producer_clears_dependent_consumer_pause_shadows() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );
    assert_eq!(
        router.set_producer_route_state(ProducerId(300), ProducerRouteState::Paused),
        Ok(())
    );

    assert_eq!(
        router.set_producer_route_state(ProducerId(300), ProducerRouteState::Active),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.producer_route_state_matches(ProducerId(300), ProducerRouteState::Active,));
    assert!(snapshot.consumer_route_matches(
        ConsumerId(400),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn pausing_a_consumer_only_changes_its_local_pause_flag() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Audio,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(
        router.set_consumer_route_state(ConsumerId(400), ConsumerRouteState::Paused),
        Ok(())
    );
    assert_eq!(
        router.set_producer_route_state(ProducerId(300), ProducerRouteState::Paused),
        Ok(())
    );
    assert_eq!(
        router.set_producer_route_state(ProducerId(300), ProducerRouteState::Active),
        Ok(())
    );

    let snapshot = router_state_snapshot(&router);
    assert!(snapshot.consumer_route_matches(
        ConsumerId(400),
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    ));
    assert_router_is_consistent(&router);
}

#[test]
fn joined_sessions_store_only_router_lifecycle_state() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));

    let session = router.sessions().next();
    assert!(session.is_some());
    let Some(session) = session else {
        return;
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
    let mut router = Router::new_with_observer(RouterId(1), observer);

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(session(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Receive,
        )),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(200),
            SessionId(20),
            TransportDirection::Send,
        )),
        Ok(())
    );
    assert_eq!(
        router.add_producer(Producer::new(
            ProducerId(300),
            TransportId(100),
            MediaKind::Video,
        )),
        Ok(())
    );
    assert_eq!(router.remove_session(SessionId(10)), Ok(()));
    assert_eq!(router.remove_session(SessionId(20)), Ok(()));

    assert_eq!(
        inspector.recorded_events(),
        vec![
            RouterEvent::SessionJoined {
                session_id: SessionId(10),
            },
            RouterEvent::SessionJoined {
                session_id: SessionId(20),
            },
            RouterEvent::ProducerAdded {
                session_id: SessionId(10),
                transport_id: TransportId(100),
                producer_id: ProducerId(300),
                media_kind: MediaKind::Video,
            },
            RouterEvent::ProducerRemoved {
                session_id: SessionId(10),
                transport_id: TransportId(100),
                producer_id: ProducerId(300),
                media_kind: MediaKind::Video,
            },
            RouterEvent::SessionLeft {
                session_id: SessionId(10),
            },
            RouterEvent::SessionLeft {
                session_id: SessionId(20),
            },
        ]
    );
}
