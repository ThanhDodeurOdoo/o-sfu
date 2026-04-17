use std::cell::RefCell;
use std::rc::Rc;

use super::router_invariants::assert_router_is_consistent;
use crate::{
    Consumer, ConsumerCapability, ConsumerId, MediaKind, Producer, ProducerId, Router, RouterError,
    RouterEvent, RouterId, RouterObserver, Session, SessionId, SessionPermissionFlags,
    SessionPermissions, SessionState, StreamType, Transport, TransportDirection, TransportId,
};

fn session(id: SessionId) -> Session {
    Session::new(id, SessionPermissions::default())
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
            StreamType::Audio,
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
                StreamType::Audio,
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
            StreamType::Audio,
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
            StreamType::Audio,
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
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_session(SessionId(10)), Ok(()));
    assert_eq!(router.sessions.len(), 1);
    assert_eq!(router.transports.len(), 1);
    assert_eq!(router.producers.len(), 0);
    assert_eq!(router.consumers.len(), 0);
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
            StreamType::Audio,
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
                StreamType::Audio,
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
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.remove_session(SessionId(10)), Ok(()));
    assert!(router.sessions.contains_key(&SessionId(20)));
    assert!(router.transports.contains_key(&TransportId(200)));
    assert!(!router.transports.contains_key(&TransportId(100)));
    assert!(!router.transports.contains_key(&TransportId(101)));
    assert!(!router.producers.contains_key(&ProducerId(300)));
    assert!(!router.consumers.contains_key(&ConsumerId(400)));
    assert!(!router.consumers.contains_key(&ConsumerId(401)));
    assert!(!router.session_transports.contains_key(&SessionId(10)));
    assert!(
        router
            .session_transports
            .get(&SessionId(20))
            .is_some_and(|transport_ids| { transport_ids.contains(&TransportId(200)) })
    );
    assert!(!router.transport_consumers.contains_key(&TransportId(200)));
    assert!(!router.producer_consumers.contains_key(&ProducerId(300)));
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
            StreamType::Audio,
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
            StreamType::Camera,
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
                StreamType::Camera,
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
            StreamType::Audio,
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
                StreamType::Audio,
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
fn consumers_must_match_their_producer_stream_type() {
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
            MediaKind::Video,
            StreamType::Camera,
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
                StreamType::Screen,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(RouterError::ConsumerStreamTypeMismatch {
            producer_id: ProducerId(300),
            expected: StreamType::Camera,
            actual: StreamType::Screen,
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
            StreamType::Audio,
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
                StreamType::Audio,
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
            StreamType::Audio,
        )),
        Ok(())
    );
    assert_eq!(router.set_producer_paused(ProducerId(300), true), Ok(()));

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                ConsumerId(400),
                ProducerId(300),
                TransportId(200),
                MediaKind::Audio,
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    let consumer = router.consumers.get(&ConsumerId(400));
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };
    assert!(!consumer.paused());
    assert!(consumer.producer_paused());
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
            StreamType::Camera,
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
                StreamType::Camera,
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
                StreamType::Camera,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.set_producer_paused(ProducerId(300), true), Ok(()));

    let producer = router.producers.get(&ProducerId(300));
    assert!(producer.is_some());
    let Some(producer) = producer else {
        return;
    };
    assert!(producer.paused());
    assert!(
        router
            .consumers
            .get(&ConsumerId(400))
            .is_some_and(Consumer::producer_paused)
    );
    assert!(
        router
            .consumers
            .get(&ConsumerId(401))
            .is_some_and(Consumer::producer_paused)
    );
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
            StreamType::Audio,
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
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );
    assert_eq!(router.set_producer_paused(ProducerId(300), true), Ok(()));

    assert_eq!(router.set_producer_paused(ProducerId(300), false), Ok(()));

    assert!(
        router
            .producers
            .get(&ProducerId(300))
            .is_some_and(|producer| !producer.paused())
    );
    assert!(
        router
            .consumers
            .get(&ConsumerId(400))
            .is_some_and(|consumer| !consumer.producer_paused())
    );
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
            StreamType::Audio,
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
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Ok(())
    );

    assert_eq!(router.set_consumer_paused(ConsumerId(400), true), Ok(()));

    let consumer = router.consumers.get(&ConsumerId(400));
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };
    assert!(consumer.paused());
    assert!(!consumer.producer_paused());
    assert_router_is_consistent(&router);
}

#[test]
fn joined_sessions_store_permissions_and_active_state() {
    let mut router = Router::new(RouterId(1));
    let permissions = SessionPermissions::from_flags(SessionPermissionFlags {
        transcription: true,
        audio_recording: false,
        video_recording: true,
    });

    assert_eq!(
        router.join_session(Session::new(SessionId(10), permissions)),
        Ok(())
    );

    let session = router.sessions().next();
    assert!(session.is_some());
    let Some(session) = session else {
        return;
    };
    assert_eq!(session.state(), SessionState::Active);
    assert_eq!(session.permissions(), permissions);
}

#[test]
fn router_updates_session_permissions() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(session(SessionId(10))), Ok(()));
    let permissions = SessionPermissions::from_flags(SessionPermissionFlags {
        transcription: true,
        audio_recording: true,
        video_recording: false,
    });
    assert_eq!(
        router.update_session_permissions(SessionId(10), permissions),
        Ok(())
    );

    let session = router.sessions().next();
    assert!(session.is_some());
    let Some(session) = session else {
        return;
    };
    assert_eq!(session.permissions(), permissions);
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
            StreamType::Camera,
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
                stream_type: StreamType::Camera,
            },
            RouterEvent::ProducerRemoved {
                session_id: SessionId(10),
                transport_id: TransportId(100),
                producer_id: ProducerId(300),
                media_kind: MediaKind::Video,
                stream_type: StreamType::Camera,
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
