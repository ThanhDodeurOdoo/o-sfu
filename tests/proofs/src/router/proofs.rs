use super::{ProofRouterModel, model::ProofRouterError};
use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, MediaKind, Producer, ProducerId, RouterId, Session,
    SessionId, SessionPermissionFlags, SessionPermissions, StreamType, Transport,
    TransportDirection, TransportId,
};

type ProofRouter = ProofRouterModel<2, 2, 1, 1>;
type PauseProofRouter = ProofRouterModel<3, 3, 1, 2>;
type TeardownProofRouter = ProofRouterModel<2, 3, 1, 2>;

fn session(id: SessionId) -> Session {
    Session::new(id, SessionPermissions::default())
}

fn all_consumers_shadow_pause<
    const MAX_SESSIONS: usize,
    const MAX_TRANSPORTS: usize,
    const MAX_PRODUCERS: usize,
    const MAX_CONSUMERS: usize,
>(
    router: &ProofRouterModel<MAX_SESSIONS, MAX_TRANSPORTS, MAX_PRODUCERS, MAX_CONSUMERS>,
    paused: bool,
) -> bool {
    let mut consumer_index = 0;
    while let Some(consumer_slot) = router.consumers.get(consumer_index) {
        if let Some(consumer) = consumer_slot
            && consumer.producer_paused() != paused
        {
            return false;
        }
        consumer_index += 1;
    }
    true
}

#[kani::proof]
fn join_session_preserves_invariants() {
    let mut router = ProofRouter::new(RouterId(0));
    let _ = router.join_session(session(SessionId(kani::any())));
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn session_updates_preserve_invariants() {
    let mut router = ProofRouter::new(RouterId(0));
    let session_id = SessionId(kani::any());
    let permissions = SessionPermissions::from_flags(SessionPermissionFlags {
        transcription: kani::any(),
        audio_recording: kani::any(),
        video_recording: kani::any(),
    });

    let _ = router.join_session(session(session_id));
    let _ = router.update_session_permissions(session_id, permissions);

    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn routing_flow_preserves_invariants() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_a = SessionId(kani::any());
    let session_b = SessionId(kani::any());
    let transport_a = TransportId(kani::any());
    let transport_b = TransportId(kani::any());
    let producer = ProducerId(kani::any());
    let consumer = ConsumerId(kani::any());

    kani::assume(session_a != session_b);
    kani::assume(transport_a != transport_b);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        transport_a,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        transport_b,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer,
        transport_a,
        MediaKind::Audio,
        StreamType::Audio,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            consumer,
            producer,
            transport_b,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );

    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn session_teardown_clears_reverse_indices_and_dependents() {
    let mut router = TeardownProofRouter::new(RouterId(0));

    let session_a = SessionId(1);
    let session_b = SessionId(2);
    let receive_transport = TransportId(10);
    let shared_send_transport = TransportId(11);
    let survivor_send_transport = TransportId(20);
    let producer_id = ProducerId(30);
    let removed_consumer_id = ConsumerId(40);
    let surviving_consumer_id = ConsumerId(41);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        receive_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        shared_send_transport,
        session_a,
        TransportDirection::Send,
    ));
    let _ = router.open_transport(Transport::new(
        survivor_send_transport,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        receive_transport,
        MediaKind::Audio,
        StreamType::Audio,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            removed_consumer_id,
            producer_id,
            shared_send_transport,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );
    let _ = router.add_consumer(
        Consumer::new(
            surviving_consumer_id,
            producer_id,
            survivor_send_transport,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );

    let _ = router.remove_session(session_a);

    assert!(!router.contains_session(session_a));
    assert!(router.contains_session(session_b));
    assert!(!router.contains_transport(receive_transport));
    assert!(!router.contains_transport(shared_send_transport));
    assert!(router.contains_transport(survivor_send_transport));
    assert!(!router.contains_producer(producer_id));
    assert!(router.consumer_by_id(removed_consumer_id).is_none());
    assert!(router.consumer_by_id(surviving_consumer_id).is_none());
    assert!(!router.session_transports.contains_key(session_a));
    assert!(router.session_transports.contains_key(session_b));
    assert_eq!(router.session_transports.member_count(session_b), 1);
    assert!(
        router
            .session_transports
            .contains_member(session_b, survivor_send_transport)
    );
    assert!(!router.transport_producers.contains_key(receive_transport));
    assert!(
        !router
            .transport_consumers
            .contains_key(shared_send_transport)
    );
    assert!(
        !router
            .transport_consumers
            .contains_key(survivor_send_transport)
    );
    assert!(!router.producer_consumers.contains_key(producer_id));
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn removing_a_producer_clears_dependents_but_keeps_live_transports() {
    let mut router = TeardownProofRouter::new(RouterId(0));

    let session_a = SessionId(1);
    let session_b = SessionId(2);
    let receive_transport = TransportId(10);
    let same_session_send_transport = TransportId(11);
    let remote_send_transport = TransportId(20);
    let producer_id = ProducerId(30);
    let same_session_consumer = ConsumerId(40);
    let remote_consumer = ConsumerId(41);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        receive_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        same_session_send_transport,
        session_a,
        TransportDirection::Send,
    ));
    let _ = router.open_transport(Transport::new(
        remote_send_transport,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        receive_transport,
        MediaKind::Audio,
        StreamType::Audio,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            same_session_consumer,
            producer_id,
            same_session_send_transport,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );
    let _ = router.add_consumer(
        Consumer::new(
            remote_consumer,
            producer_id,
            remote_send_transport,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );

    let _ = router.remove_producer(producer_id);

    assert!(router.contains_session(session_a));
    assert!(router.contains_session(session_b));
    assert!(router.contains_transport(receive_transport));
    assert!(router.contains_transport(same_session_send_transport));
    assert!(router.contains_transport(remote_send_transport));
    assert!(!router.contains_producer(producer_id));
    assert!(router.consumer_by_id(same_session_consumer).is_none());
    assert!(router.consumer_by_id(remote_consumer).is_none());
    assert!(!router.transport_producers.contains_key(receive_transport));
    assert!(
        !router
            .transport_consumers
            .contains_key(same_session_send_transport)
    );
    assert!(
        !router
            .transport_consumers
            .contains_key(remote_send_transport)
    );
    assert!(!router.producer_consumers.contains_key(producer_id));
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn removing_a_consumer_preserves_other_routes_and_indices() {
    let mut router = TeardownProofRouter::new(RouterId(0));

    let session_a = SessionId(1);
    let session_b = SessionId(2);
    let receive_transport = TransportId(10);
    let removed_consumer_transport = TransportId(11);
    let surviving_consumer_transport = TransportId(20);
    let producer_id = ProducerId(30);
    let removed_consumer_id = ConsumerId(40);
    let surviving_consumer_id = ConsumerId(41);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        receive_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        removed_consumer_transport,
        session_a,
        TransportDirection::Send,
    ));
    let _ = router.open_transport(Transport::new(
        surviving_consumer_transport,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        receive_transport,
        MediaKind::Audio,
        StreamType::Audio,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            removed_consumer_id,
            producer_id,
            removed_consumer_transport,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );
    let _ = router.add_consumer(
        Consumer::new(
            surviving_consumer_id,
            producer_id,
            surviving_consumer_transport,
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );

    let _ = router.remove_consumer(removed_consumer_id);

    assert!(router.contains_producer(producer_id));
    assert!(router.contains_transport(receive_transport));
    assert!(router.contains_transport(removed_consumer_transport));
    assert!(router.contains_transport(surviving_consumer_transport));
    assert!(router.consumer_by_id(removed_consumer_id).is_none());
    assert!(router.consumer_by_id(surviving_consumer_id).is_some());
    assert!(
        !router
            .transport_consumers
            .contains_key(removed_consumer_transport)
    );
    assert!(
        router
            .transport_consumers
            .contains_member(surviving_consumer_transport, surviving_consumer_id)
    );
    assert!(
        router
            .producer_consumers
            .contains_member(producer_id, surviving_consumer_id)
    );
    assert!(
        !router
            .producer_consumers
            .contains_member(producer_id, removed_consumer_id)
    );
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn producers_are_rejected_on_send_transports() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_id = SessionId(kani::any());
    let transport_id = TransportId(kani::any());
    let producer_id = ProducerId(kani::any());

    let _ = router.join_session(session(session_id));
    let _ = router.open_transport(Transport::new(
        transport_id,
        session_id,
        TransportDirection::Send,
    ));

    assert_eq!(
        router.add_producer(Producer::new(
            producer_id,
            transport_id,
            MediaKind::Audio,
            StreamType::Audio,
        )),
        Err(ProofRouterError::Router(
            o_sfu_router::RouterError::ProducerRequiresReceiveTransport(transport_id),
        )),
    );
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn consumers_are_rejected_on_receive_transports() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_a = SessionId(kani::any());
    let session_b = SessionId(kani::any());
    let producer_transport = TransportId(kani::any());
    let consumer_transport = TransportId(kani::any());
    let producer_id = ProducerId(kani::any());
    let consumer_id = ConsumerId(kani::any());

    kani::assume(session_a != session_b);
    kani::assume(producer_transport != consumer_transport);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        producer_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        consumer_transport,
        session_b,
        TransportDirection::Receive,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        producer_transport,
        MediaKind::Audio,
        StreamType::Audio,
    ));

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                consumer_id,
                producer_id,
                consumer_transport,
                MediaKind::Audio,
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(ProofRouterError::Router(
            o_sfu_router::RouterError::ConsumerRequiresSendTransport(consumer_transport),
        )),
    );
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn consumers_are_rejected_when_media_kind_differs_from_producer() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_a = SessionId(kani::any());
    let session_b = SessionId(kani::any());
    let producer_transport = TransportId(kani::any());
    let consumer_transport = TransportId(kani::any());
    let producer_id = ProducerId(kani::any());
    let consumer_id = ConsumerId(kani::any());

    kani::assume(session_a != session_b);
    kani::assume(producer_transport != consumer_transport);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        producer_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        consumer_transport,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        producer_transport,
        MediaKind::Audio,
        StreamType::Audio,
    ));

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                consumer_id,
                producer_id,
                consumer_transport,
                MediaKind::Video,
                StreamType::Audio,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(ProofRouterError::Router(
            o_sfu_router::RouterError::ConsumerMediaKindMismatch {
                producer_id,
                expected: MediaKind::Audio,
                actual: MediaKind::Video,
            },
        )),
    );
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn consumers_are_rejected_when_stream_type_differs_from_producer() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_a = SessionId(kani::any());
    let session_b = SessionId(kani::any());
    let producer_transport = TransportId(kani::any());
    let consumer_transport = TransportId(kani::any());
    let producer_id = ProducerId(kani::any());
    let consumer_id = ConsumerId(kani::any());

    kani::assume(session_a != session_b);
    kani::assume(producer_transport != consumer_transport);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        producer_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        consumer_transport,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        producer_transport,
        MediaKind::Video,
        StreamType::Camera,
    ));

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                consumer_id,
                producer_id,
                consumer_transport,
                MediaKind::Video,
                StreamType::Screen,
            ),
            ConsumerCapability::Compatible,
        ),
        Err(ProofRouterError::Router(
            o_sfu_router::RouterError::ConsumerStreamTypeMismatch {
                producer_id,
                expected: StreamType::Camera,
                actual: StreamType::Screen,
            },
        )),
    );
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn new_consumers_inherit_their_producer_pause_shadow() {
    let mut router = ProofRouter::new(RouterId(0));

    let _ = router.join_session(session(SessionId(1)));
    let _ = router.join_session(session(SessionId(2)));
    let _ = router.open_transport(Transport::new(
        TransportId(10),
        SessionId(1),
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        TransportId(20),
        SessionId(2),
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        ProducerId(30),
        TransportId(10),
        MediaKind::Audio,
        StreamType::Audio,
    ));
    let _ = router.set_producer_paused(ProducerId(30), true);
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(40),
            ProducerId(30),
            TransportId(20),
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );

    assert!(all_consumers_shadow_pause(&router, true));
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn pausing_a_producer_updates_all_dependent_consumers() {
    let mut router = PauseProofRouter::new(RouterId(0));

    let _ = router.join_session(session(SessionId(1)));
    let _ = router.join_session(session(SessionId(2)));
    let _ = router.join_session(session(SessionId(3)));
    let _ = router.open_transport(Transport::new(
        TransportId(10),
        SessionId(1),
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        TransportId(20),
        SessionId(2),
        TransportDirection::Send,
    ));
    let _ = router.open_transport(Transport::new(
        TransportId(21),
        SessionId(3),
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        ProducerId(30),
        TransportId(10),
        MediaKind::Video,
        StreamType::Camera,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(40),
            ProducerId(30),
            TransportId(20),
            MediaKind::Video,
            StreamType::Camera,
        ),
        ConsumerCapability::Compatible,
    );
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(41),
            ProducerId(30),
            TransportId(21),
            MediaKind::Video,
            StreamType::Camera,
        ),
        ConsumerCapability::Compatible,
    );

    let _ = router.set_producer_paused(ProducerId(30), true);

    assert!(all_consumers_shadow_pause(&router, true));
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn resuming_a_producer_clears_dependent_consumer_pause_shadows() {
    let mut router = PauseProofRouter::new(RouterId(0));

    let _ = router.join_session(session(SessionId(1)));
    let _ = router.join_session(session(SessionId(2)));
    let _ = router.join_session(session(SessionId(3)));
    let _ = router.open_transport(Transport::new(
        TransportId(10),
        SessionId(1),
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        TransportId(20),
        SessionId(2),
        TransportDirection::Send,
    ));
    let _ = router.open_transport(Transport::new(
        TransportId(21),
        SessionId(3),
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        ProducerId(30),
        TransportId(10),
        MediaKind::Video,
        StreamType::Camera,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(40),
            ProducerId(30),
            TransportId(20),
            MediaKind::Video,
            StreamType::Camera,
        ),
        ConsumerCapability::Compatible,
    );
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(41),
            ProducerId(30),
            TransportId(21),
            MediaKind::Video,
            StreamType::Camera,
        ),
        ConsumerCapability::Compatible,
    );
    let _ = router.set_producer_paused(ProducerId(30), true);

    let _ = router.set_producer_paused(ProducerId(30), false);

    assert!(all_consumers_shadow_pause(&router, false));
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn consumer_local_pause_stays_independent_from_producer_shadow_updates() {
    let mut router = PauseProofRouter::new(RouterId(0));

    let _ = router.join_session(session(SessionId(1)));
    let _ = router.join_session(session(SessionId(2)));
    let _ = router.open_transport(Transport::new(
        TransportId(10),
        SessionId(1),
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        TransportId(20),
        SessionId(2),
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        ProducerId(30),
        TransportId(10),
        MediaKind::Audio,
        StreamType::Audio,
    ));
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(40),
            ProducerId(30),
            TransportId(20),
            MediaKind::Audio,
            StreamType::Audio,
        ),
        ConsumerCapability::Compatible,
    );

    let _ = router.set_consumer_paused(ConsumerId(40), true);
    let consumer = router.consumer_by_id(ConsumerId(40));
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };
    assert!(consumer.paused());
    assert!(!consumer.producer_paused());

    let _ = router.set_producer_paused(ProducerId(30), true);
    let consumer = router.consumer_by_id(ConsumerId(40));
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };
    assert!(consumer.paused());
    assert!(consumer.producer_paused());

    let _ = router.set_producer_paused(ProducerId(30), false);
    let consumer = router.consumer_by_id(ConsumerId(40));
    assert!(consumer.is_some());
    let Some(consumer) = consumer else {
        return;
    };
    assert!(consumer.paused());
    assert!(!consumer.producer_paused());
    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn consumers_are_rejected_when_capabilities_are_incompatible() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_a = SessionId(kani::any());
    let session_b = SessionId(kani::any());
    let producer_transport = TransportId(kani::any());
    let consumer_transport = TransportId(kani::any());
    let producer_id = ProducerId(kani::any());
    let consumer_id = ConsumerId(kani::any());

    kani::assume(session_a != session_b);
    kani::assume(producer_transport != consumer_transport);

    let _ = router.join_session(session(session_a));
    let _ = router.join_session(session(session_b));
    let _ = router.open_transport(Transport::new(
        producer_transport,
        session_a,
        TransportDirection::Receive,
    ));
    let _ = router.open_transport(Transport::new(
        consumer_transport,
        session_b,
        TransportDirection::Send,
    ));
    let _ = router.add_producer(Producer::new(
        producer_id,
        producer_transport,
        MediaKind::Audio,
        StreamType::Audio,
    ));

    assert_eq!(
        router.add_consumer(
            Consumer::new(
                consumer_id,
                producer_id,
                consumer_transport,
                MediaKind::Audio,
                StreamType::Audio,
            ),
            ConsumerCapability::Incompatible,
        ),
        Err(ProofRouterError::Router(
            o_sfu_router::RouterError::IncompatibleCapabilities { producer_id },
        )),
    );
    assert!(router.satisfies_invariants());
}
