use super::{ProofRouterModel, model::ProofRouterError};
use crate::{
    Consumer, ConsumerId, MediaKind, Producer, ProducerId, RouterId, Session, SessionId,
    SessionInfo, SessionPermissionFlags, SessionPermissions, StreamType, Transport,
    TransportDirection, TransportId,
};

type ProofRouter = ProofRouterModel<2, 2, 1, 1>;
type PauseProofRouter = ProofRouterModel<3, 3, 1, 2>;

fn session(id: SessionId) -> Session {
    Session::new(id, SessionPermissions::default())
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
    let info = SessionInfo::builder()
        .talking(kani::any())
        .camera_on(kani::any())
        .screen_sharing_on(kani::any())
        .self_muted(kani::any())
        .deaf(kani::any())
        .raising_hand(kani::any())
        .build();

    let _ = router.join_session(session(session_id));
    let _ = router.update_session_permissions(session_id, permissions);
    let _ = router.update_session_info(session_id, info);

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
        true,
    );

    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn session_teardown_preserves_invariants() {
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
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(40),
            ProducerId(30),
            TransportId(20),
            MediaKind::Audio,
            StreamType::Audio,
        ),
        true,
    );

    let _ = router.remove_session(SessionId(1));

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
            crate::RouterError::ProducerRequiresReceiveTransport(transport_id),
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
            true,
        ),
        Err(ProofRouterError::Router(
            crate::RouterError::ConsumerRequiresSendTransport(consumer_transport),
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
            true,
        ),
        Err(ProofRouterError::Router(
            crate::RouterError::ConsumerMediaKindMismatch {
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
            true,
        ),
        Err(ProofRouterError::Router(
            crate::RouterError::ConsumerStreamTypeMismatch {
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
        true,
    );

    assert!(
        router
            .consumers
            .iter()
            .flatten()
            .all(|consumer| consumer.producer_paused())
    );
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
        true,
    );
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(41),
            ProducerId(30),
            TransportId(21),
            MediaKind::Video,
            StreamType::Camera,
        ),
        true,
    );

    let _ = router.set_producer_paused(ProducerId(30), true);

    assert!(
        router
            .consumers
            .iter()
            .flatten()
            .all(|consumer| consumer.producer_paused())
    );
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
        true,
    );
    let _ = router.add_consumer(
        Consumer::new(
            ConsumerId(41),
            ProducerId(30),
            TransportId(21),
            MediaKind::Video,
            StreamType::Camera,
        ),
        true,
    );
    let _ = router.set_producer_paused(ProducerId(30), true);

    let _ = router.set_producer_paused(ProducerId(30), false);

    assert!(
        router
            .consumers
            .iter()
            .flatten()
            .all(|consumer| !consumer.producer_paused())
    );
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
            false,
        ),
        Err(ProofRouterError::Router(
            crate::RouterError::IncompatibleCapabilities { producer_id },
        )),
    );
    assert!(router.satisfies_invariants());
}
