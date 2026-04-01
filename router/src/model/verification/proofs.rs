use crate::{
    Consumer, ConsumerId, Producer, ProducerId, RouterId, RouterModel, Session, SessionId,
    Transport, TransportId,
};

type ProofRouter = RouterModel<2, 2, 1, 1>;

#[kani::proof]
fn join_session_preserves_invariants() {
    let mut router = ProofRouter::new(RouterId(0));
    let _ = router.join_session(Session::new(SessionId(kani::any())));
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

    let _ = router.join_session(Session::new(session_a));
    let _ = router.join_session(Session::new(session_b));
    let _ = router.open_transport(Transport::new(transport_a, session_a));
    let _ = router.open_transport(Transport::new(transport_b, session_b));
    let _ = router.add_producer(Producer::new(producer, transport_a));
    let _ = router.add_consumer(Consumer::new(consumer, producer, transport_b));

    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn session_teardown_preserves_invariants() {
    let mut router = ProofRouter::new(RouterId(0));

    let _ = router.join_session(Session::new(SessionId(1)));
    let _ = router.join_session(Session::new(SessionId(2)));
    let _ = router.open_transport(Transport::new(TransportId(10), SessionId(1)));
    let _ = router.open_transport(Transport::new(TransportId(20), SessionId(2)));
    let _ = router.add_producer(Producer::new(ProducerId(30), TransportId(10)));
    let _ = router.add_consumer(Consumer::new(
        ConsumerId(40),
        ProducerId(30),
        TransportId(20),
    ));

    let _ = router.remove_session(SessionId(1));

    assert!(router.satisfies_invariants());
}
