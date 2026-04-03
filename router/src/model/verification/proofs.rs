use super::{ProofRouterModel, model::ProofRouterError};
use crate::{
    Consumer, ConsumerId, Producer, ProducerId, RouterId, Session, SessionId, Transport,
    TransportDirection, TransportId,
};

type ProofRouter = ProofRouterModel<2, 2, 1, 1>;

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
    let _ = router.add_producer(Producer::new(producer, transport_a));
    let _ = router.add_consumer(Consumer::new(consumer, producer, transport_b));

    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn session_teardown_preserves_invariants() {
    let mut router = ProofRouter::new(RouterId(0));

    let _ = router.join_session(Session::new(SessionId(1)));
    let _ = router.join_session(Session::new(SessionId(2)));
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
    let _ = router.add_producer(Producer::new(ProducerId(30), TransportId(10)));
    let _ = router.add_consumer(Consumer::new(
        ConsumerId(40),
        ProducerId(30),
        TransportId(20),
    ));

    let _ = router.remove_session(SessionId(1));

    assert!(router.satisfies_invariants());
}

#[kani::proof]
fn producers_are_rejected_on_send_transports() {
    let mut router = ProofRouter::new(RouterId(0));

    let session_id = SessionId(kani::any());
    let transport_id = TransportId(kani::any());
    let producer_id = ProducerId(kani::any());

    let _ = router.join_session(Session::new(session_id));
    let _ = router.open_transport(Transport::new(
        transport_id,
        session_id,
        TransportDirection::Send,
    ));

    assert_eq!(
        router.add_producer(Producer::new(producer_id, transport_id)),
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

    let _ = router.join_session(Session::new(session_a));
    let _ = router.join_session(Session::new(session_b));
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
    let _ = router.add_producer(Producer::new(producer_id, producer_transport));

    assert_eq!(
        router.add_consumer(Consumer::new(consumer_id, producer_id, consumer_transport,)),
        Err(ProofRouterError::Router(
            crate::RouterError::ConsumerRequiresSendTransport(consumer_transport),
        )),
    );
    assert!(router.satisfies_invariants());
}
