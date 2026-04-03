use super::helpers::assert_router_is_consistent;
use crate::{
    Consumer, ConsumerId, Producer, ProducerId, Router, RouterError, RouterId, Session, SessionId,
    Transport, TransportDirection, TransportId,
};

#[test]
fn router_accepts_a_basic_publish_and_subscribe_flow() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(Session::new(SessionId(20))), Ok(()));
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
        router.add_producer(Producer::new(ProducerId(300), TransportId(100))),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(Consumer::new(
            ConsumerId(400),
            ProducerId(300),
            TransportId(200),
        )),
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
        router.add_producer(Producer::new(ProducerId(300), TransportId(100))),
        Err(RouterError::MissingTransport(TransportId(100)))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn removing_a_session_cleans_dependent_resources() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(Session::new(SessionId(20))), Ok(()));
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
        router.add_producer(Producer::new(ProducerId(300), TransportId(100))),
        Ok(())
    );
    assert_eq!(
        router.add_consumer(Consumer::new(
            ConsumerId(400),
            ProducerId(300),
            TransportId(200),
        )),
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
fn producers_must_use_receive_transports() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(
            TransportId(100),
            SessionId(10),
            TransportDirection::Send,
        )),
        Ok(())
    );

    assert_eq!(
        router.add_producer(Producer::new(ProducerId(300), TransportId(100))),
        Err(RouterError::ProducerRequiresReceiveTransport(TransportId(
            100
        )))
    );
    assert_router_is_consistent(&router);
}

#[test]
fn consumers_must_use_send_transports() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(Session::new(SessionId(20))), Ok(()));
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
        router.add_producer(Producer::new(ProducerId(300), TransportId(100))),
        Ok(())
    );

    assert_eq!(
        router.add_consumer(Consumer::new(
            ConsumerId(400),
            ProducerId(300),
            TransportId(200),
        )),
        Err(RouterError::ConsumerRequiresSendTransport(TransportId(200)))
    );
    assert_router_is_consistent(&router);
}
