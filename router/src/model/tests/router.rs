use super::helpers::count_present;
use crate::{
    Consumer, ConsumerId, Producer, ProducerId, Router, RouterError, RouterId, Session, SessionId,
    Transport, TransportId,
};

#[test]
fn router_accepts_a_basic_publish_and_subscribe_flow() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(Session::new(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(TransportId(100), SessionId(10))),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(TransportId(200), SessionId(20))),
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

    assert!(router.satisfies_invariants());
}

#[test]
fn router_rejects_orphan_resources() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(
        router.open_transport(Transport::new(TransportId(100), SessionId(10))),
        Err(RouterError::MissingSession(SessionId(10)))
    );
    assert_eq!(
        router.add_producer(Producer::new(ProducerId(300), TransportId(100))),
        Err(RouterError::MissingTransport(TransportId(100)))
    );
    assert!(router.satisfies_invariants());
}

#[test]
fn removing_a_session_cleans_dependent_resources() {
    let mut router = Router::new(RouterId(1));

    assert_eq!(router.join_session(Session::new(SessionId(10))), Ok(()));
    assert_eq!(router.join_session(Session::new(SessionId(20))), Ok(()));
    assert_eq!(
        router.open_transport(Transport::new(TransportId(100), SessionId(10))),
        Ok(())
    );
    assert_eq!(
        router.open_transport(Transport::new(TransportId(200), SessionId(20))),
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
    assert_eq!(count_present(&router.sessions), 1);
    assert_eq!(count_present(&router.transports), 1);
    assert_eq!(count_present(&router.producers), 0);
    assert_eq!(count_present(&router.consumers), 0);
    assert!(router.satisfies_invariants());
}
