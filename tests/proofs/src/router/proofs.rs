//! capacity-two symbolic witnesses over the production router facade

use o_sfu_model::UserId;

use crate::{
    ConnectionId, ConsumerId, MediaWorkerId, ProducerId, Router, RouterError, RouterId,
    model::topology::test_support::InvariantView,
    rtp::MediaCapabilities,
    topology::{RoutedConsumerId, RoutedProducerId, RouterPlacement},
};

fn user(id: i64) -> UserId {
    UserId::Integer(id)
}

fn connection(id: u64) -> ConnectionId {
    ConnectionId::from_raw(id)
}

fn placement(router: u64) -> RouterPlacement {
    RouterPlacement {
        router: RouterId(router),
        media_worker: MediaWorkerId::from_raw(0),
    }
}

fn router() -> Router {
    Router::new(RouterId(1), MediaCapabilities::new(Vec::new(), Vec::new()))
}

fn expect_ok<T>(result: Result<T, RouterError>) -> T {
    let ok = result.is_ok();
    assert!(ok);
    kani::assume(ok);
    match result {
        Ok(value) => value,
        Err(_) => panic!("verified router operation failed"),
    }
}

fn join(router: &mut Router, user: &UserId, connection_id: u64, home: u64) {
    let worker = expect_ok(router.commit_session_placement(
        user,
        connection(connection_id),
        placement(home),
    ));
    assert!(worker == MediaWorkerId::from_raw(0));
}

fn add_producer(router: &mut Router, user: &UserId, producer: u64) -> RoutedProducerId {
    expect_ok(router.add_producer(user, ProducerId(producer)))
}

fn add_consumer(
    router: &mut Router,
    user: &UserId,
    consumer: u64,
    producer: RoutedProducerId,
) -> RoutedConsumerId {
    expect_ok(router.add_consumer(user, ConsumerId(consumer), producer))
}

mod flow {
    use super::*;

    #[kani::proof]
    #[kani::unwind(3)]
    fn cross_router_consumer_forms_valid_graph() {
        let mut router = router();
        let first = user(10);
        let second = user(20);
        join(&mut router, &first, 1, 1);
        join(&mut router, &second, 2, 2);
        let first_is_source: bool = kani::any();
        if first_is_source {
            let producer = add_producer(&mut router, &first, 10);
            let consumer = add_consumer(&mut router, &second, 20, producer);
            let view = InvariantView::new(&router);
            assert!(view.has_session(&first, connection(1), RouterId(1)));
            assert!(view.has_empty_session(&second, connection(2), RouterId(2)));
            assert!(view.has_only_route(producer, consumer));
            assert!(view.has_committed_sessions(2));
            assert!(view.has_placement_pair(placement(1), placement(2)));
            assert!(view.session_count(RouterId(1)) == Some(2));
            assert!(view.session_count(RouterId(2)) == Some(1));
            kani::cover!(first_is_source);
            std::mem::forget(router);
        } else {
            let producer = add_producer(&mut router, &second, 10);
            let consumer = add_consumer(&mut router, &first, 20, producer);
            let view = InvariantView::new(&router);
            assert!(view.has_empty_session(&first, connection(1), RouterId(1)));
            assert!(view.has_session(&second, connection(2), RouterId(2)));
            assert!(view.has_only_route(producer, consumer));
            assert!(view.has_committed_sessions(2));
            assert!(view.has_placement_pair(placement(1), placement(2)));
            assert!(view.session_count(RouterId(1)) == Some(1));
            assert!(view.session_count(RouterId(2)) == Some(2));
            kani::cover!(!first_is_source);
            std::mem::forget(router);
        }
    }
}

mod replacement {
    use super::*;

    #[kani::proof]
    #[kani::unwind(5)]
    fn replacement_rehomes_and_retires_graph() {
        let mut router = router();
        let source = user(10);
        join(&mut router, &source, 1, 1);
        let producer = add_producer(&mut router, &source, 10);
        join(&mut router, &source, 2, 2);

        let replaced = InvariantView::new(&router);
        assert!(replaced.has_empty_session(&source, connection(2), RouterId(2)));
        assert!(replaced.has_committed_sessions(1));
        assert!(replaced.has_placement_pair(placement(1), placement(2)));
        assert!(!replaced.has_connection(connection(1)));
        assert!(replaced.has_connection(connection(2)));
        assert!(!replaced.has_producer(producer));
        assert!(replaced.session_count(RouterId(1)) == Some(0));
        assert!(replaced.session_count(RouterId(2)) == Some(1));
        kani::cover!(true);

        std::mem::forget(router);
    }
}

mod teardown {
    use super::*;

    #[kani::proof]
    #[kani::unwind(3)]
    fn removing_consumer_preserves_sibling_and_shadow() {
        let mut router = router();
        let source = user(10);
        let receiver = user(20);
        join(&mut router, &source, 1, 1);
        join(&mut router, &receiver, 2, 2);
        let producer = add_producer(&mut router, &source, 10);
        let first = add_consumer(&mut router, &receiver, 20, producer);
        let second = add_consumer(&mut router, &receiver, 21, producer);
        let remove_first: bool = kani::any();
        if remove_first {
            expect_ok(router.remove_consumer(first));
            let view = InvariantView::new(&router);
            assert!(view.has_session(&source, connection(1), RouterId(1)));
            assert!(view.has_empty_session(&receiver, connection(2), RouterId(2)));
            assert!(view.has_only_route(producer, second));
            assert!(view.has_committed_sessions(2));
            assert!(view.has_placement_pair(placement(1), placement(2)));
            assert!(view.session_count(RouterId(1)) == Some(2));
            assert!(view.session_count(RouterId(2)) == Some(1));
            kani::cover!(remove_first);
            std::mem::forget(router);
        } else {
            expect_ok(router.remove_consumer(second));
            let view = InvariantView::new(&router);
            assert!(view.has_session(&source, connection(1), RouterId(1)));
            assert!(view.has_empty_session(&receiver, connection(2), RouterId(2)));
            assert!(view.has_only_route(producer, first));
            assert!(view.has_committed_sessions(2));
            assert!(view.has_placement_pair(placement(1), placement(2)));
            assert!(view.session_count(RouterId(1)) == Some(2));
            assert!(view.session_count(RouterId(2)) == Some(1));
            kani::cover!(!remove_first);
            std::mem::forget(router);
        }
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn removing_producer_cascades_only_its_dependents() {
        let mut router = router();
        let source = user(10);
        let receiver = user(20);
        join(&mut router, &source, 1, 1);
        join(&mut router, &receiver, 2, 2);
        let first_producer = add_producer(&mut router, &source, 10);
        let second_producer = add_producer(&mut router, &source, 11);
        let first_consumer = add_consumer(&mut router, &receiver, 20, first_producer);
        let second_consumer = add_consumer(&mut router, &receiver, 21, second_producer);
        let remove_first: bool = kani::any();
        if remove_first {
            expect_ok(router.remove_producer(first_producer));
            let view = InvariantView::new(&router);
            assert!(view.has_session(&source, connection(1), RouterId(1)));
            assert!(view.has_empty_session(&receiver, connection(2), RouterId(2)));
            assert!(view.has_only_route(second_producer, second_consumer));
            assert!(view.has_committed_sessions(2));
            assert!(view.has_placement_pair(placement(1), placement(2)));
            assert!(view.session_count(RouterId(1)) == Some(2));
            assert!(view.session_count(RouterId(2)) == Some(1));
            kani::cover!(remove_first);
            std::mem::forget(router);
        } else {
            expect_ok(router.remove_producer(second_producer));
            let view = InvariantView::new(&router);
            assert!(view.has_session(&source, connection(1), RouterId(1)));
            assert!(view.has_empty_session(&receiver, connection(2), RouterId(2)));
            assert!(view.has_only_route(first_producer, first_consumer));
            assert!(view.has_committed_sessions(2));
            assert!(view.has_placement_pair(placement(1), placement(2)));
            assert!(view.session_count(RouterId(1)) == Some(2));
            assert!(view.session_count(RouterId(2)) == Some(1));
            kani::cover!(!remove_first);
            std::mem::forget(router);
        }
    }

    #[kani::proof]
    #[kani::unwind(5)]
    fn receiver_teardown_clears_shadow() {
        let mut router = router();
        let source = user(10);
        let receiver = user(20);
        join(&mut router, &source, 1, 1);
        join(&mut router, &receiver, 2, 2);
        let producer = add_producer(&mut router, &source, 10);
        let consumer = add_consumer(&mut router, &receiver, 20, producer);
        expect_ok(router.remove_session(&receiver));

        let view = InvariantView::new(&router);
        assert!(view.has_session(&source, connection(1), RouterId(1)));
        assert!(view.has_only_producer(producer));
        assert!(view.dependent_count(producer) == Some(0));
        assert!(view.has_committed_sessions(1));
        assert!(view.has_placement_pair(placement(1), placement(2)));
        assert!(!view.has_session(&receiver, connection(2), RouterId(2)));
        assert!(!view.has_connection(connection(2)));
        assert!(!view.has_consumer(consumer));
        assert!(view.session_count(RouterId(1)) == Some(1));
        assert!(view.session_count(RouterId(2)) == Some(0));
        kani::cover!(true);

        std::mem::forget(router);
    }
}
