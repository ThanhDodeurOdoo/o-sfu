use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, MediaKind, Producer, ProducerId, RouterId, Session,
    SessionId, SessionPermissions, StreamType, Transport, TransportDirection, TransportId,
};

use super::ProofRouterModel;

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

// Proves session teardown is transitive and exact: removing one session must
// clear its transports, producers, and consumers, and also remove dependent
// routes that point at those producers, while leaving unrelated session state
// consistent. This is high value because stale reverse-index entries here would
// poison later routing decisions long after the teardown.
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

// Proves producer removal only tears down the routes that depend on that
// producer and does not over-delete still-live transports or sessions. This is
// worth proving because producer teardown is a fan-out operation where stale
// dependents and accidental collateral cleanup are both easy regression risks.
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

// Proves consumer removal is local: deleting one subscription must update the
// producer and transport reverse indices for that consumer only, while keeping
// sibling routes intact. This matters because consumer churn is common and a
// sloppy removal path can silently break unrelated deliveries.
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

// Proves a newly created consumer starts with the producer's current pause
// shadow instead of assuming the producer is live. This is high value because a
// stale initial pause shadow would expose consumers to media that should still
// be considered paused until the next explicit producer update.
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

// Proves pausing one producer propagates to every dependent consumer shadow,
// not just the first or most recent one. This matters because the pause fan-out
// path is easy to under-update, and sampled tests do not exhaustively protect
// against missing one dependent route.
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

// Proves resuming a producer clears the producer-pause shadow on every
// dependent consumer again. This is valuable because pause propagation has to
// work in both directions; otherwise consumers can get stuck in a phantom
// paused state after the upstream producer has resumed.
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

// Proves a consumer's own local pause bit is independent from producer pause
// shadow updates: producer pauses may toggle the shadow, but they must not
// erase an explicit local pause. This is worth proving because the router keeps
// two pause causes, and mixing them would create incorrect user-visible state.
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
