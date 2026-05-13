use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, ConsumerRouteState, MediaKind, Producer, ProducerId,
    ProducerRouteState, Router, RouterId, Session, SessionId, Transport, TransportDirection,
    TransportId,
    test_support::{
        router_consumer_count, router_consumer_origin_matches, router_consumer_route_matches,
        router_consumer_shadows_producer, router_contains_consumer, router_contains_producer,
        router_contains_session, router_contains_transport, router_has_producer_consumer,
        router_has_producer_consumer_index, router_has_session_transport,
        router_has_session_transport_index, router_has_transport_consumer,
        router_has_transport_consumer_index, router_has_transport_producer,
        router_has_transport_producer_index, router_producer_consumer_count,
        router_producer_consumer_index_count, router_producer_count,
        router_producer_origin_matches, router_satisfies_invariants,
        router_session_transport_count, router_session_transport_index_count,
        router_transport_consumer_count, router_transport_consumer_index_count,
        router_transport_count, router_transport_matches, router_transport_producer_count,
        router_transport_producer_index_count,
    },
};

const SYMBOLIC_ROUTE_COMMAND_VARIANTS: u8 = 9;
const SYMBOLIC_CLEANUP_COMMAND_VARIANTS: u8 = 7;

fn user(id: SessionId) -> Session {
    Session::new(id)
}

#[kani::proof]
#[kani::unwind(8)]
fn bounded_symbolic_router_adds_preserve_invariants() {
    let mut router = Router::new(RouterId(0));

    build_symbolic_trace_topology(&mut router);
    add_symbolic_trace_consumers(&mut router);
    assert_symbolic_trace_invariants(&router);

    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn bounded_symbolic_router_route_trace_preserves_invariants() {
    let mut router = Router::new(RouterId(0));

    build_symbolic_trace_topology(&mut router);
    add_symbolic_trace_consumers(&mut router);
    apply_symbolic_route_command(&mut router, symbolic_route_command());
    assert_symbolic_route_trace_invariants(&router);

    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn bounded_symbolic_router_cleanup_trace_preserves_invariants() {
    let mut router = Router::new(RouterId(0));

    build_symbolic_trace_topology(&mut router);
    add_compatible_trace_consumers(&mut router);
    apply_cleanup_trace_route_state(&mut router);
    apply_symbolic_cleanup_command(&mut router, symbolic_cleanup_command());
    assert_symbolic_trace_invariants(&router);

    std::mem::forget(router);
}

fn symbolic_capability() -> ConsumerCapability {
    if kani::any() {
        ConsumerCapability::Compatible
    } else {
        ConsumerCapability::Incompatible
    }
}

fn symbolic_route_command() -> u8 {
    kani::any_where(|command| *command < SYMBOLIC_ROUTE_COMMAND_VARIANTS)
}

fn symbolic_cleanup_command() -> u8 {
    kani::any_where(|command| *command < SYMBOLIC_CLEANUP_COMMAND_VARIANTS)
}

fn add_symbolic_trace_consumers(router: &mut Router) {
    let _ = router.add_consumer(trace_audio_consumer(), symbolic_capability());
    let _ = router.add_consumer(trace_video_consumer(), symbolic_capability());
}

fn add_compatible_trace_consumers(router: &mut Router) {
    assert!(
        router
            .add_consumer(trace_audio_consumer(), ConsumerCapability::Compatible)
            .is_ok()
    );

    assert!(
        router
            .add_consumer(trace_video_consumer(), ConsumerCapability::Compatible)
            .is_ok()
    );
}

fn trace_audio_consumer() -> Consumer {
    Consumer::new(
        ConsumerId(40),
        ProducerId(30),
        TransportId(21),
        MediaKind::Audio,
    )
}

fn trace_video_consumer() -> Consumer {
    Consumer::new(
        ConsumerId(41),
        ProducerId(31),
        TransportId(11),
        MediaKind::Video,
    )
}

fn apply_cleanup_trace_route_state(router: &mut Router) {
    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );
    assert!(
        router
            .set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Paused)
            .is_ok()
    );
    assert!(
        router
            .set_consumer_route_state(ConsumerId(41), ConsumerRouteState::Paused)
            .is_ok()
    );
}

fn assert_symbolic_trace_invariants(router: &Router) {
    let session_1 = router_contains_session(router, SessionId(1));
    let session_2 = router_contains_session(router, SessionId(2));
    let transport_10 = router_contains_transport(router, TransportId(10));
    let transport_11 = router_contains_transport(router, TransportId(11));
    let transport_20 = router_contains_transport(router, TransportId(20));
    let transport_21 = router_contains_transport(router, TransportId(21));
    let producer_30 = router_contains_producer(router, ProducerId(30));
    let producer_31 = router_contains_producer(router, ProducerId(31));
    let consumer_40 = router_contains_consumer(router, ConsumerId(40));
    let consumer_41 = router_contains_consumer(router, ConsumerId(41));

    assert!(router.session_count() == present(session_1) + present(session_2));
    assert!(
        router_transport_count(router)
            == present(transport_10)
                + present(transport_11)
                + present(transport_20)
                + present(transport_21)
    );
    assert!(router_producer_count(router) == present(producer_30) + present(producer_31));
    assert!(router_consumer_count(router) == present(consumer_40) + present(consumer_41));

    assert_session_transport_index(router, SessionId(1), transport_10, transport_11);
    assert_session_transport_index(router, SessionId(2), transport_20, transport_21);
    assert!(
        router_session_transport_index_count(router)
            == present(transport_10 || transport_11) + present(transport_20 || transport_21)
    );

    assert_known_transport(
        router,
        TransportId(10),
        SessionId(1),
        TransportDirection::Receive,
        transport_10,
        session_1,
    );
    assert_known_transport(
        router,
        TransportId(11),
        SessionId(1),
        TransportDirection::Send,
        transport_11,
        session_1,
    );
    assert_known_transport(
        router,
        TransportId(20),
        SessionId(2),
        TransportDirection::Receive,
        transport_20,
        session_2,
    );
    assert_known_transport(
        router,
        TransportId(21),
        SessionId(2),
        TransportDirection::Send,
        transport_21,
        session_2,
    );

    assert_transport_producer_index(router, TransportId(10), ProducerId(30), producer_30);
    assert_empty_transport_producer_index(router, TransportId(11));
    assert_transport_producer_index(router, TransportId(20), ProducerId(31), producer_31);
    assert_empty_transport_producer_index(router, TransportId(21));
    assert!(
        router_transport_producer_index_count(router)
            == present(producer_30) + present(producer_31)
    );

    assert_known_producer(
        router,
        ProducerId(30),
        TransportId(10),
        MediaKind::Audio,
        producer_30,
        transport_10,
    );
    assert_known_producer(
        router,
        ProducerId(31),
        TransportId(20),
        MediaKind::Video,
        producer_31,
        transport_20,
    );

    assert_transport_consumer_index(router, TransportId(11), ConsumerId(41), consumer_41);
    assert_empty_transport_consumer_index(router, TransportId(10));
    assert_empty_transport_consumer_index(router, TransportId(20));
    assert_transport_consumer_index(router, TransportId(21), ConsumerId(40), consumer_40);
    assert!(
        router_transport_consumer_index_count(router)
            == present(consumer_40) + present(consumer_41)
    );

    assert_producer_consumer_index(router, ProducerId(30), ConsumerId(40), consumer_40);
    assert_producer_consumer_index(router, ProducerId(31), ConsumerId(41), consumer_41);
    assert!(
        router_producer_consumer_index_count(router) == present(consumer_40) + present(consumer_41)
    );

    assert_known_consumer(
        router,
        ConsumerId(40),
        ProducerId(30),
        TransportId(21),
        MediaKind::Audio,
        consumer_40,
        producer_30,
        transport_21,
    );
    assert_known_consumer(
        router,
        ConsumerId(41),
        ProducerId(31),
        TransportId(11),
        MediaKind::Video,
        consumer_41,
        producer_31,
        transport_11,
    );
}

fn assert_symbolic_route_trace_invariants(router: &Router) {
    let consumer_40 = router_contains_consumer(router, ConsumerId(40));
    let consumer_41 = router_contains_consumer(router, ConsumerId(41));

    assert!(router_contains_session(router, SessionId(1)));
    assert!(router_contains_session(router, SessionId(2)));
    assert!(router_contains_transport(router, TransportId(10)));
    assert!(router_contains_transport(router, TransportId(11)));
    assert!(router_contains_transport(router, TransportId(20)));
    assert!(router_contains_transport(router, TransportId(21)));
    assert!(router_contains_producer(router, ProducerId(30)));
    assert!(router_contains_producer(router, ProducerId(31)));
    assert!(router.session_count() == 2);
    assert!(router_transport_count(router) == 4);
    assert!(router_producer_count(router) == 2);
    assert!(router_consumer_count(router) == present(consumer_40) + present(consumer_41));

    assert_session_transport_index(router, SessionId(1), true, true);
    assert_session_transport_index(router, SessionId(2), true, true);
    assert!(router_session_transport_index_count(router) == 2);

    assert_known_transport(
        router,
        TransportId(10),
        SessionId(1),
        TransportDirection::Receive,
        true,
        true,
    );
    assert_known_transport(
        router,
        TransportId(11),
        SessionId(1),
        TransportDirection::Send,
        true,
        true,
    );
    assert_known_transport(
        router,
        TransportId(20),
        SessionId(2),
        TransportDirection::Receive,
        true,
        true,
    );
    assert_known_transport(
        router,
        TransportId(21),
        SessionId(2),
        TransportDirection::Send,
        true,
        true,
    );

    assert_transport_producer_index(router, TransportId(10), ProducerId(30), true);
    assert_empty_transport_producer_index(router, TransportId(11));
    assert_transport_producer_index(router, TransportId(20), ProducerId(31), true);
    assert_empty_transport_producer_index(router, TransportId(21));
    assert!(router_transport_producer_index_count(router) == 2);

    assert_known_producer(
        router,
        ProducerId(30),
        TransportId(10),
        MediaKind::Audio,
        true,
        true,
    );
    assert_known_producer(
        router,
        ProducerId(31),
        TransportId(20),
        MediaKind::Video,
        true,
        true,
    );

    assert_transport_consumer_index(router, TransportId(11), ConsumerId(41), consumer_41);
    assert_empty_transport_consumer_index(router, TransportId(10));
    assert_empty_transport_consumer_index(router, TransportId(20));
    assert_transport_consumer_index(router, TransportId(21), ConsumerId(40), consumer_40);
    assert!(
        router_transport_consumer_index_count(router)
            == present(consumer_40) + present(consumer_41)
    );

    assert_producer_consumer_index(router, ProducerId(30), ConsumerId(40), consumer_40);
    assert_producer_consumer_index(router, ProducerId(31), ConsumerId(41), consumer_41);
    assert!(
        router_producer_consumer_index_count(router) == present(consumer_40) + present(consumer_41)
    );

    assert_known_consumer(
        router,
        ConsumerId(40),
        ProducerId(30),
        TransportId(21),
        MediaKind::Audio,
        consumer_40,
        true,
        true,
    );
    assert_known_consumer(
        router,
        ConsumerId(41),
        ProducerId(31),
        TransportId(11),
        MediaKind::Video,
        consumer_41,
        true,
        true,
    );
}

fn assert_session_transport_index(
    router: &Router,
    session_id: SessionId,
    first_transport: bool,
    second_transport: bool,
) {
    let expected = present(first_transport) + present(second_transport);

    assert!(router_session_transport_count(router, session_id) == expected);
    assert!(router_has_session_transport_index(router, session_id) == (expected > 0));
}

fn assert_known_transport(
    router: &Router,
    transport_id: TransportId,
    session_id: SessionId,
    direction: TransportDirection,
    transport_exists: bool,
    session_exists: bool,
) {
    assert!(
        router_transport_matches(router, transport_id, session_id, direction) == transport_exists
    );
    if transport_exists {
        assert!(session_exists);
        assert!(router_has_session_transport(
            router,
            session_id,
            transport_id
        ));
    }
}

fn assert_transport_producer_index(
    router: &Router,
    transport_id: TransportId,
    producer_id: ProducerId,
    producer_exists: bool,
) {
    assert!(router_transport_producer_count(router, transport_id) == present(producer_exists));
    assert!(router_has_transport_producer_index(router, transport_id) == producer_exists);
    assert!(router_has_transport_producer(router, transport_id, producer_id) == producer_exists);
}

fn assert_empty_transport_producer_index(router: &Router, transport_id: TransportId) {
    assert!(router_transport_producer_count(router, transport_id) == 0);
    assert!(!router_has_transport_producer_index(router, transport_id));
}

fn assert_known_producer(
    router: &Router,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    producer_exists: bool,
    transport_exists: bool,
) {
    assert!(
        router_producer_origin_matches(router, producer_id, transport_id, media_kind)
            == producer_exists
    );
    if producer_exists {
        assert!(transport_exists);
    }
}

fn assert_transport_consumer_index(
    router: &Router,
    transport_id: TransportId,
    consumer_id: ConsumerId,
    consumer_exists: bool,
) {
    assert!(router_transport_consumer_count(router, transport_id) == present(consumer_exists));
    assert!(router_has_transport_consumer_index(router, transport_id) == consumer_exists);
    assert!(router_has_transport_consumer(router, transport_id, consumer_id) == consumer_exists);
}

fn assert_empty_transport_consumer_index(router: &Router, transport_id: TransportId) {
    assert!(router_transport_consumer_count(router, transport_id) == 0);
    assert!(!router_has_transport_consumer_index(router, transport_id));
}

fn assert_producer_consumer_index(
    router: &Router,
    producer_id: ProducerId,
    consumer_id: ConsumerId,
    consumer_exists: bool,
) {
    assert!(router_producer_consumer_count(router, producer_id) == present(consumer_exists));
    assert!(router_has_producer_consumer_index(router, producer_id) == consumer_exists);
    assert!(router_has_producer_consumer(router, producer_id, consumer_id) == consumer_exists);
}

fn assert_known_consumer(
    router: &Router,
    consumer_id: ConsumerId,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    consumer_exists: bool,
    producer_exists: bool,
    transport_exists: bool,
) {
    assert!(
        router_consumer_origin_matches(router, consumer_id, producer_id, transport_id, media_kind)
            == consumer_exists
    );
    if consumer_exists {
        assert!(producer_exists);
        assert!(transport_exists);
        assert!(router_consumer_shadows_producer(router, consumer_id));
    }
}

fn present(value: bool) -> usize {
    if value { 1 } else { 0 }
}

fn build_symbolic_trace_topology(router: &mut Router) {
    assert!(router.join_session(user(SessionId(1))).is_ok());
    assert!(router.join_session(user(SessionId(2))).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(10),
                SessionId(1),
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(11),
                SessionId(1),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(20),
                SessionId(2),
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(21),
                SessionId(2),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                ProducerId(30),
                TransportId(10),
                MediaKind::Audio,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                ProducerId(31),
                TransportId(20),
                MediaKind::Video,
            ))
            .is_ok()
    );
}

#[kani::proof]
#[kani::unwind(8)]
fn session_teardown_clears_reverse_indices_and_dependents() {
    let mut router = Router::new(RouterId(0));

    let session_a = SessionId(1);
    let session_b = SessionId(2);
    let receive_transport = TransportId(10);
    let shared_send_transport = TransportId(11);
    let survivor_send_transport = TransportId(20);
    let producer_id = ProducerId(30);
    let removed_consumer_id = ConsumerId(40);
    let surviving_consumer_id = ConsumerId(41);

    assert!(router.join_session(user(session_a)).is_ok());
    assert!(router.join_session(user(session_b)).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                receive_transport,
                session_a,
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                shared_send_transport,
                session_a,
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                survivor_send_transport,
                session_b,
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                producer_id,
                receive_transport,
                MediaKind::Audio,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    removed_consumer_id,
                    producer_id,
                    shared_send_transport,
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    surviving_consumer_id,
                    producer_id,
                    survivor_send_transport,
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );

    assert!(router.remove_session(session_a).is_ok());

    assert!(!router_contains_session(&router, session_a));
    assert!(router_contains_session(&router, session_b));
    assert!(!router_contains_transport(&router, receive_transport));
    assert!(!router_contains_transport(&router, shared_send_transport));
    assert!(router_contains_transport(&router, survivor_send_transport));
    assert!(!router_contains_producer(&router, producer_id));
    assert!(!router_contains_consumer(&router, removed_consumer_id));
    assert!(!router_contains_consumer(&router, surviving_consumer_id));
    assert!(!router_has_session_transport_index(&router, session_a));
    assert!(router_session_transport_count(&router, session_b) == 1);
    assert!(router_has_session_transport(
        &router,
        session_b,
        survivor_send_transport
    ));
    assert!(!router_has_transport_producer_index(
        &router,
        receive_transport
    ));
    assert!(!router_has_transport_consumer_index(
        &router,
        shared_send_transport
    ));
    assert!(!router_has_transport_consumer_index(
        &router,
        survivor_send_transport
    ));
    assert!(!router_has_producer_consumer_index(&router, producer_id));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn removing_a_producer_clears_dependents_but_keeps_live_transports() {
    let mut router = Router::new(RouterId(0));

    let session_a = SessionId(1);
    let session_b = SessionId(2);
    let receive_transport = TransportId(10);
    let same_session_send_transport = TransportId(11);
    let remote_send_transport = TransportId(20);
    let producer_id = ProducerId(30);
    let same_session_consumer = ConsumerId(40);
    let remote_consumer = ConsumerId(41);

    assert!(router.join_session(user(session_a)).is_ok());
    assert!(router.join_session(user(session_b)).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                receive_transport,
                session_a,
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                same_session_send_transport,
                session_a,
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                remote_send_transport,
                session_b,
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                producer_id,
                receive_transport,
                MediaKind::Audio,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    same_session_consumer,
                    producer_id,
                    same_session_send_transport,
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    remote_consumer,
                    producer_id,
                    remote_send_transport,
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );

    assert!(router.remove_producer(producer_id).is_ok());

    assert!(router_contains_session(&router, session_a));
    assert!(router_contains_session(&router, session_b));
    assert!(router_contains_transport(&router, receive_transport));
    assert!(router_contains_transport(
        &router,
        same_session_send_transport
    ));
    assert!(router_contains_transport(&router, remote_send_transport));
    assert!(!router_contains_producer(&router, producer_id));
    assert!(!router_contains_consumer(&router, same_session_consumer));
    assert!(!router_contains_consumer(&router, remote_consumer));
    assert!(!router_has_transport_producer_index(
        &router,
        receive_transport
    ));
    assert!(!router_has_transport_consumer_index(
        &router,
        same_session_send_transport
    ));
    assert!(!router_has_transport_consumer_index(
        &router,
        remote_send_transport
    ));
    assert!(!router_has_producer_consumer_index(&router, producer_id));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn removing_a_consumer_preserves_other_routes_and_indices() {
    let mut router = Router::new(RouterId(0));

    let session_a = SessionId(1);
    let session_b = SessionId(2);
    let receive_transport = TransportId(10);
    let removed_consumer_transport = TransportId(11);
    let surviving_consumer_transport = TransportId(20);
    let producer_id = ProducerId(30);
    let removed_consumer_id = ConsumerId(40);
    let surviving_consumer_id = ConsumerId(41);

    assert!(router.join_session(user(session_a)).is_ok());
    assert!(router.join_session(user(session_b)).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                receive_transport,
                session_a,
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                removed_consumer_transport,
                session_a,
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                surviving_consumer_transport,
                session_b,
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                producer_id,
                receive_transport,
                MediaKind::Audio,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    removed_consumer_id,
                    producer_id,
                    removed_consumer_transport,
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    surviving_consumer_id,
                    producer_id,
                    surviving_consumer_transport,
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );

    assert!(router.remove_consumer(removed_consumer_id).is_ok());

    assert!(router_contains_producer(&router, producer_id));
    assert!(router_contains_transport(&router, receive_transport));
    assert!(router_contains_transport(
        &router,
        removed_consumer_transport
    ));
    assert!(router_contains_transport(
        &router,
        surviving_consumer_transport
    ));
    assert!(!router_contains_consumer(&router, removed_consumer_id));
    assert!(router_contains_consumer(&router, surviving_consumer_id));
    assert!(!router_has_transport_consumer_index(
        &router,
        removed_consumer_transport
    ));
    assert!(router_has_transport_consumer(
        &router,
        surviving_consumer_transport,
        surviving_consumer_id
    ));
    assert!(router_has_producer_consumer(
        &router,
        producer_id,
        surviving_consumer_id
    ));
    assert!(!router_has_producer_consumer(
        &router,
        producer_id,
        removed_consumer_id
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn new_consumers_inherit_their_producer_pause_shadow() {
    let mut router = Router::new(RouterId(0));

    assert!(router.join_session(user(SessionId(1))).is_ok());
    assert!(router.join_session(user(SessionId(2))).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(10),
                SessionId(1),
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(20),
                SessionId(2),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                ProducerId(30),
                TransportId(10),
                MediaKind::Audio,
            ))
            .is_ok()
    );
    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    ConsumerId(40),
                    ProducerId(30),
                    TransportId(20),
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );

    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn pausing_a_producer_updates_all_dependent_consumers() {
    let mut router = Router::new(RouterId(0));

    assert!(router.join_session(user(SessionId(1))).is_ok());
    assert!(router.join_session(user(SessionId(2))).is_ok());
    assert!(router.join_session(user(SessionId(3))).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(10),
                SessionId(1),
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(20),
                SessionId(2),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(21),
                SessionId(3),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                ProducerId(30),
                TransportId(10),
                MediaKind::Video,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    ConsumerId(40),
                    ProducerId(30),
                    TransportId(20),
                    MediaKind::Video,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    ConsumerId(41),
                    ProducerId(30),
                    TransportId(21),
                    MediaKind::Video,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );

    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(41),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn resuming_a_producer_clears_dependent_consumer_pause_shadows() {
    let mut router = Router::new(RouterId(0));

    assert!(router.join_session(user(SessionId(1))).is_ok());
    assert!(router.join_session(user(SessionId(2))).is_ok());
    assert!(router.join_session(user(SessionId(3))).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(10),
                SessionId(1),
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(20),
                SessionId(2),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(21),
                SessionId(3),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                ProducerId(30),
                TransportId(10),
                MediaKind::Video,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    ConsumerId(40),
                    ProducerId(30),
                    TransportId(20),
                    MediaKind::Video,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    ConsumerId(41),
                    ProducerId(30),
                    TransportId(21),
                    MediaKind::Video,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );
    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Active)
            .is_ok()
    );

    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    ));
    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(41),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(8)]
fn consumer_local_pause_stays_independent_from_producer_shadow_updates() {
    let mut router = Router::new(RouterId(0));

    assert!(router.join_session(user(SessionId(1))).is_ok());
    assert!(router.join_session(user(SessionId(2))).is_ok());
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(10),
                SessionId(1),
                TransportDirection::Receive,
            ))
            .is_ok()
    );
    assert!(
        router
            .open_transport(Transport::new(
                TransportId(20),
                SessionId(2),
                TransportDirection::Send,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_producer(Producer::new(
                ProducerId(30),
                TransportId(10),
                MediaKind::Audio,
            ))
            .is_ok()
    );
    assert!(
        router
            .add_consumer(
                Consumer::new(
                    ConsumerId(40),
                    ProducerId(30),
                    TransportId(20),
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            )
            .is_ok()
    );

    assert!(
        router
            .set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Paused)
            .is_ok()
    );
    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    ));
    assert!(router_satisfies_invariants(&router));

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );
    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Paused,
    ));
    assert!(router_satisfies_invariants(&router));

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Active)
            .is_ok()
    );
    assert!(router_consumer_route_matches(
        &router,
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

fn apply_symbolic_route_command(router: &mut Router, command: u8) {
    match command {
        0 => {}
        1 => {
            let _ = router.set_producer_route_state(ProducerId(30), ProducerRouteState::Paused);
        }
        2 => {
            let _ = router.set_producer_route_state(ProducerId(30), ProducerRouteState::Active);
        }
        3 => {
            let _ = router.set_producer_route_state(ProducerId(31), ProducerRouteState::Paused);
        }
        4 => {
            let _ = router.set_producer_route_state(ProducerId(31), ProducerRouteState::Active);
        }
        5 => {
            let _ = router.set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Paused);
        }
        6 => {
            let _ = router.set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Active);
        }
        7 => {
            let _ = router.set_consumer_route_state(ConsumerId(41), ConsumerRouteState::Paused);
        }
        8 => {
            let _ = router.set_consumer_route_state(ConsumerId(41), ConsumerRouteState::Active);
        }
        _ => {}
    }
}

fn apply_symbolic_cleanup_command(router: &mut Router, command: u8) {
    match command {
        0 => {}
        1 => {
            let _ = router.remove_consumer(ConsumerId(40));
        }
        2 => {
            let _ = router.remove_consumer(ConsumerId(41));
        }
        3 => {
            let _ = router.remove_producer(ProducerId(30));
        }
        4 => {
            let _ = router.remove_producer(ProducerId(31));
        }
        5 => {
            let _ = router.remove_session(SessionId(1));
        }
        6 => {
            let _ = router.remove_session(SessionId(2));
        }
        _ => {}
    }
}
