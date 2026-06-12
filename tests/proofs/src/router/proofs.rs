//! bounded Kani proofs for router topology invariants
//!
//! the harnesses build small symbolic call graphs that cover session ownership,
//! transport direction, producer dependency, consumer dependency and route-state
//! shadowing
//! each proof checks both primary maps and reverse indexes so teardown
//! changes cannot silently leave stale dependency edges

use o_sfu_router::{
    ConsumerCapability, ConsumerId, ConsumerRouteState, ConsumerSpec, MediaKind, ProducerId,
    ProducerRouteState, ProducerSpec, Router, RouterError, RouterId, Session, SessionId,
    TransportDirection, TransportId,
    test_support::{proof::RouterProofView, router_satisfies_invariants},
};

const SYMBOLIC_ROUTE_COMMAND_VARIANTS: u8 = 9;
const SYMBOLIC_CLEANUP_COMMAND_VARIANTS: u8 = 7;

fn session(id: SessionId) -> Session {
    Session::new(id)
}

fn join(router: &mut Router, session_id: SessionId) -> bool {
    router.join(session(session_id)).is_ok()
}

fn open_receive(router: &mut Router, session_id: SessionId, transport_id: TransportId) -> bool {
    router
        .session(session_id)
        .and_then(|session| session.open_receive_transport(transport_id))
        .is_ok()
}

fn open_send(router: &mut Router, session_id: SessionId, transport_id: TransportId) -> bool {
    router
        .session(session_id)
        .and_then(|session| session.open_send_transport(transport_id))
        .is_ok()
}

fn publish(
    router: &mut Router,
    transport_id: TransportId,
    producer_id: ProducerId,
    media_kind: MediaKind,
) -> bool {
    router
        .receive_transport(transport_id)
        .and_then(|transport| transport.publish(ProducerSpec::new(producer_id, media_kind)))
        .is_ok()
}

fn try_consume(
    router: &mut Router,
    transport_id: TransportId,
    consumer_id: ConsumerId,
    producer_id: ProducerId,
    capability: ConsumerCapability,
) -> Result<ConsumerId, RouterError> {
    router.send_transport(transport_id).and_then(|transport| {
        transport.consume(ConsumerSpec::new(consumer_id, producer_id, capability))
    })
}

/// prove that all bounded add paths preserve the complete router invariant
///
/// consumers are accepted or rejected with symbolic capability results
/// the postcondition must hold for every accepted subset
#[kani::proof]
#[kani::unwind(8)]
fn bounded_symbolic_router_adds_preserve_invariants() {
    let mut router = Router::new(RouterId(0));

    build_symbolic_trace_topology(&mut router);
    add_symbolic_trace_consumers(&mut router);
    assert_symbolic_trace_invariants(&router);

    std::mem::forget(router);
}

/// prove that route-state commands preserve topology and shadow invariants
///
/// this keeps cleanup out of scope so the proof can assert that the base
/// topology remains live while producer shadows follow source-side route state
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

/// prove that cleanup commands preserve reverse-index exactness
///
/// the proof starts from a topology where consumers are known to exist
/// symbolic cleanup then removes one route, one producer or one session and checks that
/// every remaining relation is still exact
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

/// choose whether a symbolic consumer should pass the external capability gate
fn symbolic_capability() -> ConsumerCapability {
    if kani::any() {
        ConsumerCapability::Compatible
    } else {
        ConsumerCapability::Incompatible
    }
}

/// choose one bounded route-state command for the route trace proof
fn symbolic_route_command() -> u8 {
    kani::any_where(|command| *command < SYMBOLIC_ROUTE_COMMAND_VARIANTS)
}

/// choose one bounded cleanup command for the teardown trace proof
fn symbolic_cleanup_command() -> u8 {
    kani::any_where(|command| *command < SYMBOLIC_CLEANUP_COMMAND_VARIANTS)
}

/// add consumers whose capability result remains symbolic
fn add_symbolic_trace_consumers(router: &mut Router) {
    let _ = try_consume(
        router,
        TransportId(21),
        ConsumerId(40),
        ProducerId(30),
        symbolic_capability(),
    );
    let _ = try_consume(
        router,
        TransportId(11),
        ConsumerId(41),
        ProducerId(31),
        symbolic_capability(),
    );
}

/// add consumers that must exist before cleanup is explored
fn add_compatible_trace_consumers(router: &mut Router) {
    assert!(
        try_consume(
            router,
            TransportId(21),
            ConsumerId(40),
            ProducerId(30),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    assert!(
        try_consume(
            router,
            TransportId(11),
            ConsumerId(41),
            ProducerId(31),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );
}

/// place the cleanup trace in a mixed route-state configuration
///
/// cleanup must preserve invariants regardless of local pause state or producer
/// pause shadows
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

/// assert exact topology facts after symbolic add or cleanup commands
///
/// each entity may or may not be present depending on earlier symbolic choices
/// every count, reverse-index key and membership assertion is derived from that
/// present set
fn assert_symbolic_trace_invariants(router: &Router) {
    let view = RouterProofView::new(router);
    let session_1 = view.contains_session(SessionId(1));
    let session_2 = view.contains_session(SessionId(2));
    let transport_10 = view.contains_transport(TransportId(10));
    let transport_11 = view.contains_transport(TransportId(11));
    let transport_20 = view.contains_transport(TransportId(20));
    let transport_21 = view.contains_transport(TransportId(21));
    let producer_30 = view.contains_producer(ProducerId(30));
    let producer_31 = view.contains_producer(ProducerId(31));
    let consumer_40 = view.contains_consumer(ConsumerId(40));
    let consumer_41 = view.contains_consumer(ConsumerId(41));

    assert!(router.session_count() == present(session_1) + present(session_2));
    assert!(
        view.transport_count()
            == present(transport_10)
                + present(transport_11)
                + present(transport_20)
                + present(transport_21)
    );
    assert!(view.producer_count() == present(producer_30) + present(producer_31));
    assert!(view.consumer_count() == present(consumer_40) + present(consumer_41));

    assert_session_transport_index(router, SessionId(1), transport_10, transport_11);
    assert_session_transport_index(router, SessionId(2), transport_20, transport_21);
    assert!(
        view.session_transports().key_count()
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
    assert!(view.transport_producers().key_count() == present(producer_30) + present(producer_31));

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
    assert!(view.transport_consumers().key_count() == present(consumer_40) + present(consumer_41));

    assert_producer_consumer_index(router, ProducerId(30), ConsumerId(40), consumer_40);
    assert_producer_consumer_index(router, ProducerId(31), ConsumerId(41), consumer_41);
    assert!(view.producer_consumers().key_count() == present(consumer_40) + present(consumer_41));

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

/// assert route-state trace facts when base topology must remain live
///
/// route commands are not allowed to remove entities
/// only consumer acceptance remains symbolic
fn assert_symbolic_route_trace_invariants(router: &Router) {
    let view = RouterProofView::new(router);
    let consumer_40 = view.contains_consumer(ConsumerId(40));
    let consumer_41 = view.contains_consumer(ConsumerId(41));

    assert!(view.contains_session(SessionId(1)));
    assert!(view.contains_session(SessionId(2)));
    assert!(view.contains_transport(TransportId(10)));
    assert!(view.contains_transport(TransportId(11)));
    assert!(view.contains_transport(TransportId(20)));
    assert!(view.contains_transport(TransportId(21)));
    assert!(view.contains_producer(ProducerId(30)));
    assert!(view.contains_producer(ProducerId(31)));
    assert!(router.session_count() == 2);
    assert!(view.transport_count() == 4);
    assert!(view.producer_count() == 2);
    assert!(view.consumer_count() == present(consumer_40) + present(consumer_41));

    assert_session_transport_index(router, SessionId(1), true, true);
    assert_session_transport_index(router, SessionId(2), true, true);
    assert!(view.session_transports().key_count() == 2);

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
    assert!(view.transport_producers().key_count() == 2);

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
    assert!(view.transport_consumers().key_count() == present(consumer_40) + present(consumer_41));

    assert_producer_consumer_index(router, ProducerId(30), ConsumerId(40), consumer_40);
    assert_producer_consumer_index(router, ProducerId(31), ConsumerId(41), consumer_41);
    assert!(view.producer_consumers().key_count() == present(consumer_40) + present(consumer_41));

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

/// assert the session reverse index for a pair of known transports
fn assert_session_transport_index(
    router: &Router,
    session_id: SessionId,
    first_transport: bool,
    second_transport: bool,
) {
    let expected = present(first_transport) + present(second_transport);
    let view = RouterProofView::new(router);

    assert!(view.session_transports().count(session_id) == expected);
    assert!(view.session_transports().contains_key(session_id) == (expected > 0));
}

/// assert transport primary-map data plus the owning session index edge
fn assert_known_transport(
    router: &Router,
    transport_id: TransportId,
    session_id: SessionId,
    direction: TransportDirection,
    transport_exists: bool,
    session_exists: bool,
) {
    let view = RouterProofView::new(router);
    assert!(view.transport_matches(transport_id, session_id, direction) == transport_exists);
    if transport_exists {
        assert!(session_exists);
        assert!(view.session_transports().contains(session_id, transport_id));
    }
}

/// assert the receive-transport-to-producer reverse index for one producer
fn assert_transport_producer_index(
    router: &Router,
    transport_id: TransportId,
    producer_id: ProducerId,
    producer_exists: bool,
) {
    let view = RouterProofView::new(router);
    assert!(view.transport_producers().count(transport_id) == present(producer_exists));
    assert!(view.transport_producers().contains_key(transport_id) == producer_exists);
    assert!(
        view.transport_producers()
            .contains(transport_id, producer_id)
            == producer_exists
    );
}

/// assert that a transport has no indexed producer
fn assert_empty_transport_producer_index(router: &Router, transport_id: TransportId) {
    let view = RouterProofView::new(router);
    assert!(view.transport_producers().count(transport_id) == 0);
    assert!(!view.transport_producers().contains_key(transport_id));
}

/// assert producer primary-map data against its owning receive transport
fn assert_known_producer(
    router: &Router,
    producer_id: ProducerId,
    transport_id: TransportId,
    media_kind: MediaKind,
    producer_exists: bool,
    transport_exists: bool,
) {
    let view = RouterProofView::new(router);
    assert!(view.producer_origin_matches(producer_id, transport_id, media_kind) == producer_exists);
    if producer_exists {
        assert!(transport_exists);
    }
}

/// assert the send-transport-to-consumer reverse index for one consumer
fn assert_transport_consumer_index(
    router: &Router,
    transport_id: TransportId,
    consumer_id: ConsumerId,
    consumer_exists: bool,
) {
    let view = RouterProofView::new(router);
    assert!(view.transport_consumers().count(transport_id) == present(consumer_exists));
    assert!(view.transport_consumers().contains_key(transport_id) == consumer_exists);
    assert!(
        view.transport_consumers()
            .contains(transport_id, consumer_id)
            == consumer_exists
    );
}

/// assert that a transport has no indexed consumer
fn assert_empty_transport_consumer_index(router: &Router, transport_id: TransportId) {
    let view = RouterProofView::new(router);
    assert!(view.transport_consumers().count(transport_id) == 0);
    assert!(!view.transport_consumers().contains_key(transport_id));
}

/// assert the producer-to-consumer reverse index for one consumer dependency
fn assert_producer_consumer_index(
    router: &Router,
    producer_id: ProducerId,
    consumer_id: ConsumerId,
    consumer_exists: bool,
) {
    let view = RouterProofView::new(router);
    assert!(view.producer_consumers().count(producer_id) == present(consumer_exists));
    assert!(view.producer_consumers().contains_key(producer_id) == consumer_exists);
    assert!(view.producer_consumers().contains(producer_id, consumer_id) == consumer_exists);
}

/// assert consumer primary-map data plus required producer and transport owners
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
    let view = RouterProofView::new(router);
    assert!(
        view.consumer_origin_matches(consumer_id, producer_id, transport_id, media_kind)
            == consumer_exists
    );
    if consumer_exists {
        assert!(producer_exists);
        assert!(transport_exists);
        assert!(view.consumer_shadows_producer(consumer_id));
    }
}

/// convert symbolic presence into an arithmetic count contribution
fn present(value: bool) -> usize {
    if value { 1 } else { 0 }
}

/// build the base two-session topology shared by symbolic traces
///
/// sessions own one receive and one send transport each
/// producers live on the
/// receive transports so consumers can later target the opposite send transport
fn build_symbolic_trace_topology(router: &mut Router) {
    assert!(join(router, SessionId(1)));
    assert!(join(router, SessionId(2)));
    assert!(open_receive(router, SessionId(1), TransportId(10)));
    assert!(open_send(router, SessionId(1), TransportId(11)));
    assert!(open_receive(router, SessionId(2), TransportId(20)));
    assert!(open_send(router, SessionId(2), TransportId(21)));
    assert!(publish(
        router,
        TransportId(10),
        ProducerId(30),
        MediaKind::Audio,
    ));
    assert!(publish(
        router,
        TransportId(20),
        ProducerId(31),
        MediaKind::Video,
    ));
}

/// prove session teardown drains all owned transports and dependent media
///
/// the scenario includes a consumer on another live session that depends on a
/// removed producer
/// removing the source session must remove that remote
/// consumer as well
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

    assert!(join(&mut router, session_a));
    assert!(join(&mut router, session_b));
    assert!(open_receive(&mut router, session_a, receive_transport));
    assert!(open_send(&mut router, session_a, shared_send_transport));
    assert!(open_send(&mut router, session_b, survivor_send_transport));
    assert!(publish(
        &mut router,
        receive_transport,
        producer_id,
        MediaKind::Audio,
    ));
    assert!(
        try_consume(
            &mut router,
            shared_send_transport,
            removed_consumer_id,
            producer_id,
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );
    assert!(
        try_consume(
            &mut router,
            survivor_send_transport,
            surviving_consumer_id,
            producer_id,
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    assert!(router.remove_session(session_a).is_ok());

    let view = RouterProofView::new(&router);
    assert!(!view.contains_session(session_a));
    assert!(view.contains_session(session_b));
    assert!(!view.contains_transport(receive_transport));
    assert!(!view.contains_transport(shared_send_transport));
    assert!(view.contains_transport(survivor_send_transport));
    assert!(!view.contains_producer(producer_id));
    assert!(!view.contains_consumer(removed_consumer_id));
    assert!(!view.contains_consumer(surviving_consumer_id));
    assert!(!view.session_transports().contains_key(session_a));
    assert!(view.session_transports().count(session_b) == 1);
    assert!(
        view.session_transports()
            .contains(session_b, survivor_send_transport)
    );
    assert!(!view.transport_producers().contains_key(receive_transport));
    assert!(
        !view
            .transport_consumers()
            .contains_key(shared_send_transport)
    );
    assert!(
        !view
            .transport_consumers()
            .contains_key(survivor_send_transport)
    );
    assert!(!view.producer_consumers().contains_key(producer_id));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// prove producer teardown drains all dependent consumers but keeps transports
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

    assert!(join(&mut router, session_a));
    assert!(join(&mut router, session_b));
    assert!(open_receive(&mut router, session_a, receive_transport));
    assert!(open_send(
        &mut router,
        session_a,
        same_session_send_transport,
    ));
    assert!(open_send(&mut router, session_b, remote_send_transport));
    assert!(publish(
        &mut router,
        receive_transport,
        producer_id,
        MediaKind::Audio,
    ));
    assert!(
        try_consume(
            &mut router,
            same_session_send_transport,
            same_session_consumer,
            producer_id,
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );
    assert!(
        try_consume(
            &mut router,
            remote_send_transport,
            remote_consumer,
            producer_id,
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    assert!(router.remove_producer(producer_id).is_ok());

    let view = RouterProofView::new(&router);
    assert!(view.contains_session(session_a));
    assert!(view.contains_session(session_b));
    assert!(view.contains_transport(receive_transport));
    assert!(view.contains_transport(same_session_send_transport));
    assert!(view.contains_transport(remote_send_transport));
    assert!(!view.contains_producer(producer_id));
    assert!(!view.contains_consumer(same_session_consumer));
    assert!(!view.contains_consumer(remote_consumer));
    assert!(!view.transport_producers().contains_key(receive_transport));
    assert!(
        !view
            .transport_consumers()
            .contains_key(same_session_send_transport)
    );
    assert!(
        !view
            .transport_consumers()
            .contains_key(remote_send_transport)
    );
    assert!(!view.producer_consumers().contains_key(producer_id));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// prove consumer teardown leaves sibling routes and producer indexes intact
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

    assert!(join(&mut router, session_a));
    assert!(join(&mut router, session_b));
    assert!(open_receive(&mut router, session_a, receive_transport));
    assert!(open_send(
        &mut router,
        session_a,
        removed_consumer_transport
    ));
    assert!(open_send(
        &mut router,
        session_b,
        surviving_consumer_transport,
    ));
    assert!(publish(
        &mut router,
        receive_transport,
        producer_id,
        MediaKind::Audio,
    ));
    assert!(
        try_consume(
            &mut router,
            removed_consumer_transport,
            removed_consumer_id,
            producer_id,
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );
    assert!(
        try_consume(
            &mut router,
            surviving_consumer_transport,
            surviving_consumer_id,
            producer_id,
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    assert!(router.remove_consumer(removed_consumer_id).is_ok());

    let view = RouterProofView::new(&router);
    assert!(view.contains_producer(producer_id));
    assert!(view.contains_transport(receive_transport));
    assert!(view.contains_transport(removed_consumer_transport));
    assert!(view.contains_transport(surviving_consumer_transport));
    assert!(!view.contains_consumer(removed_consumer_id));
    assert!(view.contains_consumer(surviving_consumer_id));
    assert!(
        !view
            .transport_consumers()
            .contains_key(removed_consumer_transport)
    );
    assert!(
        view.transport_consumers()
            .contains(surviving_consumer_transport, surviving_consumer_id)
    );
    assert!(
        view.producer_consumers()
            .contains(producer_id, surviving_consumer_id)
    );
    assert!(
        !view
            .producer_consumers()
            .contains(producer_id, removed_consumer_id)
    );
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// prove new consumers inherit the current producer route-state shadow
#[kani::proof]
#[kani::unwind(8)]
fn new_consumers_inherit_their_producer_pause_shadow() {
    let mut router = Router::new(RouterId(0));

    assert!(join(&mut router, SessionId(1)));
    assert!(join(&mut router, SessionId(2)));
    assert!(open_receive(&mut router, SessionId(1), TransportId(10)));
    assert!(open_send(&mut router, SessionId(2), TransportId(20)));
    assert!(publish(
        &mut router,
        TransportId(10),
        ProducerId(30),
        MediaKind::Audio,
    ));
    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );
    assert!(
        try_consume(
            &mut router,
            TransportId(20),
            ConsumerId(40),
            ProducerId(30),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    let view = RouterProofView::new(&router);
    assert!(view.consumer_route_matches(
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// prove pausing a producer updates every dependent consumer shadow
#[kani::proof]
#[kani::unwind(8)]
fn pausing_a_producer_updates_all_dependent_consumers() {
    let mut router = Router::new(RouterId(0));

    assert!(join(&mut router, SessionId(1)));
    assert!(join(&mut router, SessionId(2)));
    assert!(join(&mut router, SessionId(3)));
    assert!(open_receive(&mut router, SessionId(1), TransportId(10)));
    assert!(open_send(&mut router, SessionId(2), TransportId(20)));
    assert!(open_send(&mut router, SessionId(3), TransportId(21)));
    assert!(publish(
        &mut router,
        TransportId(10),
        ProducerId(30),
        MediaKind::Video,
    ));
    assert!(
        try_consume(
            &mut router,
            TransportId(20),
            ConsumerId(40),
            ProducerId(30),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );
    assert!(
        try_consume(
            &mut router,
            TransportId(21),
            ConsumerId(41),
            ProducerId(30),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );

    let view = RouterProofView::new(&router);
    assert!(view.consumer_route_matches(
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(view.consumer_route_matches(
        ConsumerId(41),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// prove resuming a producer clears every dependent consumer shadow
#[kani::proof]
#[kani::unwind(8)]
fn resuming_a_producer_clears_dependent_consumer_pause_shadows() {
    let mut router = Router::new(RouterId(0));

    assert!(join(&mut router, SessionId(1)));
    assert!(join(&mut router, SessionId(2)));
    assert!(join(&mut router, SessionId(3)));
    assert!(open_receive(&mut router, SessionId(1), TransportId(10)));
    assert!(open_send(&mut router, SessionId(2), TransportId(20)));
    assert!(open_send(&mut router, SessionId(3), TransportId(21)));
    assert!(publish(
        &mut router,
        TransportId(10),
        ProducerId(30),
        MediaKind::Video,
    ));
    assert!(
        try_consume(
            &mut router,
            TransportId(20),
            ConsumerId(40),
            ProducerId(30),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );
    assert!(
        try_consume(
            &mut router,
            TransportId(21),
            ConsumerId(41),
            ProducerId(30),
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

    let view = RouterProofView::new(&router);
    assert!(view.consumer_route_matches(
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    ));
    assert!(view.consumer_route_matches(
        ConsumerId(41),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// prove consumer-local pause state is independent from producer shadows
#[kani::proof]
#[kani::unwind(8)]
fn consumer_local_pause_stays_independent_from_producer_shadow_updates() {
    let mut router = Router::new(RouterId(0));

    assert!(join(&mut router, SessionId(1)));
    assert!(join(&mut router, SessionId(2)));
    assert!(open_receive(&mut router, SessionId(1), TransportId(10)));
    assert!(open_send(&mut router, SessionId(2), TransportId(20)));
    assert!(publish(
        &mut router,
        TransportId(10),
        ProducerId(30),
        MediaKind::Audio,
    ));
    assert!(
        try_consume(
            &mut router,
            TransportId(20),
            ConsumerId(40),
            ProducerId(30),
            ConsumerCapability::Compatible,
        )
        .is_ok()
    );

    assert!(
        router
            .set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Paused)
            .is_ok()
    );
    let view = RouterProofView::new(&router);
    assert!(view.consumer_route_matches(
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
    let view = RouterProofView::new(&router);
    assert!(view.consumer_route_matches(
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
    let view = RouterProofView::new(&router);
    assert!(view.consumer_route_matches(
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    ));
    assert!(router_satisfies_invariants(&router));
    std::mem::forget(router);
}

/// apply one bounded route-state command to the symbolic trace topology
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

/// apply one bounded cleanup command to the symbolic trace topology
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
