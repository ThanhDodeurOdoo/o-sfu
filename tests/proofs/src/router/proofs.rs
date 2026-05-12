use o_sfu_router::{
    Consumer, ConsumerCapability, ConsumerId, ConsumerRouteState, MediaKind, Producer, ProducerId,
    ProducerRouteState, Router, RouterId, Session, SessionId, SessionState, Transport,
    TransportDirection, TransportId,
    test_support::{RouterStateSnapshot, router_state_snapshot},
};

const SYMBOLIC_TRACE_LENGTH: usize = 6;
const SYMBOLIC_TRACE_LENGTH_U8: u8 = 6;
const SYMBOLIC_COMMAND_VARIANTS: u8 = 26;

fn user(id: SessionId) -> Session {
    Session::new(id)
}

#[kani::proof]
#[kani::unwind(64)]
fn bounded_symbolic_router_trace_preserves_invariants() {
    let commands: [u8; SYMBOLIC_TRACE_LENGTH] = kani::any();
    let active_len = usize::from(kani::any::<u8>() % (SYMBOLIC_TRACE_LENGTH_U8 + 1));
    let mut router = Router::new(RouterId(0));

    assert_production_router_invariants(&router);

    let mut index = 0;
    while index < active_len {
        apply_symbolic_command(&mut router, commands[index]);
        assert_production_router_invariants(&router);
        index += 1;
    }

    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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

    let snapshot = router_state_snapshot(&router);
    assert!(!contains_session(&snapshot, session_a));
    assert!(contains_session(&snapshot, session_b));
    assert!(!contains_transport(&snapshot, receive_transport));
    assert!(!contains_transport(&snapshot, shared_send_transport));
    assert!(contains_transport(&snapshot, survivor_send_transport));
    assert!(!contains_producer(&snapshot, producer_id));
    assert!(!contains_consumer(&snapshot, removed_consumer_id));
    assert!(!contains_consumer(&snapshot, surviving_consumer_id));
    assert!(!index_contains_key(&snapshot.session_transports, session_a));
    assert!(index_member_count(&snapshot.session_transports, session_b) == 1);
    assert!(index_contains(
        &snapshot.session_transports,
        session_b,
        survivor_send_transport
    ));
    assert!(!index_contains_key(
        &snapshot.transport_producers,
        receive_transport
    ));
    assert!(!index_contains_key(
        &snapshot.transport_consumers,
        shared_send_transport
    ));
    assert!(!index_contains_key(
        &snapshot.transport_consumers,
        survivor_send_transport
    ));
    assert!(!index_contains_key(
        &snapshot.producer_consumers,
        producer_id
    ));
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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

    let snapshot = router_state_snapshot(&router);
    assert!(contains_session(&snapshot, session_a));
    assert!(contains_session(&snapshot, session_b));
    assert!(contains_transport(&snapshot, receive_transport));
    assert!(contains_transport(&snapshot, same_session_send_transport));
    assert!(contains_transport(&snapshot, remote_send_transport));
    assert!(!contains_producer(&snapshot, producer_id));
    assert!(!contains_consumer(&snapshot, same_session_consumer));
    assert!(!contains_consumer(&snapshot, remote_consumer));
    assert!(!index_contains_key(
        &snapshot.transport_producers,
        receive_transport
    ));
    assert!(!index_contains_key(
        &snapshot.transport_consumers,
        same_session_send_transport
    ));
    assert!(!index_contains_key(
        &snapshot.transport_consumers,
        remote_send_transport
    ));
    assert!(!index_contains_key(
        &snapshot.producer_consumers,
        producer_id
    ));
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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

    let snapshot = router_state_snapshot(&router);
    assert!(contains_producer(&snapshot, producer_id));
    assert!(contains_transport(&snapshot, receive_transport));
    assert!(contains_transport(&snapshot, removed_consumer_transport));
    assert!(contains_transport(&snapshot, surviving_consumer_transport));
    assert!(!contains_consumer(&snapshot, removed_consumer_id));
    assert!(contains_consumer(&snapshot, surviving_consumer_id));
    assert!(!index_contains_key(
        &snapshot.transport_consumers,
        removed_consumer_transport
    ));
    assert!(index_contains(
        &snapshot.transport_consumers,
        surviving_consumer_transport,
        surviving_consumer_id
    ));
    assert!(index_contains(
        &snapshot.producer_consumers,
        producer_id,
        surviving_consumer_id
    ));
    assert!(!index_contains(
        &snapshot.producer_consumers,
        producer_id,
        removed_consumer_id
    ));
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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

    let snapshot = router_state_snapshot(&router);
    assert_consumer_route(
        &snapshot,
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    );
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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

    let snapshot = router_state_snapshot(&router);
    assert_consumer_route(
        &snapshot,
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    );
    assert_consumer_route(
        &snapshot,
        ConsumerId(41),
        ConsumerRouteState::Active,
        ProducerRouteState::Paused,
    );
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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

    let snapshot = router_state_snapshot(&router);
    assert_consumer_route(
        &snapshot,
        ConsumerId(40),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    );
    assert_consumer_route(
        &snapshot,
        ConsumerId(41),
        ConsumerRouteState::Active,
        ProducerRouteState::Active,
    );
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

#[kani::proof]
#[kani::unwind(64)]
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
    let snapshot = router_state_snapshot(&router);
    assert_consumer_route(
        &snapshot,
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    );
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Paused)
            .is_ok()
    );
    let snapshot = router_state_snapshot(&router);
    assert_consumer_route(
        &snapshot,
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Paused,
    );
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);

    assert!(
        router
            .set_producer_route_state(ProducerId(30), ProducerRouteState::Active)
            .is_ok()
    );
    let snapshot = router_state_snapshot(&router);
    assert_consumer_route(
        &snapshot,
        ConsumerId(40),
        ConsumerRouteState::Paused,
        ProducerRouteState::Active,
    );
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
    std::mem::forget(router);
}

fn apply_symbolic_command(router: &mut Router, command: u8) {
    match command % SYMBOLIC_COMMAND_VARIANTS {
        0 => {
            let _ = router.join_session(user(SessionId(1)));
        }
        1 => {
            let _ = router.join_session(user(SessionId(2)));
        }
        2 => {
            let _ = router.open_transport(Transport::new(
                TransportId(10),
                SessionId(1),
                TransportDirection::Receive,
            ));
        }
        3 => {
            let _ = router.open_transport(Transport::new(
                TransportId(11),
                SessionId(1),
                TransportDirection::Send,
            ));
        }
        4 => {
            let _ = router.open_transport(Transport::new(
                TransportId(20),
                SessionId(2),
                TransportDirection::Receive,
            ));
        }
        5 => {
            let _ = router.open_transport(Transport::new(
                TransportId(21),
                SessionId(2),
                TransportDirection::Send,
            ));
        }
        6 => {
            let _ = router.add_producer(Producer::new(
                ProducerId(30),
                TransportId(10),
                MediaKind::Audio,
            ));
        }
        7 => {
            let _ = router.add_producer(Producer::new(
                ProducerId(31),
                TransportId(20),
                MediaKind::Video,
            ));
        }
        8 => {
            let _ = router.add_consumer(
                Consumer::new(
                    ConsumerId(40),
                    ProducerId(30),
                    TransportId(21),
                    MediaKind::Audio,
                ),
                ConsumerCapability::Compatible,
            );
        }
        9 => {
            let _ = router.add_consumer(
                Consumer::new(
                    ConsumerId(40),
                    ProducerId(30),
                    TransportId(21),
                    MediaKind::Audio,
                ),
                ConsumerCapability::Incompatible,
            );
        }
        10 => {
            let _ = router.add_consumer(
                Consumer::new(
                    ConsumerId(41),
                    ProducerId(31),
                    TransportId(11),
                    MediaKind::Video,
                ),
                ConsumerCapability::Compatible,
            );
        }
        11 => {
            let _ = router.add_consumer(
                Consumer::new(
                    ConsumerId(41),
                    ProducerId(31),
                    TransportId(11),
                    MediaKind::Video,
                ),
                ConsumerCapability::Incompatible,
            );
        }
        12 => {
            let _ = router.set_producer_route_state(ProducerId(30), ProducerRouteState::Paused);
        }
        13 => {
            let _ = router.set_producer_route_state(ProducerId(30), ProducerRouteState::Active);
        }
        14 => {
            let _ = router.set_producer_route_state(ProducerId(31), ProducerRouteState::Paused);
        }
        15 => {
            let _ = router.set_producer_route_state(ProducerId(31), ProducerRouteState::Active);
        }
        16 => {
            let _ = router.set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Paused);
        }
        17 => {
            let _ = router.set_consumer_route_state(ConsumerId(40), ConsumerRouteState::Active);
        }
        18 => {
            let _ = router.set_consumer_route_state(ConsumerId(41), ConsumerRouteState::Paused);
        }
        19 => {
            let _ = router.set_consumer_route_state(ConsumerId(41), ConsumerRouteState::Active);
        }
        20 => {
            let _ = router.remove_consumer(ConsumerId(40));
        }
        21 => {
            let _ = router.remove_consumer(ConsumerId(41));
        }
        22 => {
            let _ = router.remove_producer(ProducerId(30));
        }
        23 => {
            let _ = router.remove_producer(ProducerId(31));
        }
        24 => {
            let _ = router.remove_session(SessionId(1));
        }
        _ => {
            let _ = router.remove_session(SessionId(2));
        }
    }
}

fn assert_production_router_invariants(router: &Router) {
    let snapshot = router_state_snapshot(router);
    assert_snapshot_satisfies_invariants(&snapshot);
    std::mem::forget(snapshot);
}

fn assert_snapshot_satisfies_invariants(snapshot: &RouterStateSnapshot) {
    assert!(session_ids_are_unique(snapshot));
    assert!(live_sessions_are_active(snapshot));
    assert!(transport_ids_are_unique(snapshot));
    assert!(producer_ids_are_unique(snapshot));
    assert!(consumer_ids_are_unique(snapshot));
    assert!(references_are_valid(snapshot));
    assert!(reverse_indices_are_exact(snapshot));
    assert!(transport_directions_are_valid(snapshot));
    assert!(consumer_media_matches_producer(snapshot));
    assert!(consumer_pause_shadows_producer(snapshot));
}

fn session_ids_are_unique(snapshot: &RouterStateSnapshot) -> bool {
    let mut left_index = 0;
    while left_index < snapshot.sessions.len() {
        let left_id = snapshot.sessions[left_index].0;
        let mut right_index = left_index + 1;
        while right_index < snapshot.sessions.len() {
            if snapshot.sessions[right_index].0 == left_id {
                return false;
            }
            right_index += 1;
        }
        left_index += 1;
    }
    true
}

fn live_sessions_are_active(snapshot: &RouterStateSnapshot) -> bool {
    let mut index = 0;
    while index < snapshot.sessions.len() {
        if snapshot.sessions[index].1 != SessionState::Active {
            return false;
        }
        index += 1;
    }
    true
}

fn transport_ids_are_unique(snapshot: &RouterStateSnapshot) -> bool {
    let mut left_index = 0;
    while left_index < snapshot.transports.len() {
        let left_id = snapshot.transports[left_index].0;
        let mut right_index = left_index + 1;
        while right_index < snapshot.transports.len() {
            if snapshot.transports[right_index].0 == left_id {
                return false;
            }
            right_index += 1;
        }
        left_index += 1;
    }
    true
}

fn producer_ids_are_unique(snapshot: &RouterStateSnapshot) -> bool {
    let mut left_index = 0;
    while left_index < snapshot.producers.len() {
        let left_id = snapshot.producers[left_index].0;
        let mut right_index = left_index + 1;
        while right_index < snapshot.producers.len() {
            if snapshot.producers[right_index].0 == left_id {
                return false;
            }
            right_index += 1;
        }
        left_index += 1;
    }
    true
}

fn consumer_ids_are_unique(snapshot: &RouterStateSnapshot) -> bool {
    let mut left_index = 0;
    while left_index < snapshot.consumers.len() {
        let left_id = snapshot.consumers[left_index].0;
        let mut right_index = left_index + 1;
        while right_index < snapshot.consumers.len() {
            if snapshot.consumers[right_index].0 == left_id {
                return false;
            }
            right_index += 1;
        }
        left_index += 1;
    }
    true
}

fn references_are_valid(snapshot: &RouterStateSnapshot) -> bool {
    let mut transport_index = 0;
    while transport_index < snapshot.transports.len() {
        if !contains_session(snapshot, snapshot.transports[transport_index].1) {
            return false;
        }
        transport_index += 1;
    }

    let mut producer_index = 0;
    while producer_index < snapshot.producers.len() {
        if !contains_transport(snapshot, snapshot.producers[producer_index].1) {
            return false;
        }
        producer_index += 1;
    }

    let mut consumer_index = 0;
    while consumer_index < snapshot.consumers.len() {
        let consumer = snapshot.consumers[consumer_index];
        if !contains_transport(snapshot, consumer.2) || !contains_producer(snapshot, consumer.1) {
            return false;
        }
        consumer_index += 1;
    }

    true
}

fn reverse_indices_are_exact(snapshot: &RouterStateSnapshot) -> bool {
    session_transport_index_is_exact(snapshot)
        && transport_producer_index_is_exact(snapshot)
        && transport_consumer_index_is_exact(snapshot)
        && producer_consumer_index_is_exact(snapshot)
}

fn session_transport_index_is_exact(snapshot: &RouterStateSnapshot) -> bool {
    let mut entry_index = 0;
    while entry_index < snapshot.session_transports.len() {
        let (session_id, transport_ids) = &snapshot.session_transports[entry_index];
        if !contains_session(snapshot, *session_id) || transport_ids.is_empty() {
            return false;
        }

        let mut value_index = 0;
        while value_index < transport_ids.len() {
            let Some(transport) = transport_by_id(snapshot, transport_ids[value_index]) else {
                return false;
            };
            if transport.1 != *session_id {
                return false;
            }
            value_index += 1;
        }
        entry_index += 1;
    }

    let mut transport_index = 0;
    while transport_index < snapshot.transports.len() {
        let transport = snapshot.transports[transport_index];
        if !index_contains(&snapshot.session_transports, transport.1, transport.0) {
            return false;
        }
        transport_index += 1;
    }

    true
}

fn transport_producer_index_is_exact(snapshot: &RouterStateSnapshot) -> bool {
    let mut entry_index = 0;
    while entry_index < snapshot.transport_producers.len() {
        let (transport_id, producer_ids) = &snapshot.transport_producers[entry_index];
        if !contains_transport(snapshot, *transport_id) || producer_ids.is_empty() {
            return false;
        }

        let mut value_index = 0;
        while value_index < producer_ids.len() {
            let Some(producer) = producer_by_id(snapshot, producer_ids[value_index]) else {
                return false;
            };
            if producer.1 != *transport_id {
                return false;
            }
            value_index += 1;
        }
        entry_index += 1;
    }

    let mut producer_index = 0;
    while producer_index < snapshot.producers.len() {
        let producer = snapshot.producers[producer_index];
        if !index_contains(&snapshot.transport_producers, producer.1, producer.0) {
            return false;
        }
        producer_index += 1;
    }

    true
}

fn transport_consumer_index_is_exact(snapshot: &RouterStateSnapshot) -> bool {
    let mut entry_index = 0;
    while entry_index < snapshot.transport_consumers.len() {
        let (transport_id, consumer_ids) = &snapshot.transport_consumers[entry_index];
        if !contains_transport(snapshot, *transport_id) || consumer_ids.is_empty() {
            return false;
        }

        let mut value_index = 0;
        while value_index < consumer_ids.len() {
            let Some(consumer) = consumer_by_id(snapshot, consumer_ids[value_index]) else {
                return false;
            };
            if consumer.2 != *transport_id {
                return false;
            }
            value_index += 1;
        }
        entry_index += 1;
    }

    let mut consumer_index = 0;
    while consumer_index < snapshot.consumers.len() {
        let consumer = snapshot.consumers[consumer_index];
        if !index_contains(&snapshot.transport_consumers, consumer.2, consumer.0) {
            return false;
        }
        consumer_index += 1;
    }

    true
}

fn producer_consumer_index_is_exact(snapshot: &RouterStateSnapshot) -> bool {
    let mut entry_index = 0;
    while entry_index < snapshot.producer_consumers.len() {
        let (producer_id, consumer_ids) = &snapshot.producer_consumers[entry_index];
        if !contains_producer(snapshot, *producer_id) || consumer_ids.is_empty() {
            return false;
        }

        let mut value_index = 0;
        while value_index < consumer_ids.len() {
            let Some(consumer) = consumer_by_id(snapshot, consumer_ids[value_index]) else {
                return false;
            };
            if consumer.1 != *producer_id {
                return false;
            }
            value_index += 1;
        }
        entry_index += 1;
    }

    let mut consumer_index = 0;
    while consumer_index < snapshot.consumers.len() {
        let consumer = snapshot.consumers[consumer_index];
        if !index_contains(&snapshot.producer_consumers, consumer.1, consumer.0) {
            return false;
        }
        consumer_index += 1;
    }

    true
}

fn transport_directions_are_valid(snapshot: &RouterStateSnapshot) -> bool {
    let mut producer_index = 0;
    while producer_index < snapshot.producers.len() {
        let Some(transport) = transport_by_id(snapshot, snapshot.producers[producer_index].1)
        else {
            return false;
        };
        if transport.2 != TransportDirection::Receive {
            return false;
        }
        producer_index += 1;
    }

    let mut consumer_index = 0;
    while consumer_index < snapshot.consumers.len() {
        let Some(transport) = transport_by_id(snapshot, snapshot.consumers[consumer_index].2)
        else {
            return false;
        };
        if transport.2 != TransportDirection::Send {
            return false;
        }
        consumer_index += 1;
    }

    true
}

fn consumer_media_matches_producer(snapshot: &RouterStateSnapshot) -> bool {
    let mut consumer_index = 0;
    while consumer_index < snapshot.consumers.len() {
        let consumer = snapshot.consumers[consumer_index];
        let Some(producer) = producer_by_id(snapshot, consumer.1) else {
            return false;
        };
        if consumer.3 != producer.2 {
            return false;
        }
        consumer_index += 1;
    }
    true
}

fn consumer_pause_shadows_producer(snapshot: &RouterStateSnapshot) -> bool {
    let mut consumer_index = 0;
    while consumer_index < snapshot.consumers.len() {
        let consumer = snapshot.consumers[consumer_index];
        let Some(producer) = producer_by_id(snapshot, consumer.1) else {
            return false;
        };
        if consumer.5 != producer.3 {
            return false;
        }
        consumer_index += 1;
    }
    true
}

fn assert_consumer_route(
    snapshot: &RouterStateSnapshot,
    consumer_id: ConsumerId,
    route_state: ConsumerRouteState,
    producer_route_state: ProducerRouteState,
) {
    let Some(consumer) = consumer_by_id(snapshot, consumer_id) else {
        assert!(false);
        return;
    };
    assert!(consumer.4 == route_state);
    assert!(consumer.5 == producer_route_state);
}

fn contains_session(snapshot: &RouterStateSnapshot, session_id: SessionId) -> bool {
    let mut index = 0;
    while index < snapshot.sessions.len() {
        if snapshot.sessions[index].0 == session_id {
            return true;
        }
        index += 1;
    }
    false
}

fn contains_transport(snapshot: &RouterStateSnapshot, transport_id: TransportId) -> bool {
    transport_by_id(snapshot, transport_id).is_some()
}

fn contains_producer(snapshot: &RouterStateSnapshot, producer_id: ProducerId) -> bool {
    producer_by_id(snapshot, producer_id).is_some()
}

fn contains_consumer(snapshot: &RouterStateSnapshot, consumer_id: ConsumerId) -> bool {
    consumer_by_id(snapshot, consumer_id).is_some()
}

fn transport_by_id(
    snapshot: &RouterStateSnapshot,
    transport_id: TransportId,
) -> Option<(TransportId, SessionId, TransportDirection)> {
    let mut index = 0;
    while index < snapshot.transports.len() {
        let transport = snapshot.transports[index];
        if transport.0 == transport_id {
            return Some(transport);
        }
        index += 1;
    }
    None
}

fn producer_by_id(
    snapshot: &RouterStateSnapshot,
    producer_id: ProducerId,
) -> Option<(ProducerId, TransportId, MediaKind, ProducerRouteState)> {
    let mut index = 0;
    while index < snapshot.producers.len() {
        let producer = snapshot.producers[index];
        if producer.0 == producer_id {
            return Some(producer);
        }
        index += 1;
    }
    None
}

fn consumer_by_id(
    snapshot: &RouterStateSnapshot,
    consumer_id: ConsumerId,
) -> Option<(
    ConsumerId,
    ProducerId,
    TransportId,
    MediaKind,
    ConsumerRouteState,
    ProducerRouteState,
)> {
    let mut index = 0;
    while index < snapshot.consumers.len() {
        let consumer = snapshot.consumers[index];
        if consumer.0 == consumer_id {
            return Some(consumer);
        }
        index += 1;
    }
    None
}

fn index_contains_key<K: Copy + Eq, V>(index: &[(K, Vec<V>)], key: K) -> bool {
    let mut entry_index = 0;
    while entry_index < index.len() {
        if index[entry_index].0 == key {
            return true;
        }
        entry_index += 1;
    }
    false
}

fn index_contains<K: Copy + Eq, V: Copy + Eq>(index: &[(K, Vec<V>)], key: K, value: V) -> bool {
    let mut entry_index = 0;
    while entry_index < index.len() {
        let (entry_key, values) = &index[entry_index];
        if *entry_key == key {
            let mut value_index = 0;
            while value_index < values.len() {
                if values[value_index] == value {
                    return true;
                }
                value_index += 1;
            }
            return false;
        }
        entry_index += 1;
    }
    false
}

fn index_member_count<K: Copy + Eq, V>(index: &[(K, Vec<V>)], key: K) -> usize {
    let mut entry_index = 0;
    while entry_index < index.len() {
        let (entry_key, values) = &index[entry_index];
        if *entry_key == key {
            return values.len();
        }
        entry_index += 1;
    }
    0
}
