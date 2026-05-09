//! miri coverage for packet-loop state that must stay valid under Rust's
//! strict aliasing and allocation rules
//!
//! these tests focus on pure packet-loop pieces that are cheap enough to run
//! under Miri: the recent routing miss cache, scheduler deadlines, scratch reuse
//! and idle turns because those surfaces sit on the RTP hot path
//! refactors can easily create stale references, dropped capacity or stale wakeups

use std::net::SocketAddr;

use o_sfu_core::{
    ConnectionId, RoomInstanceId,
    server::{
        session::UserId,
        transport::packet_loop_verification::{
            PacketLoopEffects, PacketLoopRouteSnapshot, PacketLoopRoutingMissKey,
            PacketLoopRoutingState, PacketLoopScratch, PacketLoopState, PacketLoopTime,
            PacketLoopTurn, PacketLoopTurnInput,
        },
    },
    transport::TransportSessionKey,
};

/// checks that recent routing miss cache entries match only the exact observed packet
///
/// this protects unknown-source recovery from skipping a later scan for a
/// similar packet after topology changes or a successful fallback route
#[test]
fn demux_negative_state_is_exact_and_clearable() {
    let source = SocketAddr::from(([127, 0, 0, 1], 41_000));
    let candidate = SocketAddr::from(([127, 0, 0, 1], 41_001));
    let packet = [0x80, 0x60, 0x00, 0x01, 0x55];
    let mutated = [0x80, 0x60, 0x00, 0x02, 0x55];
    let mut demux = PacketLoopRoutingState::new();
    let miss_key = PacketLoopRoutingMissKey::new(source, candidate, &packet);
    let mutated_key = PacketLoopRoutingMissKey::new(source, candidate, &mutated);

    demux.record_miss(miss_key, &packet, source, PacketLoopTime::from_millis(0));

    assert!(demux.should_skip_scan(miss_key, &packet));
    assert!(!demux.should_skip_scan(mutated_key, &mutated));

    demux.record_fallback_route_success(miss_key, &packet, source);
    assert!(!demux.should_skip_scan(miss_key, &packet));

    demux.record_miss(miss_key, &packet, source, PacketLoopTime::from_millis(1));
    demux.clear_on_topology_change();
    assert!(!demux.should_skip_scan(miss_key, &packet));
}

/// checks that timeout scheduling ignores obsolete heap entries
///
/// this keeps dirty sessions and timeout wakeups deterministic when a session
/// updates its deadline before the previous deadline is drained
#[test]
fn scheduler_ignores_stale_deadlines_and_drains_dirty_sessions() {
    let session = session_key(7, 11);
    let mut schedule = PacketLoopState::default();
    let mut ready_sessions = Vec::new();

    schedule.update_session_timeout(&session, Some(PacketLoopTime::from_millis(50)));
    schedule.update_session_timeout(&session, Some(PacketLoopTime::from_millis(10)));

    assert_eq!(
        schedule.next_timeout_deadline(),
        Some(PacketLoopTime::from_millis(10))
    );
    schedule.drain_ready_sessions(PacketLoopTime::from_millis(9), &mut ready_sessions);
    assert_eq!(ready_sessions.len(), 0);
    schedule.drain_ready_sessions(PacketLoopTime::from_millis(10), &mut ready_sessions);
    assert_eq!(ready_sessions.len(), 1);
    assert_eq!(schedule.next_timeout_deadline(), None);

    schedule.update_session_timeout(&session, Some(PacketLoopTime::from_millis(25)));
    schedule.mark_session_dirty(&session);
    schedule.drain_ready_sessions(PacketLoopTime::from_millis(20), &mut ready_sessions);
    assert_eq!(ready_sessions.len(), 1);
    assert_eq!(
        schedule.next_timeout_deadline(),
        Some(PacketLoopTime::from_millis(25))
    );
    schedule.clear_session_schedule(&session);
    assert_eq!(schedule.next_timeout_deadline(), None);
}

/// checks that reusable packet-loop scratch keeps capacity across idle turns
///
/// this is useful because the hot path relies on stable scratch ownership
/// instead of allocating fresh buffers every time the worker has no packets
#[test]
fn scratch_reuse_and_idle_turn_stay_pure() {
    let mut state = PacketLoopState::default();
    let mut scratch = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();
    let mut session_outputs = Vec::new();
    let mut relay_packets = Vec::new();
    let routes = PacketLoopRouteSnapshot::default();
    let payload = [0xA5; 24];

    for idx in 0..32 {
        let port = 40_000_u16.saturating_add(idx);
        scratch.push_pending_transmit(SocketAddr::from(([127, 0, 0, 1], port)), &payload);
        scratch.mark_source_policy_dirty(RoomInstanceId::from_raw(u64::from(idx)));
    }
    let warmed = scratch.capacities();
    scratch.clear();

    assert!(scratch.is_turn_empty());
    assert!(scratch.capacities().retained_at_least(warmed));

    for now_ms in 0..16 {
        PacketLoopTurn::step(
            &mut state,
            &mut scratch,
            &mut effects,
            PacketLoopTurnInput::new(
                PacketLoopTime::from_millis(now_ms),
                &mut session_outputs,
                &mut relay_packets,
                &routes,
            ),
        );
        assert_eq!(effects.invalid_reference_count(&scratch), 0);
    }
}

fn session_key(room_instance_id: u64, connection_id: u64) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        0,
        ConnectionId::from_raw(connection_id),
        UserId::Integer(i64::try_from(connection_id).unwrap_or(i64::MAX)),
    )
}
