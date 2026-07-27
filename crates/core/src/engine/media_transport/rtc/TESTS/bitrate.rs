use std::{
    slice,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    thread,
};

use super::*;
use crate::engine::{UserId, media_transport::rtc::test_support::test_transport_session_key};

#[test]
fn incoming_media_bitrate_reports_recent_bits() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert!(bitrate.record(now, 120));

    assert_eq!(bitrate.snapshot(now), Bitrate::from_bps(960));
}

#[test]
fn incoming_media_bitrate_accumulates_same_window_bytes() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert!(bitrate.record(now, 120));
    assert!(!bitrate.record(now + Duration::from_millis(200), 30));

    assert_eq!(
        bitrate.snapshot(now + Duration::from_millis(200)),
        Bitrate::from_bps(1_200)
    );
}

#[test]
fn incoming_media_bitrate_resets_before_recording_new_window() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert!(bitrate.record(now, 120));
    assert!(!bitrate.record(now + Duration::from_secs(1), 30));

    assert_eq!(
        bitrate.snapshot(now + Duration::from_secs(1)),
        Bitrate::from_bps(240)
    );
}

#[test]
fn incoming_media_bitrate_expires_after_the_window() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert!(bitrate.record(now, 64));

    assert_eq!(
        bitrate.snapshot(now + Duration::from_secs(1) + Duration::from_millis(1)),
        Bitrate::zero()
    );
}

#[test]
fn egress_bitrate_snapshot_reports_recent_session_bits() {
    let now = Instant::now();
    let session_key = test_transport_session_key(0, 0, 0, UserId::Integer(7));
    let mut state = BitrateRegistry::default();
    let counter = Arc::new(MediaBitrateCounter::new(now));
    state.register_session_egress(&session_key, Arc::clone(&counter));

    assert!(counter.record(now, 125));

    assert_eq!(
        state.egress_bitrate_snapshot_at(&[session_key], now),
        Bitrate::from_bps(1_000)
    );
}

#[test]
fn incoming_media_bitrate_first_observation_fires_once() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert!(bitrate.record(now, 1));
    assert!(!bitrate.record(now, 1));
}

#[test]
fn bitrate_snapshot_observes_packet_loop_thread_writes() {
    let now = Instant::now();
    let bitrate = Arc::new(MediaBitrateCounter::new(now));
    let writer = Arc::clone(&bitrate);
    let started = Arc::new(AtomicBool::new(false));
    let writer_started = Arc::clone(&started);

    let handle = thread::spawn(move || {
        writer_started.store(true, AtomicOrdering::Release);
        for _ in 0..1024 {
            writer.record(now, 10);
        }
    });

    while !started.load(AtomicOrdering::Acquire) {
        thread::yield_now();
    }

    let mut observed_bitrate = Bitrate::zero();
    for _ in 0..1024 {
        observed_bitrate = bitrate.snapshot(now);
        if observed_bitrate > Bitrate::zero() {
            break;
        }
        thread::yield_now();
    }

    assert!(handle.join().is_ok());
    assert!(observed_bitrate > Bitrate::zero() || bitrate.snapshot(now) > Bitrate::zero());
}

#[test]
fn removing_session_hides_registered_counters_from_snapshots() {
    let mut state = BitrateRegistry::default();
    let now = Instant::now();
    let session_key = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let media_id = TransportMediaId::new(4);
    let counter = state.register_incoming_media(&session_key, media_id, now);
    let egress = Arc::new(MediaBitrateCounter::new(now));
    state.register_session_egress(&session_key, Arc::clone(&egress));
    counter.record(now, 16);
    egress.record(now, 16);

    state.remove_session(&session_key);

    let snapshot = state.transport_bitrate_snapshot_at(slice::from_ref(&session_key), now);
    assert_eq!(snapshot, TransportBitrateSnapshot::default());
    assert_eq!(
        state.egress_bitrate_snapshot_at(&[session_key], now),
        Bitrate::zero()
    );
}

#[test]
fn packet_loop_counter_write_does_not_need_the_snapshot_lock() {
    let mut shared_registry = BitrateRegistry::default();
    let mut packet_loop_state = PacketLoopState::default();
    let now = Instant::now();
    let session_key = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let media_id = TransportMediaId::new(4);
    let counter = shared_registry.register_incoming_media(&session_key, media_id, now);
    packet_loop_state.register_incoming_bitrate_counter(media_id, counter);
    let shared_registry = Mutex::new(shared_registry);
    let Ok(_snapshot_guard) = shared_registry.lock() else {
        return;
    };

    assert_eq!(
        packet_loop_state.record_incoming_bitrate(media_id, now, 32),
        Some(true)
    );
}
