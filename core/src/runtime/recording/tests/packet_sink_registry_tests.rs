use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::runtime::{
    RoomInstanceId, UserId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::RtpForwardDestinationKind,
    packet_sink_registry::{PacketSinkRouteCache, RoomPacketSinkRegistry},
    recording::{MediaPacketSink, into_packet_sink},
    rtc_engine::test_support::{sample_forwarded_packet, test_transport_session_key},
};

struct CountingSink {
    frames: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            frames: AtomicUsize::new(0),
        }
    }
}

struct PayloadCapturingSink {
    payloads: Mutex<Vec<Vec<u8>>>,
}

impl PayloadCapturingSink {
    fn new() -> Self {
        Self {
            payloads: Mutex::new(Vec::new()),
        }
    }
}

impl MediaPacketSink for CountingSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        self.frames.fetch_add(1, Ordering::Relaxed);
    }
}

impl MediaPacketSink for PayloadCapturingSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        payload: &[u8],
    ) {
        self.payloads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(payload.to_vec());
    }
}

fn register_recording_sink<T>(
    registry: &RoomPacketSinkRegistry,
    room_instance_id: RoomInstanceId,
    sink: Arc<T>,
) where
    T: MediaPacketSink + 'static,
{
    registry.register_room(
        room_instance_id,
        into_packet_sink(sink),
        RtpForwardDestinationKind::Recording,
    );
}

#[test]
fn packet_sink_registry_is_a_noop_when_no_room_is_active() {
    let registry = RoomPacketSinkRegistry::default();
    let session_key = test_transport_session_key(10, 0, 1, UserId::Integer(1));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    registry.write_packet(&packet, TransportMediaId::default());
}

#[test]
fn packet_sink_registry_routes_packets_only_for_active_rooms() {
    let registry = RoomPacketSinkRegistry::default();
    let counting_sink = Arc::new(CountingSink::new());
    let active_packet = sample_forwarded_packet(
        test_transport_session_key(10, 0, 1, UserId::Integer(1)),
        "aud-up",
        b"first",
    );
    let inactive_packet = sample_forwarded_packet(
        test_transport_session_key(11, 0, 1, UserId::Integer(2)),
        "aud-up",
        b"second",
    );

    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(10),
        Arc::<CountingSink>::clone(&counting_sink),
    );
    registry.write_packet(&active_packet, TransportMediaId::new(3));
    registry.write_packet(&inactive_packet, TransportMediaId::new(4));

    assert_eq!(counting_sink.frames.load(Ordering::Relaxed), 1);
}

#[test]
fn packet_sink_registry_exposes_the_active_room_sink_for_forwarding_destinations() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());

    assert!(
        registry
            .sink_for_room(RoomInstanceId::from_raw(10))
            .is_none()
    );
    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(10),
        Arc::<CountingSink>::clone(&sink),
    );

    assert!(
        registry
            .sink_for_room(RoomInstanceId::from_raw(10))
            .is_some()
    );
    assert!(
        registry
            .sink_for_room(RoomInstanceId::from_raw(11))
            .is_none()
    );
}

#[test]
fn packet_sink_route_cache_refreshes_after_registry_changes() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());
    let mut cache = PacketSinkRouteCache::default();
    let room_id = RoomInstanceId::from_raw(13);

    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_none());

    register_recording_sink(&registry, room_id, Arc::<CountingSink>::clone(&sink));

    assert!(cache.sink_for_room(room_id).is_none());
    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_some());

    registry.unregister_room(room_id);

    assert!(cache.sink_for_room(room_id).is_some());
    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_none());
}

#[test]
fn packet_sink_registry_keeps_multiple_rooms_active_at_once() {
    let registry = RoomPacketSinkRegistry::default();
    let first_sink = Arc::new(CountingSink::new());
    let second_sink = Arc::new(CountingSink::new());
    let first_packet = sample_forwarded_packet(
        test_transport_session_key(10, 0, 1, UserId::Integer(1)),
        "aud-up",
        b"first",
    );
    let second_packet = sample_forwarded_packet(
        test_transport_session_key(11, 0, 1, UserId::Integer(2)),
        "aud-up",
        b"second",
    );

    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(10),
        Arc::<CountingSink>::clone(&first_sink),
    );
    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(11),
        Arc::<CountingSink>::clone(&second_sink),
    );
    registry.write_packet(&first_packet, TransportMediaId::new(3));
    registry.write_packet(&second_packet, TransportMediaId::new(4));

    assert_eq!(first_sink.frames.load(Ordering::Relaxed), 1);
    assert_eq!(second_sink.frames.load(Ordering::Relaxed), 1);
}

#[test]
fn packet_sink_registry_records_forwarded_payload_bytes_through_the_shared_boundary() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(PayloadCapturingSink::new());
    let packet = sample_forwarded_packet(
        test_transport_session_key(12, 0, 1, UserId::Integer(3)),
        "aud-up",
        b"captured",
    );

    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(12),
        Arc::<PayloadCapturingSink>::clone(&sink),
    );
    registry.write_packet(&packet, TransportMediaId::new(5));

    let payloads = sink
        .payloads
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(payloads.as_slice(), [b"captured".to_vec()]);
}
