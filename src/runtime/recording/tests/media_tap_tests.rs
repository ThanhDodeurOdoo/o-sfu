use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

use crate::runtime::{
    ChannelInstanceId,
    recording::{MediaPacketSink, MediaSource, MediaTap, into_packet_sink},
    rtc_adapter::{sample_forwarded_packet, test_support::test_transport_session_key},
    transport_adapter::{TransportMediaId, TransportSessionKey},
};
use o_sfu_protocol::shared::SessionId;

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

#[test]
fn media_tap_is_a_noop_when_no_channel_is_active() {
    let tap = MediaTap::default();
    let session_key = test_transport_session_key(10, 0, 1, SessionId::Integer(1));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    tap.write_packet(&packet, TransportMediaId::default());
}

#[test]
fn media_tap_routes_packets_only_for_active_channels() {
    let tap = MediaTap::default();
    let counting_sink = Arc::new(CountingSink::new());
    let active_packet = sample_forwarded_packet(
        test_transport_session_key(10, 0, 1, SessionId::Integer(1)),
        "aud-up",
        b"first",
    );
    let inactive_packet = sample_forwarded_packet(
        test_transport_session_key(11, 0, 1, SessionId::Integer(2)),
        "aud-up",
        b"second",
    );

    tap.activate_channel(
        ChannelInstanceId::from_raw(10),
        into_packet_sink(Arc::<CountingSink>::clone(&counting_sink)),
    );
    tap.write_packet(&active_packet, TransportMediaId::new(3));
    tap.write_packet(&inactive_packet, TransportMediaId::new(4));

    assert_eq!(counting_sink.frames.load(Ordering::Relaxed), 1);
}

#[test]
fn media_tap_exposes_the_active_channel_sink_for_forwarding_destinations() {
    let tap = MediaTap::default();
    let sink = Arc::new(CountingSink::new());

    assert!(
        tap.sink_for_channel(ChannelInstanceId::from_raw(10))
            .is_none()
    );
    tap.activate_channel(
        ChannelInstanceId::from_raw(10),
        into_packet_sink(Arc::<CountingSink>::clone(&sink)),
    );

    assert!(
        tap.sink_for_channel(ChannelInstanceId::from_raw(10))
            .is_some()
    );
    assert!(
        tap.sink_for_channel(ChannelInstanceId::from_raw(11))
            .is_none()
    );
}

#[test]
fn media_tap_keeps_multiple_channels_active_at_once() {
    let tap = MediaTap::default();
    let first_sink = Arc::new(CountingSink::new());
    let second_sink = Arc::new(CountingSink::new());
    let first_packet = sample_forwarded_packet(
        test_transport_session_key(10, 0, 1, SessionId::Integer(1)),
        "aud-up",
        b"first",
    );
    let second_packet = sample_forwarded_packet(
        test_transport_session_key(11, 0, 1, SessionId::Integer(2)),
        "aud-up",
        b"second",
    );

    tap.activate_channel(
        ChannelInstanceId::from_raw(10),
        into_packet_sink(Arc::<CountingSink>::clone(&first_sink)),
    );
    tap.activate_channel(
        ChannelInstanceId::from_raw(11),
        into_packet_sink(Arc::<CountingSink>::clone(&second_sink)),
    );
    tap.write_packet(&first_packet, TransportMediaId::new(3));
    tap.write_packet(&second_packet, TransportMediaId::new(4));

    assert_eq!(first_sink.frames.load(Ordering::Relaxed), 1);
    assert_eq!(second_sink.frames.load(Ordering::Relaxed), 1);
}

#[test]
fn media_tap_records_forwarded_payload_bytes_through_the_shared_boundary() {
    let tap = MediaTap::default();
    let sink = Arc::new(PayloadCapturingSink::new());
    let packet = sample_forwarded_packet(
        test_transport_session_key(12, 0, 1, SessionId::Integer(3)),
        "aud-up",
        b"captured",
    );

    tap.activate_channel(
        ChannelInstanceId::from_raw(12),
        into_packet_sink(Arc::<PayloadCapturingSink>::clone(&sink)),
    );
    tap.write_packet(&packet, TransportMediaId::new(5));

    let payloads = sink
        .payloads
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(payloads.as_slice(), [b"captured".to_vec()]);
}
