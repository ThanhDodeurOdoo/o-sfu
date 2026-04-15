use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

use crate::{
    runtime::{
        recording::{MediaPacketSink, MediaSource, MediaTap, into_packet_sink},
        rtc_adapter::sample_forwarded_packet,
        transport_adapter::{TransportMediaId, TransportSessionKey},
    },
    signaling::shared::SessionId,
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

#[test]
fn media_tap_is_a_noop_when_no_channel_is_active() {
    let tap = MediaTap::default();
    let session_key = TransportSessionKey::new(10, 0, 1, SessionId::Integer(1));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    tap.write_packet(&packet, TransportMediaId::default());
}

#[test]
fn media_tap_routes_packets_only_for_active_channels() {
    let tap = MediaTap::default();
    let counting_sink = Arc::new(CountingSink::new());
    let active_packet = sample_forwarded_packet(
        TransportSessionKey::new(10, 0, 1, SessionId::Integer(1)),
        "aud-up",
        b"first",
    );
    let inactive_packet = sample_forwarded_packet(
        TransportSessionKey::new(11, 0, 1, SessionId::Integer(2)),
        "aud-up",
        b"second",
    );

    tap.activate_channel(
        10,
        into_packet_sink(Arc::<CountingSink>::clone(&counting_sink)),
    );
    tap.write_packet(&active_packet, TransportMediaId::new(3));
    tap.write_packet(&inactive_packet, TransportMediaId::new(4));

    assert_eq!(counting_sink.frames.load(Ordering::Relaxed), 1);
}

#[test]
fn media_tap_keeps_multiple_channels_active_at_once() {
    let tap = MediaTap::default();
    let first_sink = Arc::new(CountingSink::new());
    let second_sink = Arc::new(CountingSink::new());
    let first_packet = sample_forwarded_packet(
        TransportSessionKey::new(10, 0, 1, SessionId::Integer(1)),
        "aud-up",
        b"first",
    );
    let second_packet = sample_forwarded_packet(
        TransportSessionKey::new(11, 0, 1, SessionId::Integer(2)),
        "aud-up",
        b"second",
    );

    tap.activate_channel(
        10,
        into_packet_sink(Arc::<CountingSink>::clone(&first_sink)),
    );
    tap.activate_channel(
        11,
        into_packet_sink(Arc::<CountingSink>::clone(&second_sink)),
    );
    tap.write_packet(&first_packet, TransportMediaId::new(3));
    tap.write_packet(&second_packet, TransportMediaId::new(4));

    assert_eq!(first_sink.frames.load(Ordering::Relaxed), 1);
    assert_eq!(second_sink.frames.load(Ordering::Relaxed), 1);
}
