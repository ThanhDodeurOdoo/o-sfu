use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::{
    runtime::{
        recording::{MediaFrameSink, MediaSource, MediaTap, into_frame_sink},
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

impl MediaFrameSink for CountingSink {
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

    tap.write_frame(
        &session_key,
        TransportMediaId::default(),
        Instant::now(),
        b"payload",
    );
}

#[test]
fn media_tap_routes_packets_only_for_active_channels() {
    let tap = MediaTap::default();
    let counting_sink = Arc::new(CountingSink::new());
    let sink = into_frame_sink(Arc::<CountingSink>::clone(&counting_sink));
    let active_session = TransportSessionKey::new(10, 0, 1, SessionId::Integer(1));
    let inactive_session = TransportSessionKey::new(11, 0, 1, SessionId::Integer(2));

    tap.activate_channel(10, sink);
    tap.write_frame(
        &active_session,
        TransportMediaId::new(3),
        Instant::now(),
        b"first",
    );
    tap.write_frame(
        &inactive_session,
        TransportMediaId::new(4),
        Instant::now(),
        b"second",
    );

    assert_eq!(counting_sink.frames.load(Ordering::Relaxed), 1);
}
