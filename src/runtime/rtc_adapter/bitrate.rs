//! Worker-local incoming bitrate accounting for RTC media.
//!
//! The packet loop owns the write side and updates per-media counters through
//! atomics. The shared state lock protects only the cold registration and
//! snapshot map, so operator polling cannot contend with per-packet writes.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::runtime::transport_adapter::{
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};

use super::state::RtcBootstrapState;

const BITRATE_WINDOW_NANOS: u64 = 1_000_000_000;

#[derive(Debug)]
pub(super) struct IncomingMediaBitrate {
    origin: Instant,
    window_start_nanos: AtomicU64,
    bytes_in_window: AtomicU64,
    observed: AtomicBool,
}

impl IncomingMediaBitrate {
    fn new(now: Instant) -> Self {
        Self {
            origin: now,
            window_start_nanos: AtomicU64::new(0),
            bytes_in_window: AtomicU64::new(0),
            observed: AtomicBool::new(false),
        }
    }

    pub(super) fn record(&self, now: Instant, payload_bytes: usize) -> bool {
        let now_nanos = self.nanos_since_origin(now);
        let window_start = self.window_start_nanos.load(Ordering::Acquire);
        if now_nanos.saturating_sub(window_start) >= BITRATE_WINDOW_NANOS {
            self.bytes_in_window.store(0, Ordering::Release);
            self.window_start_nanos.store(now_nanos, Ordering::Release);
        }
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let _ = self
            .bytes_in_window
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(payload_bytes))
            });
        !self.observed.swap(true, Ordering::AcqRel)
    }

    fn snapshot(&self, now: Instant) -> u64 {
        let now_nanos = self.nanos_since_origin(now);
        let window_start = self.window_start_nanos.load(Ordering::Acquire);
        if now_nanos.saturating_sub(window_start) >= BITRATE_WINDOW_NANOS {
            return 0;
        }
        self.bytes_in_window
            .load(Ordering::Acquire)
            .saturating_mul(8)
    }

    fn nanos_since_origin(&self, now: Instant) -> u64 {
        let elapsed = now
            .checked_duration_since(self.origin)
            .unwrap_or(Duration::ZERO);
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Default)]
pub(super) struct SessionIncomingBitrates {
    per_media: BTreeMap<TransportMediaId, Arc<IncomingMediaBitrate>>,
}

impl SessionIncomingBitrates {
    fn register(
        &mut self,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Arc<IncomingMediaBitrate> {
        Arc::clone(
            self.per_media
                .entry(transport_media_id)
                .or_insert_with(|| Arc::new(IncomingMediaBitrate::new(now))),
        )
    }

    fn remove(&mut self, transport_media_id: TransportMediaId) {
        self.per_media.remove(&transport_media_id);
    }

    fn is_empty(&self) -> bool {
        self.per_media.is_empty()
    }

    fn snapshot(&self, now: Instant) -> Vec<(TransportMediaId, u64)> {
        self.per_media
            .iter()
            .filter_map(|(media_id, bitrate)| {
                let bits = bitrate.snapshot(now);
                if bits > 0 {
                    Some((*media_id, bits))
                } else {
                    None
                }
            })
            .collect()
    }

    fn total(&self, now: Instant) -> u64 {
        self.per_media
            .values()
            .map(|bitrate| bitrate.snapshot(now))
            .sum()
    }
}

#[derive(Debug, Default)]
pub(crate) struct RtcBitrateState {
    pub(super) incoming_bitrates_by_session: BTreeMap<TransportSessionKey, SessionIncomingBitrates>,
}

impl RtcBitrateState {
    pub(super) fn register_incoming_media(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Arc<IncomingMediaBitrate> {
        self.incoming_bitrates_by_session
            .entry(session_key.clone())
            .or_default()
            .register(transport_media_id, now)
    }

    pub(super) fn remove_incoming_media(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) {
        let Some(session_bitrates) = self.incoming_bitrates_by_session.get_mut(session_key) else {
            return;
        };
        session_bitrates.remove(transport_media_id);
        if session_bitrates.is_empty() {
            self.incoming_bitrates_by_session.remove(session_key);
        }
    }

    pub(super) fn remove_session(&mut self, session_key: &TransportSessionKey) {
        self.incoming_bitrates_by_session.remove(session_key);
    }

    pub(crate) fn transport_bitrate_snapshot_at(
        &self,
        session_keys: &[TransportSessionKey],
        now: Instant,
    ) -> TransportBitrateSnapshot {
        let mut snapshot = TransportBitrateSnapshot::default();
        for session_key in session_keys {
            let Some(session_bitrates) = self.incoming_bitrates_by_session.get(session_key) else {
                continue;
            };
            snapshot.total = snapshot.total.saturating_add(session_bitrates.total(now));
            snapshot.per_media.extend(session_bitrates.snapshot(now));
        }
        snapshot
    }
}

impl RtcBootstrapState {
    pub(super) fn register_incoming_bitrate_counter(
        &mut self,
        transport_media_id: TransportMediaId,
        counter: Arc<IncomingMediaBitrate>,
    ) {
        self.incoming_bitrate_counters
            .insert(transport_media_id, counter);
    }

    pub(super) fn remove_incoming_bitrate_counter(&mut self, transport_media_id: TransportMediaId) {
        self.incoming_bitrate_counters.remove(&transport_media_id);
    }

    pub(super) fn record_incoming_bitrate(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
        payload_bytes: usize,
    ) -> Option<bool> {
        self.incoming_bitrate_counters
            .get(&transport_media_id)
            .map(|bitrate| bitrate.record(now, payload_bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    use crate::runtime::rtc_adapter::test_support::test_transport_session_key;
    use o_sfu_protocol::shared::SessionId;

    #[test]
    fn incoming_media_bitrate_reports_recent_bits() {
        let now = Instant::now();
        let bitrate = IncomingMediaBitrate::new(now);

        assert!(bitrate.record(now, 120));

        assert_eq!(bitrate.snapshot(now), 960);
    }

    #[test]
    fn incoming_media_bitrate_expires_after_the_window() {
        let now = Instant::now();
        let bitrate = IncomingMediaBitrate::new(now);

        assert!(bitrate.record(now, 64));

        assert_eq!(
            bitrate.snapshot(now + Duration::from_secs(1) + Duration::from_millis(1)),
            0
        );
    }

    #[test]
    fn incoming_media_bitrate_first_observation_fires_once() {
        let now = Instant::now();
        let bitrate = IncomingMediaBitrate::new(now);

        assert!(bitrate.record(now, 1));
        assert!(!bitrate.record(now, 1));
    }

    #[test]
    fn removing_session_hides_registered_counters_from_snapshots() {
        let mut state = RtcBitrateState::default();
        let now = Instant::now();
        let session_key = test_transport_session_key(1, 0, 2, SessionId::Integer(3));
        let media_id = TransportMediaId::new(4);
        let counter = state.register_incoming_media(&session_key, media_id, now);
        counter.record(now, 16);

        state.remove_session(&session_key);

        let snapshot = state.transport_bitrate_snapshot_at(&[session_key], now);
        assert_eq!(snapshot, TransportBitrateSnapshot::default());
    }

    #[test]
    fn packet_loop_counter_write_does_not_need_the_snapshot_lock() {
        let mut shared_state = RtcBitrateState::default();
        let mut bootstrap_state = RtcBootstrapState::default();
        let now = Instant::now();
        let session_key = test_transport_session_key(1, 0, 2, SessionId::Integer(3));
        let media_id = TransportMediaId::new(4);
        let counter = shared_state.register_incoming_media(&session_key, media_id, now);
        bootstrap_state.register_incoming_bitrate_counter(media_id, counter);
        let shared_state = Mutex::new(shared_state);
        let Ok(_snapshot_guard) = shared_state.lock() else {
            return;
        };

        assert_eq!(
            bootstrap_state.record_incoming_bitrate(media_id, now, 32),
            Some(true)
        );
    }
}
