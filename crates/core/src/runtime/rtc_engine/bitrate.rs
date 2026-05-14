//! Worker-local bitrate accounting for RTC media.
//!
//! The packet loop owns the write side and updates counters through atomics. The
//! shared state lock protects only cold registration and snapshot maps, so
//! operator polling cannot contend with per-packet writes.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use super::state::PacketLoopState;
use crate::{
    Bitrate,
    runtime::media_transport::{TransportBitrateSnapshot, TransportMediaId, TransportSessionKey},
};

const BITRATE_WINDOW_NANOS: u64 = 1_000_000_000;

#[derive(Debug)]
pub(super) struct MediaBitrateCounter {
    origin: Instant,
    window_start_nanos: AtomicU64,
    bytes_in_window: AtomicU64,
    observed: AtomicBool,
}

impl MediaBitrateCounter {
    fn new(now: Instant) -> Self {
        Self {
            origin: now,
            window_start_nanos: AtomicU64::new(0),
            bytes_in_window: AtomicU64::new(0),
            observed: AtomicBool::new(false),
        }
    }

    /// Records one packet-loop-owned byte count into the current bitrate window.
    ///
    /// `MediaBitrateCounter` is shared so diagnostics can snapshot it from
    /// another thread, but `record` has one writer: the RTC packet loop that
    /// owns the registered media handle. That makes a release `fetch_add`
    /// enough for per-packet writes. Exact saturating addition is intentionally
    /// not part of the observable contract because one RTP bitrate window is
    /// bounded by real transport packet sizes and cannot approach `u64::MAX`.
    pub(super) fn record(&self, now: Instant, payload_bytes: usize) -> bool {
        let now_nanos = self.nanos_since_origin(now);
        let window_start = self.window_start_nanos.load(Ordering::Acquire);
        if now_nanos.saturating_sub(window_start) >= BITRATE_WINDOW_NANOS {
            self.bytes_in_window.store(0, Ordering::Release);
            self.window_start_nanos.store(now_nanos, Ordering::Release);
        }
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        self.bytes_in_window
            .fetch_add(payload_bytes, Ordering::Release);
        !self.observed.swap(true, Ordering::AcqRel)
    }

    fn snapshot(&self, now: Instant) -> Bitrate {
        let now_nanos = self.nanos_since_origin(now);
        let window_start = self.window_start_nanos.load(Ordering::Acquire);
        if now_nanos.saturating_sub(window_start) >= BITRATE_WINDOW_NANOS {
            return Bitrate::zero();
        }
        Bitrate::from_bps(
            self.bytes_in_window
                .load(Ordering::Acquire)
                .saturating_mul(8),
        )
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
    per_media: BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
}

impl SessionIncomingBitrates {
    fn register(
        &mut self,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Arc<MediaBitrateCounter> {
        Arc::clone(
            self.per_media
                .entry(transport_media_id)
                .or_insert_with(|| Arc::new(MediaBitrateCounter::new(now))),
        )
    }

    fn remove(&mut self, transport_media_id: TransportMediaId) {
        self.per_media.remove(&transport_media_id);
    }

    fn is_empty(&self) -> bool {
        self.per_media.is_empty()
    }

    fn snapshot(&self, now: Instant) -> Vec<(TransportMediaId, Bitrate)> {
        self.per_media
            .iter()
            .filter_map(|(media_id, bitrate)| {
                let bits = bitrate.snapshot(now);
                if bits > Bitrate::zero() {
                    Some((*media_id, bits))
                } else {
                    None
                }
            })
            .collect()
    }

    fn total(&self, now: Instant) -> Bitrate {
        self.per_media
            .values()
            .map(|bitrate| bitrate.snapshot(now))
            .fold(Bitrate::zero(), Bitrate::saturating_add)
    }
}

#[derive(Debug, Default)]
pub struct BitrateRegistry {
    pub(super) incoming_bitrates_by_session: BTreeMap<TransportSessionKey, SessionIncomingBitrates>,
    pub(super) egress_bitrates_by_session: BTreeMap<TransportSessionKey, Arc<MediaBitrateCounter>>,
}

impl BitrateRegistry {
    pub(super) fn register_incoming_media(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Arc<MediaBitrateCounter> {
        self.incoming_bitrates_by_session
            .entry(session_key.clone())
            .or_default()
            .register(transport_media_id, now)
    }

    pub(super) fn register_session_egress(
        &mut self,
        session_key: &TransportSessionKey,
        now: Instant,
    ) -> Arc<MediaBitrateCounter> {
        Arc::clone(
            self.egress_bitrates_by_session
                .entry(session_key.clone())
                .or_insert_with(|| Arc::new(MediaBitrateCounter::new(now))),
        )
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
        self.egress_bitrates_by_session.remove(session_key);
    }

    pub fn transport_bitrate_snapshot_at(
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

    pub fn egress_bitrate_snapshot_at(
        &self,
        session_keys: &[TransportSessionKey],
        now: Instant,
    ) -> Bitrate {
        session_keys
            .iter()
            .filter_map(|session_key| self.egress_bitrates_by_session.get(session_key))
            .fold(Bitrate::zero(), |total, bitrate| {
                total.saturating_add(bitrate.snapshot(now))
            })
    }

    pub fn total_egress_bitrate_snapshot_at(&self, now: Instant) -> Bitrate {
        self.egress_bitrates_by_session
            .values()
            .fold(Bitrate::zero(), |total, bitrate| {
                total.saturating_add(bitrate.snapshot(now))
            })
    }
}

impl PacketLoopState {
    pub(super) fn register_incoming_bitrate_counter(
        &mut self,
        transport_media_id: TransportMediaId,
        counter: Arc<MediaBitrateCounter>,
    ) {
        self.incoming_bitrate_counters
            .insert(transport_media_id, counter);
    }

    pub(super) fn register_egress_bitrate_counter(
        &mut self,
        session_key: TransportSessionKey,
        counter: Arc<MediaBitrateCounter>,
    ) {
        self.egress_bitrate_counters.insert(session_key, counter);
    }

    pub(super) fn remove_egress_bitrate_counter(&mut self, session_key: &TransportSessionKey) {
        self.egress_bitrate_counters.remove(session_key);
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

    pub(super) fn record_egress_bitrate(
        &self,
        session_key: &TransportSessionKey,
        now: Instant,
        payload_bytes: usize,
    ) -> Option<bool> {
        self.egress_bitrate_counters
            .get(session_key)
            .map(|bitrate| bitrate.record(now, payload_bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering as AtomicOrdering},
        },
        thread,
    };

    use super::*;
    use crate::runtime::{UserId, rtc_engine::test_support::test_transport_session_key};

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
        let counter = state.register_session_egress(&session_key, now);

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
        counter.record(now, 16);

        state.remove_session(&session_key);

        let snapshot = state.transport_bitrate_snapshot_at(&[session_key], now);
        assert_eq!(snapshot, TransportBitrateSnapshot::default());
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
}
