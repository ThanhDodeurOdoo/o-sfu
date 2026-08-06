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
    engine::media_transport::{TransportBitrateSnapshot, TransportMediaId, TransportSessionKey},
};

const BITRATE_WINDOW_NANOS: u64 = 1_000_000_000;

#[derive(Debug)]
pub(super) struct MediaBitrateCounter {
    origin: Instant,
    window_start_nanos: AtomicU64,
    last_observed_nanos: AtomicU64,
    bytes_in_window: AtomicU64,
    completed_bps: AtomicU64,
    observed: AtomicBool,
}

/// Packet-loop follow-up produced by one incoming bitrate observation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum IncomingBitrateObservation {
    #[default]
    /// No ingress edge or completed sample needs a policy wake.
    Unchanged,
    /// First packet after registration or one full idle window.
    IngressStarted,
    /// A completed bitrate window is available to policy readers.
    SampleUpdated,
}

impl IncomingBitrateObservation {
    pub(super) const fn policy_dirty(self) -> bool {
        !matches!(self, Self::Unchanged)
    }

    pub(super) const fn ingress_started(self) -> bool {
        matches!(self, Self::IngressStarted)
    }
}

impl MediaBitrateCounter {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            origin: now,
            window_start_nanos: AtomicU64::new(0),
            last_observed_nanos: AtomicU64::new(0),
            bytes_in_window: AtomicU64::new(0),
            completed_bps: AtomicU64::new(0),
            observed: AtomicBool::new(false),
        }
    }

    /// Records one packet-loop-owned byte count into the current bitrate window.
    ///
    /// `MediaBitrateCounter` is shared so diagnostics can snapshot it from
    /// another thread, but `record` has one writer: the RTC packet loop that
    /// owns the registered media handle. That makes a release `fetch_add`
    /// enough for per-packet writes. Exact saturating addition is
    /// not part of the observable contract because one RTP bitrate window is
    /// bounded by real transport packet sizes and cannot approach `u64::MAX`.
    pub(super) fn record(&self, now: Instant, payload_bytes: usize) -> IncomingBitrateObservation {
        let now_nanos = self.nanos_since_origin(now);
        let was_observed = self.observed.load(Ordering::Acquire);
        let previous_observed = self.last_observed_nanos.load(Ordering::Acquire);
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let observation = if was_observed {
            let ingress_started =
                now_nanos.saturating_sub(previous_observed) >= BITRATE_WINDOW_NANOS;
            if ingress_started {
                self.window_start_nanos.store(now_nanos, Ordering::Release);
                self.bytes_in_window.store(payload_bytes, Ordering::Release);
                self.completed_bps.store(0, Ordering::Release);
                IncomingBitrateObservation::IngressStarted
            } else {
                let window_start = self.window_start_nanos.load(Ordering::Acquire);
                let elapsed_nanos = now_nanos.saturating_sub(window_start);
                if elapsed_nanos >= BITRATE_WINDOW_NANOS {
                    let completed_bytes =
                        self.bytes_in_window.swap(payload_bytes, Ordering::AcqRel);
                    let completed_bps = bitrate_per_second(completed_bytes, elapsed_nanos);
                    self.completed_bps
                        .store(completed_bps.as_bps(), Ordering::Release);
                    self.window_start_nanos.store(now_nanos, Ordering::Release);
                    IncomingBitrateObservation::SampleUpdated
                } else {
                    self.bytes_in_window
                        .fetch_add(payload_bytes, Ordering::Release);
                    IncomingBitrateObservation::Unchanged
                }
            }
        } else {
            self.window_start_nanos.store(now_nanos, Ordering::Release);
            self.bytes_in_window.store(payload_bytes, Ordering::Release);
            IncomingBitrateObservation::IngressStarted
        };
        self.last_observed_nanos.store(now_nanos, Ordering::Release);
        if !was_observed {
            self.observed.store(true, Ordering::Release);
        }
        observation
    }

    pub(super) fn last_observed_age(&self, now: Instant) -> Option<Duration> {
        if !self.observed.load(Ordering::Acquire) {
            return None;
        }
        Some(Duration::from_nanos(
            self.nanos_since_origin(now)
                .saturating_sub(self.last_observed_nanos.load(Ordering::Acquire)),
        ))
    }

    fn snapshot(&self, now: Instant) -> Bitrate {
        let now_nanos = self.nanos_since_origin(now);
        if !self.observed.load(Ordering::Acquire)
            || now_nanos.saturating_sub(self.last_observed_nanos.load(Ordering::Acquire))
                >= BITRATE_WINDOW_NANOS
        {
            return Bitrate::zero();
        }
        Bitrate::from_bps(self.completed_bps.load(Ordering::Acquire))
    }

    fn nanos_since_origin(&self, now: Instant) -> u64 {
        let elapsed = now
            .checked_duration_since(self.origin)
            .unwrap_or(Duration::ZERO);
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
    }
}

fn bitrate_per_second(bytes: u64, elapsed_nanos: u64) -> Bitrate {
    let bits_per_second = u128::from(bytes)
        .saturating_mul(8)
        .saturating_mul(u128::from(BITRATE_WINDOW_NANOS))
        / u128::from(elapsed_nanos.max(1));
    Bitrate::from_bps(u64::try_from(bits_per_second).unwrap_or(u64::MAX))
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

    fn extend_snapshot(&self, now: Instant, snapshot: &mut TransportBitrateSnapshot) {
        for (&media_id, counter) in &self.per_media {
            let bitrate = counter.snapshot(now);
            snapshot.total = snapshot.total.saturating_add(bitrate);
            if bitrate > Bitrate::zero() {
                snapshot.per_media.push((media_id, bitrate));
            }
        }
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
        counter: Arc<MediaBitrateCounter>,
    ) {
        self.egress_bitrates_by_session
            .insert(session_key.clone(), counter);
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
            session_bitrates.extend_snapshot(now, &mut snapshot);
        }
        snapshot
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
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

    pub(super) fn remove_incoming_bitrate_counter(&mut self, transport_media_id: TransportMediaId) {
        self.incoming_bitrate_counters.remove(&transport_media_id);
    }

    pub(super) fn record_incoming_bitrate(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
        payload_bytes: usize,
    ) -> Option<IncomingBitrateObservation> {
        self.incoming_bitrate_counters
            .get(&transport_media_id)
            .map(|bitrate| bitrate.record(now, payload_bytes))
    }
}

#[cfg(test)]
#[path = "TESTS/bitrate.rs"]
mod tests;
