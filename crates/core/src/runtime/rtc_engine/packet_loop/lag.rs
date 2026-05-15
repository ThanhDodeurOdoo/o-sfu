use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const NO_PACKET_LOOP_LAG_SAMPLE: u64 = u64::MAX;
const PACKET_LOOP_LAG_SAMPLE_TTL: Duration = Duration::from_secs(1);
const PACKET_LOOP_LAG_PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(in crate::runtime::rtc_engine) struct PacketLoopLagSnapshot {
    started_at: Instant,
    lag_ms: AtomicU64,
    observed_elapsed_ms: AtomicU64,
}

impl PacketLoopLagSnapshot {
    pub fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            lag_ms: AtomicU64::new(0),
            observed_elapsed_ms: AtomicU64::new(NO_PACKET_LOOP_LAG_SAMPLE),
        }
    }

    fn publish(&self, lag_ms: u64, observed_at: Instant) {
        self.lag_ms.store(lag_ms, Ordering::Relaxed);
        self.observed_elapsed_ms
            .store(self.elapsed_ms(observed_at), Ordering::Release);
    }

    #[cfg(test)]
    pub fn publish_for_test(&self, lag_ms: u64, observed_at: Instant) {
        self.publish(lag_ms, observed_at);
    }

    pub fn packet_loop_lag_ms_at(&self, now: Instant) -> u64 {
        let observed_elapsed_ms = self.observed_elapsed_ms.load(Ordering::Acquire);
        if observed_elapsed_ms == NO_PACKET_LOOP_LAG_SAMPLE {
            return 0;
        }
        let now_elapsed_ms = self.elapsed_ms(now);
        if now_elapsed_ms.saturating_sub(observed_elapsed_ms)
            <= millis_u64(PACKET_LOOP_LAG_SAMPLE_TTL)
        {
            self.lag_ms.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn elapsed_ms(&self, instant: Instant) -> u64 {
        millis_u64(instant.saturating_duration_since(self.started_at))
    }
}

pub(super) struct PacketLoopLagPublisher {
    pending_max_lag_ms: u64,
    last_published_at: Instant,
}

impl PacketLoopLagPublisher {
    pub(super) fn new(started_at: Instant) -> Self {
        Self {
            pending_max_lag_ms: 0,
            last_published_at: started_at,
        }
    }

    pub(super) fn observe(
        &mut self,
        snapshot: &PacketLoopLagSnapshot,
        turn_started_at: Instant,
        observed_at: Instant,
    ) {
        self.pending_max_lag_ms = self.pending_max_lag_ms.max(millis_u64(
            observed_at.saturating_duration_since(turn_started_at),
        ));
        if observed_at.saturating_duration_since(self.last_published_at)
            < PACKET_LOOP_LAG_PUBLISH_INTERVAL
        {
            return;
        }
        snapshot.publish(self.pending_max_lag_ms, observed_at);
        self.pending_max_lag_ms = 0;
        self.last_published_at = observed_at;
    }
}

fn millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).map_or(u64::MAX, |value| value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_publisher_publishes_maximum_observed_lag_on_interval() {
        let started_at = Instant::now();
        let snapshot = PacketLoopLagSnapshot::new(started_at);
        let mut publisher = PacketLoopLagPublisher::new(started_at);

        publisher.observe(
            &snapshot,
            started_at + Duration::from_millis(5),
            started_at + Duration::from_millis(15),
        );
        publisher.observe(
            &snapshot,
            started_at + Duration::from_millis(20),
            started_at + Duration::from_millis(40),
        );

        assert_eq!(
            snapshot.packet_loop_lag_ms_at(started_at + Duration::from_millis(40)),
            0
        );

        publisher.observe(
            &snapshot,
            started_at + Duration::from_millis(105),
            started_at + Duration::from_millis(110),
        );

        assert_eq!(
            snapshot.packet_loop_lag_ms_at(started_at + Duration::from_millis(110)),
            20
        );
    }

    #[test]
    fn lag_snapshot_expires_stale_samples() {
        let started_at = Instant::now();
        let snapshot = PacketLoopLagSnapshot::new(started_at);
        let mut publisher = PacketLoopLagPublisher::new(started_at);

        publisher.observe(
            &snapshot,
            started_at + Duration::from_millis(99),
            started_at + Duration::from_millis(100),
        );

        assert_eq!(
            snapshot.packet_loop_lag_ms_at(started_at + Duration::from_millis(1101)),
            0
        );
    }
}
