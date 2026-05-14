use std::sync::{Arc, Mutex};

use super::{
    counter::{MetricLabel, PaddedCounter, PaddedCounterFamily},
    labels::{RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome},
};

const RTC_DATAGRAM_ROUTE_PATH_COUNT: usize = <RtcDatagramRoutePath as MetricLabel>::COUNT;
const RTC_DATAGRAM_DROP_REASON_COUNT: usize = <RtcDatagramDropReason as MetricLabel>::COUNT;
const RTC_ROUTE_CONTROL_OUTCOME_COUNT: usize = <RtcRouteControlOutcome as MetricLabel>::COUNT;

pub trait RtcRouteControlMetrics {
    fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome);
}

/// Worker-owned RTC packet-loop metric recorder.
///
/// Packet loops keep one recorder for their full shard lifetime. Datagram and
/// route-control updates touch only this worker's padded atomics while
/// `RuntimeMetrics` aggregates all registered recorders during snapshot export.
#[derive(Debug, Default)]
pub struct RtcMetricsRecorder {
    datagram_routes: PaddedCounterFamily<RtcDatagramRoutePath>,
    datagram_drops: PaddedCounterFamily<RtcDatagramDropReason>,
    datagram_fallback_scans: PaddedCounter,
    datagram_scan_users: PaddedCounter,
    route_control: PaddedCounterFamily<RtcRouteControlOutcome>,
}

impl RtcMetricsRecorder {
    pub fn record_rtc_datagram_route(&self, path: RtcDatagramRoutePath) {
        self.datagram_routes.increment(path);
    }

    pub fn record_rtc_datagram_drop(&self, reason: RtcDatagramDropReason) {
        self.datagram_drops.increment(reason);
    }

    pub fn record_rtc_datagram_fallback_scan(&self, examined_sessions: usize) {
        self.datagram_fallback_scans.increment();
        self.datagram_scan_users.add(examined_sessions);
    }

    pub fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.route_control.increment(outcome);
    }
}

impl RtcRouteControlMetrics for RtcMetricsRecorder {
    fn record_rtc_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.record_rtc_route_control(outcome);
    }
}

#[derive(Debug, Default)]
pub(super) struct RtcMetrics {
    process_recorder: RtcMetricsRecorder,
    worker_recorders: Mutex<Vec<Arc<RtcMetricsRecorder>>>,
}

impl RtcMetrics {
    pub(super) fn register_worker(&self) -> Arc<RtcMetricsRecorder> {
        let recorder = Arc::new(RtcMetricsRecorder::default());
        {
            let mut workers = match self.worker_recorders.lock() {
                Ok(workers) => workers,
                Err(poisoned) => poisoned.into_inner(),
            };
            workers.push(Arc::clone(&recorder));
        }
        recorder
    }

    pub(super) fn record_datagram_route(&self, path: RtcDatagramRoutePath) {
        self.process_recorder.record_rtc_datagram_route(path);
    }

    pub(super) fn record_datagram_drop(&self, reason: RtcDatagramDropReason) {
        self.process_recorder.record_rtc_datagram_drop(reason);
    }

    pub(super) fn record_datagram_fallback_scan(&self, examined_sessions: usize) {
        self.process_recorder
            .record_rtc_datagram_fallback_scan(examined_sessions);
    }

    pub(super) fn record_route_control(&self, outcome: RtcRouteControlOutcome) {
        self.process_recorder.record_rtc_route_control(outcome);
    }

    pub(super) fn snapshot(&self) -> RtcMetricsSnapshot {
        let mut snapshot = RtcMetricsSnapshot::default();
        snapshot.add_recorder(&self.process_recorder);
        {
            let workers = match self.worker_recorders.lock() {
                Ok(workers) => workers,
                Err(poisoned) => poisoned.into_inner(),
            };
            for recorder in workers.iter() {
                snapshot.add_recorder(recorder);
            }
        }
        snapshot
    }
}

#[derive(Debug, Default)]
pub(super) struct RtcMetricsSnapshot {
    datagram_routes: [u64; RTC_DATAGRAM_ROUTE_PATH_COUNT],
    datagram_drops: [u64; RTC_DATAGRAM_DROP_REASON_COUNT],
    datagram_fallback_scans: u64,
    datagram_scan_users: u64,
    route_control: [u64; RTC_ROUTE_CONTROL_OUTCOME_COUNT],
}

impl RtcMetricsSnapshot {
    pub(super) fn datagram_routes(&self, path: RtcDatagramRoutePath) -> u64 {
        self.datagram_routes
            .get(path.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn datagram_drops(&self, reason: RtcDatagramDropReason) -> u64 {
        self.datagram_drops
            .get(reason.as_index())
            .copied()
            .unwrap_or(0)
    }

    pub(super) const fn datagram_fallback_scans(&self) -> u64 {
        self.datagram_fallback_scans
    }

    pub(super) const fn datagram_scan_users(&self) -> u64 {
        self.datagram_scan_users
    }

    pub(super) fn route_control(&self, outcome: RtcRouteControlOutcome) -> u64 {
        self.route_control
            .get(outcome.as_index())
            .copied()
            .unwrap_or(0)
    }

    fn add_recorder(&mut self, recorder: &RtcMetricsRecorder) {
        for path in <RtcDatagramRoutePath as MetricLabel>::VARIANTS {
            self.add_datagram_route(*path, recorder.datagram_routes.load(*path));
        }
        for reason in <RtcDatagramDropReason as MetricLabel>::VARIANTS {
            self.add_datagram_drop(*reason, recorder.datagram_drops.load(*reason));
        }
        self.datagram_fallback_scans = self
            .datagram_fallback_scans
            .saturating_add(recorder.datagram_fallback_scans.load());
        self.datagram_scan_users = self
            .datagram_scan_users
            .saturating_add(recorder.datagram_scan_users.load());
        for outcome in <RtcRouteControlOutcome as MetricLabel>::VARIANTS {
            self.add_route_control(*outcome, recorder.route_control.load(*outcome));
        }
    }

    fn add_datagram_route(&mut self, path: RtcDatagramRoutePath, count: u64) {
        if let Some(counter) = self.datagram_routes.get_mut(path.as_index()) {
            *counter = counter.saturating_add(count);
        }
    }

    fn add_datagram_drop(&mut self, reason: RtcDatagramDropReason, count: u64) {
        if let Some(counter) = self.datagram_drops.get_mut(reason.as_index()) {
            *counter = counter.saturating_add(count);
        }
    }

    fn add_route_control(&mut self, outcome: RtcRouteControlOutcome, count: u64) {
        if let Some(counter) = self.route_control.get_mut(outcome.as_index()) {
            *counter = counter.saturating_add(count);
        }
    }
}
