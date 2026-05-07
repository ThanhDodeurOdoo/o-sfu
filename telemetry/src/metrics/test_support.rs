use super::{MetricName, RuntimeMetricsSnapshot};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DurationHistogramSnapshot {
    pub le_10_millis: u64,
    pub le_50_millis: u64,
    pub le_100_millis: u64,
    pub le_250_millis: u64,
    pub count: u64,
    pub sum_micros: u64,
}

pub trait RuntimeMetricsSnapshotLookup {
    fn counter_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64;
    fn gauge_value(&self, name: MetricName, labels: &[(&str, &str)]) -> i64;
    fn duration_snapshot(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
    ) -> DurationHistogramSnapshot;
    fn histogram_bucket_value(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
        upper_bound: &str,
    ) -> u64;
    fn histogram_count_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64;
    fn histogram_sum_micros_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64;
}

impl RuntimeMetricsSnapshotLookup for RuntimeMetricsSnapshot {
    fn counter_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64 {
        self.counter(name, labels).unwrap_or(0)
    }

    fn gauge_value(&self, name: MetricName, labels: &[(&str, &str)]) -> i64 {
        self.gauge(name, labels).unwrap_or(0)
    }

    fn duration_snapshot(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
    ) -> DurationHistogramSnapshot {
        let Some(histogram) = self.histogram(name, labels) else {
            return DurationHistogramSnapshot::default();
        };
        DurationHistogramSnapshot {
            le_10_millis: histogram.bucket("0.01"),
            le_50_millis: histogram.bucket("0.05"),
            le_100_millis: histogram.bucket("0.1"),
            le_250_millis: histogram.bucket("0.25"),
            count: histogram.count,
            sum_micros: histogram.sum_micros,
        }
    }

    fn histogram_bucket_value(
        &self,
        name: MetricName,
        labels: &[(&str, &str)],
        upper_bound: &str,
    ) -> u64 {
        self.histogram(name, labels)
            .map_or(0, |histogram| histogram.bucket(upper_bound))
    }

    fn histogram_count_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64 {
        self.histogram(name, labels)
            .map_or(0, |histogram| histogram.count)
    }

    fn histogram_sum_micros_value(&self, name: MetricName, labels: &[(&str, &str)]) -> u64 {
        self.histogram(name, labels)
            .map_or(0, |histogram| histogram.sum_micros)
    }
}
