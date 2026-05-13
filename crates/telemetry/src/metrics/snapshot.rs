use super::{catalog::RuntimeMetrics, descriptor::build_snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricLabel {
    pub name: &'static str,
    pub value: &'static str,
}

impl MetricLabel {
    #[must_use]
    pub const fn new(name: &'static str, value: &'static str) -> Self {
        Self { name, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricHistogramBucketSnapshot {
    pub upper_bound: &'static str,
    pub value: u64,
}

impl MetricHistogramBucketSnapshot {
    #[must_use]
    pub const fn new(upper_bound: &'static str, value: u64) -> Self {
        Self { upper_bound, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricHistogramSnapshot {
    pub buckets: Box<[MetricHistogramBucketSnapshot]>,
    pub count: u64,
    pub sum_micros: u64,
}

impl MetricHistogramSnapshot {
    #[must_use]
    pub fn new(buckets: Box<[MetricHistogramBucketSnapshot]>, count: u64, sum_micros: u64) -> Self {
        Self {
            buckets,
            count,
            sum_micros,
        }
    }

    #[must_use]
    pub fn bucket(&self, upper_bound: &str) -> u64 {
        self.buckets
            .iter()
            .find(|bucket| bucket.upper_bound == upper_bound)
            .map_or(0, |bucket| bucket.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Histogram(MetricHistogramSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricSample {
    pub labels: Box<[MetricLabel]>,
    pub value: MetricValue,
}

impl MetricSample {
    #[must_use]
    pub fn counter(labels: Box<[MetricLabel]>, value: u64) -> Self {
        Self {
            labels,
            value: MetricValue::Counter(value),
        }
    }

    #[must_use]
    pub fn gauge(labels: Box<[MetricLabel]>, value: i64) -> Self {
        Self {
            labels,
            value: MetricValue::Gauge(value),
        }
    }

    #[must_use]
    pub fn histogram(labels: Box<[MetricLabel]>, value: MetricHistogramSnapshot) -> Self {
        Self {
            labels,
            value: MetricValue::Histogram(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricFamilySnapshot {
    pub id: super::descriptor::MetricName,
    pub name: &'static str,
    pub help: &'static str,
    pub kind: MetricKind,
    samples: Box<[MetricSample]>,
}

impl MetricFamilySnapshot {
    pub(super) fn new(
        id: super::descriptor::MetricName,
        name: &'static str,
        help: &'static str,
        kind: MetricKind,
        samples: Box<[MetricSample]>,
    ) -> Self {
        Self {
            id,
            name,
            help,
            kind,
            samples,
        }
    }

    #[must_use]
    pub fn samples(&self) -> &[MetricSample] {
        &self.samples
    }

    fn sample(&self, labels: &[(&str, &str)]) -> Option<&MetricSample> {
        self.samples
            .iter()
            .find(|sample| labels_match(&sample.labels, labels))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMetricsSnapshot {
    families: Box<[MetricFamilySnapshot]>,
}

impl RuntimeMetricsSnapshot {
    pub(super) fn new(families: Box<[MetricFamilySnapshot]>) -> Self {
        Self { families }
    }

    #[must_use]
    pub fn families(&self) -> &[MetricFamilySnapshot] {
        &self.families
    }

    #[must_use]
    pub fn family(&self, name: super::descriptor::MetricName) -> Option<&MetricFamilySnapshot> {
        self.families.iter().find(|family| family.id == name)
    }

    #[must_use]
    pub fn counter(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> Option<u64> {
        let MetricValue::Counter(value) = &self.family(name)?.sample(labels)?.value else {
            return None;
        };
        Some(*value)
    }

    #[must_use]
    pub fn gauge(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> Option<i64> {
        let MetricValue::Gauge(value) = &self.family(name)?.sample(labels)?.value else {
            return None;
        };
        Some(*value)
    }

    #[must_use]
    pub fn histogram(
        &self,
        name: super::descriptor::MetricName,
        labels: &[(&str, &str)],
    ) -> Option<&MetricHistogramSnapshot> {
        let MetricValue::Histogram(value) = &self.family(name)?.sample(labels)?.value else {
            return None;
        };
        Some(value)
    }
}

impl RuntimeMetrics {
    #[must_use]
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        build_snapshot(self)
    }
}

fn labels_match(sample_labels: &[MetricLabel], labels: &[(&str, &str)]) -> bool {
    sample_labels.len() == labels.len()
        && labels.iter().all(|(name, value)| {
            sample_labels
                .iter()
                .any(|label| label.name == *name && label.value == *value)
        })
}
