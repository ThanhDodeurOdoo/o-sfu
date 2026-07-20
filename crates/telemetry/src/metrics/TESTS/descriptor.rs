use std::cell::Cell;

use super::{MetricDescriptor, MetricKind, MetricName, MetricOutput};
use crate::metrics::labels::ControlPlaneDurationBucket;

#[test]
fn histogram_output_remains_cumulative() {
    let mut output = MetricOutput::prometheus();
    output.begin_family(MetricDescriptor {
        id: MetricName::HttpRequestDurationSeconds,
        name: "test_histogram",
        help: "test histogram",
        kind: MetricKind::Histogram,
    });
    let first = Cell::new(true);
    output.histogram::<ControlPlaneDurationBucket>(
        &[],
        |_| u64::from(first.replace(false)),
        || 0,
        || 0,
    );

    let rendered = output.finish_prometheus();
    for expected in [
        "test_histogram_bucket{le=\"0.01\"} 1",
        "test_histogram_bucket{le=\"0.05\"} 1",
        "test_histogram_bucket{le=\"+Inf\"} 1",
        "test_histogram_count 1",
    ] {
        assert!(rendered.lines().any(|line| line == expected));
    }
}
