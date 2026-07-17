//! Prometheus text rendering for runtime metrics
//!
//! the renderer reads a `RuntimeMetrics` snapshot and emits the Prometheus
//! text exposition format usedby the server `/metrics` route
//!
//! ```
//! use o_sfu_telemetry::{
//!     metrics::{HttpRoute, RuntimeMetrics},
//!     prometheus::render_prometheus,
//! };
//!
//! let metrics = RuntimeMetrics::default();
//! let request = metrics.track_http_request(HttpRoute::Noop);
//! let rendered = render_prometheus(&metrics);
//! drop(request);
//!
//! let expected_lines = [
//!     "# TYPE osfu_http_noop_requests_total counter",
//!     "osfu_http_noop_requests_total 1",
//!     "# TYPE osfu_http_inflight_requests gauge",
//!     "osfu_http_inflight_requests{route=\"noop\"} 1",
//!     "# TYPE osfu_http_request_duration_seconds histogram",
//!     "osfu_http_request_duration_seconds_count{route=\"noop\"} 0",
//! ];
//!
//! for line in expected_lines {
//!     assert!(rendered.contains(line), "missing Prometheus line: {line}");
//! }
//! ```

use crate::metrics::{
    MetricFamilySnapshot, MetricKind, MetricLabel, MetricValue, RuntimeMetrics,
    RuntimeMetricsSnapshot,
};

pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub fn render_prometheus(metrics: &RuntimeMetrics) -> String {
    render_snapshot(&metrics.snapshot())
}

fn render_snapshot(snapshot: &RuntimeMetricsSnapshot) -> String {
    let mut output = String::with_capacity(5120);
    for family in snapshot.families() {
        append_family(&mut output, family);
    }
    output
}

fn append_family(output: &mut String, family: &MetricFamilySnapshot) {
    output.push_str("# HELP ");
    output.push_str(family.name);
    output.push(' ');
    output.push_str(family.help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(family.name);
    output.push(' ');
    output.push_str(match family.kind {
        MetricKind::Counter => "counter",
        MetricKind::Gauge => "gauge",
        MetricKind::Histogram => "histogram",
    });
    output.push('\n');

    match family.kind {
        MetricKind::Counter => append_counter_samples(output, family),
        MetricKind::Gauge => append_gauge_samples(output, family),
        MetricKind::Histogram => append_histogram_samples(output, family),
    }
}

fn append_counter_samples(output: &mut String, family: &MetricFamilySnapshot) {
    for sample in family.samples() {
        let MetricValue::Counter(value) = sample.value else {
            continue;
        };
        append_sample_name(output, family.name, &sample.labels);
        output.push(' ');
        append_u64(output, value);
        output.push('\n');
    }
}

fn append_gauge_samples(output: &mut String, family: &MetricFamilySnapshot) {
    for sample in family.samples() {
        let MetricValue::Gauge(value) = sample.value else {
            continue;
        };
        append_sample_name(output, family.name, &sample.labels);
        output.push(' ');
        output.push_str(&value.to_string());
        output.push('\n');
    }
}

fn append_histogram_samples(output: &mut String, family: &MetricFamilySnapshot) {
    for sample in family.samples() {
        let MetricValue::Histogram(ref histogram) = sample.value else {
            continue;
        };
        for bucket in &histogram.buckets {
            output.push_str(family.name);
            output.push_str("_bucket");
            append_labels(output, &sample.labels, Some(("le", bucket.upper_bound)));
            output.push(' ');
            append_u64(output, bucket.value);
            output.push('\n');
        }
        output.push_str(family.name);
        output.push_str("_bucket");
        append_labels(output, &sample.labels, Some(("le", "+Inf")));
        output.push(' ');
        append_u64(output, histogram.count);
        output.push('\n');

        output.push_str(family.name);
        output.push_str("_sum");
        append_labels(output, &sample.labels, None);
        output.push(' ');
        append_seconds_from_micros(output, histogram.sum_micros);
        output.push('\n');

        output.push_str(family.name);
        output.push_str("_count");
        append_labels(output, &sample.labels, None);
        output.push(' ');
        append_u64(output, histogram.count);
        output.push('\n');
    }
}

fn append_sample_name(output: &mut String, name: &str, labels: &[MetricLabel]) {
    output.push_str(name);
    append_labels(output, labels, None);
}

fn append_labels(output: &mut String, labels: &[MetricLabel], extra_label: Option<(&str, &str)>) {
    if labels.is_empty() && extra_label.is_none() {
        return;
    }
    output.push('{');
    let mut needs_separator = false;
    for label in labels {
        if needs_separator {
            output.push(',');
        }
        output.push_str(label.name);
        output.push_str("=\"");
        output.push_str(&label.value);
        output.push('"');
        needs_separator = true;
    }
    if let Some((name, value)) = extra_label {
        if needs_separator {
            output.push(',');
        }
        output.push_str(name);
        output.push_str("=\"");
        output.push_str(value);
        output.push('"');
    }
    output.push('}');
}

fn append_u64(output: &mut String, value: u64) {
    output.push_str(&value.to_string());
}

fn append_seconds_from_micros(output: &mut String, micros: u64) {
    let whole_seconds = micros / 1_000_000;
    let fractional_micros = micros % 1_000_000;
    output.push_str(&whole_seconds.to_string());
    if fractional_micros == 0 {
        output.push_str(".0");
        return;
    }
    output.push('.');
    let mut fractional = format!("{fractional_micros:06}");
    while fractional.ends_with('0') {
        fractional.pop();
    }
    output.push_str(&fractional);
}

#[cfg(test)]
#[path = "TESTS/mod.rs"]
mod tests;
