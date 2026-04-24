use super::shared::{
    HistogramBucketValue, LabeledGaugeValue, LabeledHistogramValue, LabeledValue, append_counter,
    append_labeled_counter_family, append_labeled_gauge_family, append_labeled_histogram_family,
};
use crate::runtime::metrics::{
    DurationHistogramSnapshot, HttpInflightSnapshot, RuntimeMetricsSnapshot,
};

pub(super) fn append_http_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_http_request_counters(output, snapshot);
    append_http_latency_metrics(output, snapshot);
}

fn append_http_request_counters(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_counter(
        output,
        "osfu_http_noop_requests_total",
        "Total HTTP requests served by /v1/noop.",
        snapshot.http_noop_requests,
    );
    append_counter(
        output,
        "osfu_http_stats_requests_total",
        "Total HTTP requests served by /v1/stats.",
        snapshot.http_stats_requests,
    );
    append_counter(
        output,
        "osfu_http_channel_requests_total",
        "Total HTTP requests received by /v1/channel.",
        snapshot.http_channel_requests,
    );
    append_labeled_counter_family(
        output,
        "osfu_http_channel_responses_total",
        "Total HTTP /v1/channel responses by status.",
        "status",
        &[
            LabeledValue::new("success", snapshot.http_channel_success),
            LabeledValue::new("unauthorized", snapshot.http_channel_unauthorized),
            LabeledValue::new("forbidden", snapshot.http_channel_forbidden),
            LabeledValue::new("bad_request", snapshot.http_channel_bad_request),
        ],
    );
    append_counter(
        output,
        "osfu_http_disconnect_requests_total",
        "Total HTTP requests received by /v1/disconnect.",
        snapshot.http_disconnect_requests,
    );
    append_labeled_counter_family(
        output,
        "osfu_http_disconnect_responses_total",
        "Total HTTP /v1/disconnect responses by status.",
        "status",
        &[
            LabeledValue::new("success", snapshot.http_disconnect_success),
            LabeledValue::new("bad_request", snapshot.http_disconnect_bad_request),
            LabeledValue::new(
                "unprocessable_entity",
                snapshot.http_disconnect_unprocessable_entity,
            ),
        ],
    );
    append_counter(
        output,
        "osfu_http_metrics_requests_total",
        "Total HTTP requests served by /metrics.",
        snapshot.http_metrics_requests,
    );
}

fn append_http_latency_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_gauge_family(
        output,
        "osfu_http_inflight_requests",
        "Current in-flight HTTP requests by route.",
        "route",
        &http_inflight_values(&snapshot.http_inflight),
    );
    let noop_duration = duration_histogram_buckets(&snapshot.http_request_duration.noop);
    let stats_duration = duration_histogram_buckets(&snapshot.http_request_duration.stats);
    let channel_duration = duration_histogram_buckets(&snapshot.http_request_duration.channel);
    let disconnect_duration =
        duration_histogram_buckets(&snapshot.http_request_duration.disconnect);
    let metrics_duration = duration_histogram_buckets(&snapshot.http_request_duration.metrics);
    append_labeled_histogram_family(
        output,
        "osfu_http_request_duration_seconds",
        "HTTP request duration by route.",
        "route",
        &[
            LabeledHistogramValue::new(
                "noop",
                &noop_duration,
                snapshot.http_request_duration.noop.sum_micros,
                snapshot.http_request_duration.noop.count,
            ),
            LabeledHistogramValue::new(
                "stats",
                &stats_duration,
                snapshot.http_request_duration.stats.sum_micros,
                snapshot.http_request_duration.stats.count,
            ),
            LabeledHistogramValue::new(
                "channel",
                &channel_duration,
                snapshot.http_request_duration.channel.sum_micros,
                snapshot.http_request_duration.channel.count,
            ),
            LabeledHistogramValue::new(
                "disconnect",
                &disconnect_duration,
                snapshot.http_request_duration.disconnect.sum_micros,
                snapshot.http_request_duration.disconnect.count,
            ),
            LabeledHistogramValue::new(
                "metrics",
                &metrics_duration,
                snapshot.http_request_duration.metrics.sum_micros,
                snapshot.http_request_duration.metrics.count,
            ),
        ],
    );
}

fn http_inflight_values(snapshot: &HttpInflightSnapshot) -> [LabeledGaugeValue; 5] {
    [
        LabeledGaugeValue::new("noop", snapshot.noop),
        LabeledGaugeValue::new("stats", snapshot.stats),
        LabeledGaugeValue::new("channel", snapshot.channel),
        LabeledGaugeValue::new("disconnect", snapshot.disconnect),
        LabeledGaugeValue::new("metrics", snapshot.metrics),
    ]
}

fn duration_histogram_buckets(snapshot: &DurationHistogramSnapshot) -> [HistogramBucketValue; 7] {
    [
        HistogramBucketValue::new("0.01", snapshot.le_10_millis),
        HistogramBucketValue::new("0.05", snapshot.le_50_millis),
        HistogramBucketValue::new("0.1", snapshot.le_100_millis),
        HistogramBucketValue::new("0.25", snapshot.le_250_millis),
        HistogramBucketValue::new("0.5", snapshot.le_500_millis),
        HistogramBucketValue::new("1", snapshot.le_1_second),
        HistogramBucketValue::new("5", snapshot.le_5_seconds),
    ]
}
