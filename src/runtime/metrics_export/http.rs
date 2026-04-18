use crate::runtime::metrics::RuntimeMetricsSnapshot;

use super::shared::{LabeledValue, append_counter, append_labeled_counter_family};

pub(super) fn append_http_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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
