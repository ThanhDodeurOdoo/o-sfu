use super::shared::{LabeledValue, append_counter, append_labeled_counter_family};
use crate::runtime::metrics::RuntimeMetricsSnapshot;

pub(super) fn append_rtc_datagram_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family(
        output,
        "osfu_rtc_datagram_routes_total",
        "Total RTC UDP datagrams accepted by routing path.",
        "path",
        &[
            LabeledValue::new("indexed", snapshot.rtc_datagram_routes_indexed),
            LabeledValue::new("scan", snapshot.rtc_datagram_routes_scan),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_rtc_datagram_drops_total",
        "Total RTC UDP datagrams dropped before reaching a live user.",
        "reason",
        &[
            LabeledValue::new(
                "recent_miss_cache",
                snapshot.rtc_datagram_drops_recent_miss_cache,
            ),
            LabeledValue::new(
                "source_rate_limited",
                snapshot.rtc_datagram_drops_source_rate_limited,
            ),
            LabeledValue::new("no_user", snapshot.rtc_datagram_drops_no_user),
            LabeledValue::new("malformed", snapshot.rtc_datagram_drops_malformed),
        ],
    );
    append_counter(
        output,
        "osfu_rtc_datagram_fallback_scans_total",
        "Total fallback scans across RTC users for UDP datagram routing.",
        snapshot.rtc_datagram_fallback_scans,
    );
    append_counter(
        output,
        "osfu_rtc_datagram_scan_users_total",
        "Total RTC users examined by UDP fallback scans.",
        snapshot.rtc_datagram_scan_users,
    );
}

pub(super) fn append_rtc_route_control_metrics(
    output: &mut String,
    snapshot: &RuntimeMetricsSnapshot,
) {
    append_labeled_counter_family(
        output,
        "osfu_rtc_route_control_total",
        "Total RTC route-control decisions observed at the transport boundary.",
        "outcome",
        &[
            LabeledValue::new("absorbed", snapshot.rtc_route_control_absorbed),
            LabeledValue::new("forwarded", snapshot.rtc_route_control_forwarded),
            LabeledValue::new(
                "route_gated_relay_drop",
                snapshot.rtc_route_control_route_gated_relay_drops,
            ),
            LabeledValue::new("layer_allowed", snapshot.rtc_route_control_layer_allowed),
            LabeledValue::new("layer_dropped", snapshot.rtc_route_control_layer_dropped),
        ],
    );
}
