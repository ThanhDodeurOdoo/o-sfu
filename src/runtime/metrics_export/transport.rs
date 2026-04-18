use crate::runtime::metrics::RuntimeMetricsSnapshot;

use super::shared::{
    HistogramBucketValue, LabeledGaugeValue, LabeledValue, LabeledValue2, append_counter,
    append_histogram, append_labeled_counter_family, append_labeled_counter_family_2,
    append_labeled_gauge_family,
};

pub(super) fn append_transport_health_gauges(
    output: &mut String,
    snapshot: &RuntimeMetricsSnapshot,
) {
    append_labeled_gauge_family(
        output,
        "osfu_transport_health_sessions",
        "Current number of transport sessions by observed health state.",
        "state",
        &[
            LabeledGaugeValue::new("connected", snapshot.connected_transport_sessions),
            LabeledGaugeValue::new("disconnected", snapshot.disconnected_transport_sessions),
        ],
    );
}

pub(super) fn append_transport_health_transition_metrics(
    output: &mut String,
    snapshot: &RuntimeMetricsSnapshot,
) {
    append_labeled_counter_family_2(
        output,
        "osfu_transport_health_transitions_total",
        "Total transport health-state transitions observed from the transport adapter.",
        ("from", "to"),
        &[
            LabeledValue2::new(
                "unset",
                "connected",
                snapshot.transport_health_transitions_unset_to_connected,
            ),
            LabeledValue2::new(
                "unset",
                "disconnected",
                snapshot.transport_health_transitions_unset_to_disconnected,
            ),
            LabeledValue2::new(
                "connected",
                "disconnected",
                snapshot.transport_health_transitions_connected_to_disconnected,
            ),
            LabeledValue2::new(
                "disconnected",
                "connected",
                snapshot.transport_health_transitions_disconnected_to_connected,
            ),
            LabeledValue2::new(
                "connected",
                "unset",
                snapshot.transport_health_transitions_connected_to_unset,
            ),
            LabeledValue2::new(
                "disconnected",
                "unset",
                snapshot.transport_health_transitions_disconnected_to_unset,
            ),
        ],
    );
}

pub(super) fn append_rtp_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family(
        output,
        "osfu_rtp_packets_total",
        "Total RTP packets processed by flow direction.",
        "direction",
        &[
            LabeledValue::new("ingress", snapshot.rtp_packets_ingress),
            LabeledValue::new("egress", snapshot.rtp_packets_egress),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_rtp_payload_bytes_total",
        "Total RTP payload bytes processed by flow direction.",
        "direction",
        &[
            LabeledValue::new("ingress", snapshot.rtp_payload_bytes_ingress),
            LabeledValue::new("egress", snapshot.rtp_payload_bytes_egress),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_rtp_forwarded_packets_total",
        "Total RTP packet fan-out operations by forwarding destination.",
        "destination",
        &[
            LabeledValue::new("local_rtc", snapshot.rtp_forwarded_packets_local_rtc),
            LabeledValue::new("recording", snapshot.rtp_forwarded_packets_recording),
            LabeledValue::new(
                "intra_node_relay",
                snapshot.rtp_forwarded_packets_intra_node_relay,
            ),
            LabeledValue::new(
                "inter_node_relay",
                snapshot.rtp_forwarded_packets_inter_node_relay,
            ),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_rtp_forwarded_payload_bytes_total",
        "Total RTP payload bytes fanned out by forwarding destination.",
        "destination",
        &[
            LabeledValue::new("local_rtc", snapshot.rtp_forwarded_payload_bytes_local_rtc),
            LabeledValue::new("recording", snapshot.rtp_forwarded_payload_bytes_recording),
            LabeledValue::new(
                "intra_node_relay",
                snapshot.rtp_forwarded_payload_bytes_intra_node_relay,
            ),
            LabeledValue::new(
                "inter_node_relay",
                snapshot.rtp_forwarded_payload_bytes_inter_node_relay,
            ),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_rtp_relay_overload_drops_total",
        "Total RTP relay packets dropped because the bounded relay mailbox was full.",
        "destination",
        &[
            LabeledValue::new(
                "intra_node_relay",
                snapshot.rtp_relay_overload_drops_intra_node_relay,
            ),
            LabeledValue::new(
                "inter_node_relay",
                snapshot.rtp_relay_overload_drops_inter_node_relay,
            ),
        ],
    );
}

pub(super) fn append_transport_lifecycle_metrics(
    output: &mut String,
    snapshot: &RuntimeMetricsSnapshot,
) {
    append_labeled_counter_family(
        output,
        "osfu_transport_ice_state_changes_total",
        "Total RTC ICE state-change events observed from the transport adapter.",
        "state",
        &[
            LabeledValue::new("new", snapshot.transport_ice_state_changes_new),
            LabeledValue::new("checking", snapshot.transport_ice_state_changes_checking),
            LabeledValue::new("connected", snapshot.transport_ice_state_changes_connected),
            LabeledValue::new("completed", snapshot.transport_ice_state_changes_completed),
            LabeledValue::new(
                "disconnected",
                snapshot.transport_ice_state_changes_disconnected,
            ),
        ],
    );
    append_counter(
        output,
        "osfu_transport_dtls_connected_total",
        "Total RTC DTLS-connected events observed from the transport adapter.",
        snapshot.transport_dtls_connected,
    );
    append_histogram(
        output,
        "osfu_transport_session_lifetime_seconds",
        "Lifetime of closed RTC transport sessions observed at cold-path teardown.",
        &[
            HistogramBucketValue::new("1", snapshot.transport_session_lifetime_le_1_second),
            HistogramBucketValue::new("10", snapshot.transport_session_lifetime_le_10_seconds),
            HistogramBucketValue::new("60", snapshot.transport_session_lifetime_le_60_seconds),
            HistogramBucketValue::new("300", snapshot.transport_session_lifetime_le_300_seconds),
        ],
        snapshot.transport_session_lifetime_sum_micros,
        snapshot.transport_session_lifetime_count,
    );
}
