use super::metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};
use o_sfu_protocol::signaling::WebSocketCloseCode;

pub(super) const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub(super) fn render_prometheus(metrics: &RuntimeMetrics) -> String {
    render_snapshot(&metrics.snapshot())
}

fn render_snapshot(snapshot: &RuntimeMetricsSnapshot) -> String {
    let mut output = String::with_capacity(5120);
    append_http_metrics(&mut output, snapshot);
    append_ws_connection_metrics(&mut output, snapshot);
    append_ws_loop_metrics(&mut output, snapshot);
    append_ws_bus_metrics(&mut output, snapshot);
    append_live_gauges(&mut output, snapshot);
    append_recording_metrics(&mut output, snapshot);
    append_transport_health_gauges(&mut output, snapshot);
    append_transport_health_transition_metrics(&mut output, snapshot);
    append_rtp_metrics(&mut output, snapshot);
    append_transport_lifecycle_metrics(&mut output, snapshot);
    append_rtc_datagram_metrics(&mut output, snapshot);
    append_rtc_route_control_metrics(&mut output, snapshot);
    output
}

fn append_http_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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

fn append_ws_connection_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family(
        output,
        "osfu_ws_connections_total",
        "Total websocket connections observed at each handshake stage.",
        "stage",
        &[
            LabeledValue::new("accepted", snapshot.ws_connections_accepted),
            LabeledValue::new(
                "credentials_received",
                snapshot.ws_handshake_credentials_received,
            ),
            LabeledValue::new("joined", snapshot.ws_sessions_joined),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_handshake_rejections_total",
        "Total websocket handshake rejections by close code bucket.",
        "close_code",
        &[
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::AuthTimeout),
                snapshot.ws_handshake_rejected_timeout,
            ),
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::AuthFailed),
                snapshot.ws_handshake_rejected_authentication_failed,
            ),
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::ProtocolError),
                snapshot.ws_handshake_rejected_protocol_error,
            ),
            LabeledValue::new(
                close_code_label(WebSocketCloseCode::ChannelFull),
                snapshot.ws_handshake_rejected_channel_full,
            ),
            LabeledValue::new("error", snapshot.ws_handshake_rejected_error),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_startup_failures_total",
        "Total websocket startup failures before the steady-state session loop.",
        "kind",
        &[
            LabeledValue::new("startup_send", snapshot.ws_startup_send_failures),
            LabeledValue::new(
                "session_initialize",
                snapshot.ws_session_initialize_failures,
            ),
        ],
    );
}

fn append_ws_loop_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_counter(
        output,
        "osfu_ws_session_loops_started_total",
        "Total websocket session loops started after a successful join.",
        snapshot.ws_session_loops_started,
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_session_loop_exits_total",
        "Total websocket session loop exits by reason.",
        "reason",
        &[
            LabeledValue::new("peer_closed", snapshot.ws_session_loop_exits_peer_closed),
            LabeledValue::new("reader_error", snapshot.ws_session_loop_exits_reader_error),
            LabeledValue::new("bus_break", snapshot.ws_session_loop_exits_bus_break),
            LabeledValue::new("ping_timeout", snapshot.ws_session_loop_exits_ping_timeout),
            LabeledValue::new(
                "transport_disconnected",
                snapshot.ws_session_loop_exits_transport_disconnected,
            ),
            LabeledValue::new(
                "outbound_channel_closed",
                snapshot.ws_session_loop_exits_outbound_channel_closed,
            ),
            LabeledValue::new(
                "outbound_close_signal",
                snapshot.ws_session_loop_exits_outbound_close_signal,
            ),
            LabeledValue::new(
                "outbound_message_send_failure",
                snapshot.ws_session_loop_exits_outbound_message_send_failure,
            ),
        ],
    );
}

fn append_ws_bus_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_batches_total",
        "Total websocket signaling batches processed by direction.",
        "direction",
        &[
            LabeledValue::new("received", snapshot.ws_bus_batches_received),
            LabeledValue::new("sent", snapshot.ws_bus_batches_sent),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_envelopes_total",
        "Total websocket signaling envelopes processed by direction.",
        "direction",
        &[
            LabeledValue::new("received", snapshot.ws_bus_envelopes_received),
            LabeledValue::new("sent", snapshot.ws_bus_envelopes_sent),
        ],
    );
    append_counter(
        output,
        "osfu_ws_bus_parse_failures_total",
        "Total websocket signaling parse failures.",
        snapshot.ws_bus_parse_failures,
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_failures_total",
        "Total websocket signaling failures by kind.",
        "kind",
        &[
            LabeledValue::new("invalid_input", snapshot.ws_bus_invalid_input_failures),
            LabeledValue::new(
                "unsupported_feature",
                snapshot.ws_bus_unsupported_feature_failures,
            ),
            LabeledValue::new("send", snapshot.ws_bus_send_failures),
        ],
    );
    append_labeled_counter_family(
        output,
        "osfu_ws_bus_client_frames_total",
        "Total client websocket signaling frames by kind.",
        "kind",
        &[
            LabeledValue::new("request", snapshot.ws_bus_client_requests),
            LabeledValue::new("message", snapshot.ws_bus_client_messages),
        ],
    );
}

fn append_live_gauges(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_gauge(
        output,
        "osfu_channels_active",
        "Current number of live channels owned by this runtime.",
        snapshot.active_channels,
    );
    append_gauge(
        output,
        "osfu_sessions_active",
        "Current number of live channel sessions owned by this runtime.",
        snapshot.active_sessions,
    );
    append_gauge(
        output,
        "osfu_transport_sessions_active",
        "Current number of live RTC transport sessions on this runtime.",
        snapshot.active_transport_sessions,
    );
}

fn append_recording_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
    append_labeled_counter_family_2(
        output,
        "osfu_recording_actions_total",
        "Total recording control actions by action and outcome.",
        ("action", "outcome"),
        &[
            LabeledValue2::new("start", "accepted", snapshot.recording_start_accepted),
            LabeledValue2::new("start", "rejected", snapshot.recording_start_rejected),
            LabeledValue2::new("stop", "accepted", snapshot.recording_stop_accepted),
            LabeledValue2::new("stop", "rejected", snapshot.recording_stop_rejected),
        ],
    );
    append_gauge(
        output,
        "osfu_recording_channels_active",
        "Current number of channels with an active recording session.",
        snapshot.active_recording_channels,
    );
    append_counter(
        output,
        "osfu_recording_captured_packets_total",
        "Total packets accepted by the recording capture path.",
        snapshot.recording_captured_packets,
    );
    append_counter(
        output,
        "osfu_recording_captured_streams_total",
        "Total unique media streams first seen by the recording capture path.",
        snapshot.recording_captured_streams,
    );
}

fn append_transport_health_gauges(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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

fn append_transport_health_transition_metrics(
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

fn append_rtp_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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

fn append_transport_lifecycle_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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

fn append_rtc_datagram_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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
        "Total RTC UDP datagrams dropped before reaching a live session.",
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
            LabeledValue::new("no_session", snapshot.rtc_datagram_drops_no_session),
            LabeledValue::new("malformed", snapshot.rtc_datagram_drops_malformed),
        ],
    );
    append_counter(
        output,
        "osfu_rtc_datagram_fallback_scans_total",
        "Total fallback scans across RTC sessions for UDP datagram routing.",
        snapshot.rtc_datagram_fallback_scans,
    );
    append_counter(
        output,
        "osfu_rtc_datagram_scan_sessions_total",
        "Total RTC sessions examined by UDP fallback scans.",
        snapshot.rtc_datagram_scan_sessions,
    );
}

fn append_rtc_route_control_metrics(output: &mut String, snapshot: &RuntimeMetricsSnapshot) {
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

#[derive(Clone, Copy)]
struct LabeledValue {
    label_value: &'static str,
    value: u64,
}

impl LabeledValue {
    const fn new(label_value: &'static str, value: u64) -> Self {
        Self { label_value, value }
    }
}

#[derive(Clone, Copy)]
struct LabeledValue2 {
    first_label_value: &'static str,
    second_label_value: &'static str,
    value: u64,
}

impl LabeledValue2 {
    const fn new(
        first_label_value: &'static str,
        second_label_value: &'static str,
        value: u64,
    ) -> Self {
        Self {
            first_label_value,
            second_label_value,
            value,
        }
    }
}

#[derive(Clone, Copy)]
struct LabeledGaugeValue {
    label_value: &'static str,
    value: i64,
}

impl LabeledGaugeValue {
    const fn new(label_value: &'static str, value: i64) -> Self {
        Self { label_value, value }
    }
}

#[derive(Clone, Copy)]
struct HistogramBucketValue {
    upper_bound: &'static str,
    value: u64,
}

impl HistogramBucketValue {
    const fn new(upper_bound: &'static str, value: u64) -> Self {
        Self { upper_bound, value }
    }
}

fn append_counter(output: &mut String, name: &str, help: &str, value: u64) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    output.push_str(name);
    output.push(' ');
    append_u64(output, value);
    output.push('\n');
}

fn append_gauge(output: &mut String, name: &str, help: &str, value: i64) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" gauge\n");
    output.push_str(name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

fn append_labeled_counter_family(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    values: &[LabeledValue],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    for value in values {
        output.push_str(name);
        output.push('{');
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\"} ");
        append_u64(output, value.value);
        output.push('\n');
    }
}

fn append_labeled_counter_family_2(
    output: &mut String,
    name: &str,
    help: &str,
    label_names: (&str, &str),
    values: &[LabeledValue2],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    for value in values {
        output.push_str(name);
        output.push('{');
        output.push_str(label_names.0);
        output.push_str("=\"");
        output.push_str(value.first_label_value);
        output.push_str("\",");
        output.push_str(label_names.1);
        output.push_str("=\"");
        output.push_str(value.second_label_value);
        output.push_str("\"} ");
        append_u64(output, value.value);
        output.push('\n');
    }
}

fn append_labeled_gauge_family(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    values: &[LabeledGaugeValue],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" gauge\n");
    for value in values {
        output.push_str(name);
        output.push('{');
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\"} ");
        output.push_str(&value.value.to_string());
        output.push('\n');
    }
}

fn append_histogram(
    output: &mut String,
    name: &str,
    help: &str,
    buckets: &[HistogramBucketValue],
    sum_micros: u64,
    count: u64,
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" histogram\n");
    for bucket in buckets {
        output.push_str(name);
        output.push_str("_bucket{le=\"");
        output.push_str(bucket.upper_bound);
        output.push_str("\"} ");
        append_u64(output, bucket.value);
        output.push('\n');
    }
    output.push_str(name);
    output.push_str("_bucket{le=\"+Inf\"} ");
    append_u64(output, count);
    output.push('\n');
    output.push_str(name);
    output.push_str("_sum ");
    append_seconds_from_micros(output, sum_micros);
    output.push('\n');
    output.push_str(name);
    output.push_str("_count ");
    append_u64(output, count);
    output.push('\n');
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

const fn close_code_label(close_code: WebSocketCloseCode) -> &'static str {
    match close_code {
        WebSocketCloseCode::AuthTimeout => "auth_timeout",
        WebSocketCloseCode::AuthFailed => "auth_failed",
        WebSocketCloseCode::ProtocolError => "protocol_error",
        WebSocketCloseCode::ChannelFull => "channel_full",
        WebSocketCloseCode::Error => "error",
        WebSocketCloseCode::Clean => "clean",
        WebSocketCloseCode::Leaving => "leaving",
        WebSocketCloseCode::Kicked => "kicked",
    }
}

#[cfg(test)]
mod tests {
    use super::{PROMETHEUS_CONTENT_TYPE, render_prometheus};
    use crate::{
        runtime::metrics::{
            RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome,
            RtpForwardDestinationKind, RuntimeMetrics, TransportIceState, WsSessionLoopExitReason,
        },
        runtime::rtc_adapter::TransportSessionHealth,
    };
    use o_sfu_protocol::signaling::WebSocketCloseCode;
    use std::time::Duration;

    fn assert_http_and_websocket_metrics(rendered: &str) {
        assert!(rendered.contains("# TYPE osfu_http_noop_requests_total counter"));
        assert!(rendered.contains("osfu_http_noop_requests_total 1"));
        assert!(rendered.contains("osfu_http_metrics_requests_total 1"));
        assert!(
            rendered
                .contains("osfu_ws_handshake_rejections_total{close_code=\"protocol_error\"} 1")
        );
        assert!(
            rendered
                .contains("osfu_ws_session_loop_exits_total{reason=\"transport_disconnected\"} 1")
        );
        assert!(rendered.contains("osfu_ws_bus_batches_total{direction=\"received\"} 1"));
        assert!(rendered.contains("osfu_ws_bus_envelopes_total{direction=\"received\"} 2"));
        assert!(rendered.contains("osfu_ws_bus_failures_total{kind=\"send\"} 1"));
    }

    fn assert_live_and_recording_metrics(rendered: &str) {
        assert!(rendered.contains("# TYPE osfu_channels_active gauge"));
        assert!(rendered.contains("osfu_sessions_active 2"));
        assert!(rendered.contains("osfu_recording_channels_active 1"));
        assert!(rendered.contains("osfu_transport_sessions_active 1"));
        assert!(rendered.contains("osfu_transport_health_sessions{state=\"connected\"} 1"));
        assert!(
            rendered
                .contains("osfu_recording_actions_total{action=\"start\",outcome=\"accepted\"} 1")
        );
        assert!(
            rendered
                .contains("osfu_recording_actions_total{action=\"stop\",outcome=\"rejected\"} 1")
        );
        assert!(rendered.contains("osfu_recording_captured_packets_total 1"));
        assert!(rendered.contains("osfu_recording_captured_streams_total 1"));
    }

    fn assert_transport_lifecycle_metrics(rendered: &str) {
        assert!(rendered.contains(
            "osfu_transport_health_transitions_total{from=\"unset\",to=\"connected\"} 1"
        ));
        assert!(rendered.contains("osfu_rtp_packets_total{direction=\"ingress\"} 1"));
        assert!(rendered.contains("osfu_rtp_payload_bytes_total{direction=\"egress\"} 900"));
        assert!(rendered.contains("osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"} 1"));
        assert!(
            rendered
                .contains("osfu_rtp_forwarded_payload_bytes_total{destination=\"recording\"} 700")
        );
        assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"absorbed\"} 1"));
        assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"forwarded\"} 1"));
        assert!(rendered.contains("osfu_transport_ice_state_changes_total{state=\"checking\"} 1"));
        assert!(rendered.contains("osfu_transport_ice_state_changes_total{state=\"connected\"} 1"));
        assert!(rendered.contains("osfu_transport_dtls_connected_total 1"));
        assert!(rendered.contains("osfu_transport_session_lifetime_seconds_bucket{le=\"1\"} 0"));
        assert!(rendered.contains("osfu_transport_session_lifetime_seconds_bucket{le=\"10\"} 1"));
        assert!(rendered.contains("osfu_transport_session_lifetime_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(rendered.contains("osfu_transport_session_lifetime_seconds_sum 1.5"));
        assert!(rendered.contains("osfu_transport_session_lifetime_seconds_count 1"));
    }

    fn sample_metrics() -> RuntimeMetrics {
        let metrics = RuntimeMetrics::default();
        metrics.record_http_noop_request();
        metrics.record_http_metrics_request();
        metrics.record_ws_connection_accepted();
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
        metrics.record_ws_session_loop_exit(WsSessionLoopExitReason::TransportDisconnected);
        metrics.record_ws_bus_batch_received(2);
        metrics.record_ws_bus_send_failure();
        metrics.add_active_channels(1);
        metrics.add_active_sessions(2);
        metrics.add_active_recording_channels(1);
        metrics.add_active_transport_sessions(1);
        metrics.record_transport_health_transition(None, Some(TransportSessionHealth::Connected));
        metrics.record_recording_start_accepted();
        metrics.record_recording_stop_rejected();
        metrics.record_recording_captured_packet();
        metrics.record_recording_captured_stream();
        metrics.record_rtp_ingress(1200);
        metrics.record_rtp_egress(900);
        metrics.record_rtp_forwarded(RtpForwardDestinationKind::LocalRtc, 900);
        metrics.record_rtp_forwarded(RtpForwardDestinationKind::Recording, 700);
        metrics.record_rtp_forwarded(RtpForwardDestinationKind::IntraNodeRelay, 500);
        metrics.record_rtp_forwarded(RtpForwardDestinationKind::InterNodeRelay, 300);
        metrics.record_transport_ice_state_change(TransportIceState::Checking);
        metrics.record_transport_ice_state_change(TransportIceState::Connected);
        metrics.record_transport_dtls_connected();
        metrics.record_transport_session_lifetime(Duration::from_millis(1500));
        metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
        metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
        metrics.record_rtc_datagram_fallback_scan(4);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
        metrics
    }

    #[test]
    fn prometheus_export_renders_existing_metric_families() {
        let rendered = render_prometheus(&sample_metrics());

        assert_eq!(
            PROMETHEUS_CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8"
        );
        assert_http_and_websocket_metrics(&rendered);
        assert_live_and_recording_metrics(&rendered);
        assert_transport_lifecycle_metrics(&rendered);
    }

    #[test]
    fn prometheus_export_renders_rtc_datagram_metric_families() {
        let rendered = render_prometheus(&sample_metrics());

        assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"indexed\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"scan\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_drops_total{reason=\"malformed\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_fallback_scans_total 1"));
        assert!(rendered.contains("osfu_rtc_datagram_scan_sessions_total 4"));
        assert!(
            rendered.contains("osfu_rtc_route_control_total{outcome=\"route_gated_relay_drop\"} 1")
        );
        assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"layer_allowed\"} 1"));
        assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"layer_dropped\"} 1"));
    }
}
