use super::metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};
use crate::signaling::protocol::WebSocketCloseCode;

pub(super) const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub(super) fn render_prometheus(metrics: &RuntimeMetrics) -> String {
    render_snapshot(&metrics.snapshot())
}

fn render_snapshot(snapshot: &RuntimeMetricsSnapshot) -> String {
    let mut output = String::with_capacity(4096);
    append_http_metrics(&mut output, snapshot);
    append_ws_connection_metrics(&mut output, snapshot);
    append_ws_loop_metrics(&mut output, snapshot);
    append_ws_bus_metrics(&mut output, snapshot);
    append_live_gauges(&mut output, snapshot);
    append_recording_metrics(&mut output, snapshot);
    append_transport_health_gauges(&mut output, snapshot);
    append_rtp_metrics(&mut output, snapshot);
    append_rtc_datagram_metrics(&mut output, snapshot);
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

fn append_u64(output: &mut String, value: u64) {
    output.push_str(&value.to_string());
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
            RtcDatagramDropReason, RtcDatagramRoutePath, RuntimeMetrics, WsSessionLoopExitReason,
        },
        runtime::rtc_adapter::TransportSessionHealth,
        signaling::protocol::WebSocketCloseCode,
    };

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
        metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
        metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
        metrics.record_rtc_datagram_fallback_scan(4);
        metrics
    }

    #[test]
    fn prometheus_export_renders_existing_metric_families() {
        let rendered = render_prometheus(&sample_metrics());

        assert_eq!(
            PROMETHEUS_CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8"
        );
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
        assert!(rendered.contains("osfu_rtp_packets_total{direction=\"ingress\"} 1"));
        assert!(rendered.contains("osfu_rtp_payload_bytes_total{direction=\"egress\"} 900"));
    }

    #[test]
    fn prometheus_export_renders_rtc_datagram_metric_families() {
        let rendered = render_prometheus(&sample_metrics());

        assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"indexed\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"scan\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_drops_total{reason=\"malformed\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_fallback_scans_total 1"));
        assert!(rendered.contains("osfu_rtc_datagram_scan_sessions_total 4"));
    }
}
