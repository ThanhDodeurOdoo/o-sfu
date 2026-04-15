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
        runtime::metrics::{RuntimeMetrics, WsSessionLoopExitReason},
        signaling::protocol::WebSocketCloseCode,
    };

    #[test]
    fn prometheus_export_renders_expected_metric_families() {
        let metrics = RuntimeMetrics::default();
        metrics.record_http_noop_request();
        metrics.record_http_metrics_request();
        metrics.record_ws_connection_accepted();
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
        metrics.record_ws_session_loop_exit(WsSessionLoopExitReason::TransportDisconnected);
        metrics.record_ws_bus_batch_received(2);
        metrics.record_ws_bus_send_failure();

        let rendered = render_prometheus(&metrics);

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
    }
}
