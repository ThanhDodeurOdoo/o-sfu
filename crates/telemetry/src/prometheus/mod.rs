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
mod tests {
    use std::time::Duration;

    use o_sfu_model::WebSocketCloseCode;

    use super::{PROMETHEUS_CONTENT_TYPE, render_prometheus};
    use crate::metrics::{
        BudgetSolverOutcome, HttpRoute, RtcDatagramDropReason, RtcDatagramRoutePath,
        RtcRouteControlOutcome, RtpForwardDestinationKind, RuntimeMetrics, SourceSelectionKind,
        TransportCleanupFailureKind, TransportHealthState, TransportIceState,
        WsSessionLoopExitReason,
    };

    fn assert_http_and_websocket_metrics(rendered: &str) {
        assert!(rendered.contains("# TYPE osfu_http_noop_requests_total counter"));
        assert!(rendered.contains("osfu_http_noop_requests_total 1"));
        assert!(rendered.contains("osfu_http_metrics_requests_total 1"));
        assert!(rendered.contains("# TYPE osfu_http_inflight_requests gauge"));
        assert!(rendered.contains("osfu_http_inflight_requests{route=\"noop\"} 1"));
        assert!(rendered.contains("# TYPE osfu_http_request_duration_seconds histogram"));
        assert!(
            rendered.contains(
                "osfu_http_request_duration_seconds_bucket{route=\"noop\",le=\"0.05\"} 1"
            )
        );
        assert!(rendered.contains("osfu_http_request_duration_seconds_count{route=\"noop\"} 1"));
        assert!(
            rendered
                .contains("osfu_ws_handshake_rejections_total{close_code=\"protocol_error\"} 1")
        );
        assert!(rendered.contains("# TYPE osfu_ws_handshake_duration_seconds histogram"));
        assert!(rendered.contains("osfu_ws_handshake_duration_seconds_bucket{le=\"0.1\"} 1"));
        assert!(rendered.contains("osfu_ws_auth_duration_seconds_count 1"));
        assert!(
            rendered.contains("osfu_ws_user_initialize_duration_seconds_bucket{le=\"0.25\"} 1")
        );
        assert!(
            rendered.contains("osfu_ws_user_loop_exits_total{reason=\"transport_disconnected\"} 1")
        );
        assert!(rendered.contains("osfu_ws_bus_batches_total{direction=\"received\"} 1"));
        assert!(rendered.contains("osfu_ws_bus_envelopes_total{direction=\"received\"} 2"));
        assert!(rendered.contains("osfu_ws_bus_failures_total{kind=\"send\"} 1"));
    }

    fn assert_live_and_recording_metrics(rendered: &str) {
        assert!(rendered.contains("# TYPE osfu_rooms_active gauge"));
        assert!(rendered.contains("osfu_users_active 2"));
        assert!(rendered.contains("osfu_publications_active 3"));
        assert!(rendered.contains("osfu_subscriptions_active 4"));
        assert!(rendered.contains("osfu_recording_rooms_active 1"));
        assert!(rendered.contains("osfu_transport_users_active 1"));
        assert!(rendered.contains("osfu_transport_health_users{state=\"connected\"} 1"));
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
        assert!(rendered.contains("osfu_source_selection_updates_total{selector=\"encoding\"} 1"));
        assert!(rendered.contains("osfu_budget_solver_outcomes_total{outcome=\"degraded\"} 1"));
        assert!(
            rendered
                .contains("osfu_budget_solver_outcomes_total{outcome=\"protected_over_budget\"} 1")
        );
        assert!(rendered.contains("osfu_transport_ice_state_changes_total{state=\"checking\"} 1"));
        assert!(rendered.contains("osfu_transport_ice_state_changes_total{state=\"connected\"} 1"));
        assert!(rendered.contains("osfu_transport_dtls_connected_total 1"));
        assert!(rendered.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"1\"} 0"));
        assert!(rendered.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"10\"} 1"));
        assert!(rendered.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(rendered.contains("osfu_transport_user_lifetime_seconds_sum 1.5"));
        assert!(rendered.contains("osfu_transport_user_lifetime_seconds_count 1"));
        assert!(rendered.contains("osfu_transport_cleanup_retries_total 1"));
        assert!(rendered.contains("osfu_transport_cleanup_retry_successes_total 1"));
        assert!(
            rendered.contains("osfu_transport_cleanup_failures_total{kind=\"retry_exhausted\"} 1")
        );
    }

    fn sample_metrics() -> RuntimeMetrics {
        let metrics = RuntimeMetrics::default();
        metrics.record_http_noop_request();
        metrics.record_http_metrics_request();
        metrics.add_http_inflight_requests(HttpRoute::Noop, 1);
        metrics.record_http_request_duration(HttpRoute::Noop, Duration::from_millis(25));
        metrics.record_ws_connection_accepted();
        metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
        metrics.record_ws_user_loop_exit(WsSessionLoopExitReason::TransportDisconnected);
        metrics.record_ws_bus_batch_received(2);
        metrics.record_ws_bus_send_failure();
        metrics.record_ws_handshake_duration(Duration::from_millis(80));
        metrics.record_ws_auth_duration(Duration::from_millis(8));
        metrics.record_ws_user_initialize_duration(Duration::from_millis(120));
        metrics.add_active_rooms(1);
        metrics.add_active_users(2);
        metrics.add_active_publications(3);
        metrics.add_active_subscriptions(4);
        metrics.add_active_recording_rooms(1);
        metrics.add_active_transport_users(1);
        metrics.record_transport_health_transition(None, Some(TransportHealthState::Connected));
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
        metrics.record_transport_user_lifetime(Duration::from_millis(1500));
        metrics.record_transport_cleanup_retry_scheduled();
        metrics.record_transport_cleanup_retry_succeeded();
        metrics.record_transport_cleanup_failure(TransportCleanupFailureKind::RetryExhausted);
        metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
        metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
        metrics.record_rtc_datagram_fallback_scan(4);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
        metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
        metrics.record_source_selection_update(SourceSelectionKind::Encoding);
        metrics.record_budget_solver_outcome(BudgetSolverOutcome::Degraded);
        metrics.record_budget_solver_outcome(BudgetSolverOutcome::Paused);
        metrics.record_budget_solver_outcome(BudgetSolverOutcome::Resumed);
        metrics.record_budget_solver_outcome(BudgetSolverOutcome::ProtectedOverBudget);
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
    fn prometheus_export_keeps_rtp_shape_for_worker_recorders() {
        let metrics = RuntimeMetrics::default();
        let first_worker = metrics.register_rtp_worker_for_media_worker(0);
        let second_worker = metrics.register_rtp_worker_for_media_worker(1);

        first_worker.record_ingress(1200);
        first_worker.record_egress(900);
        first_worker.record_forwarded(RtpForwardDestinationKind::LocalRtc, 900);
        second_worker.record_ingress(300);
        second_worker.record_forwarded(RtpForwardDestinationKind::Recording, 300);

        let rendered = render_prometheus(&metrics);

        assert!(rendered.contains("osfu_rtp_packets_total{direction=\"ingress\"} 2"));
        assert!(rendered.contains("osfu_rtp_payload_bytes_total{direction=\"ingress\"} 1500"));
        assert!(rendered.contains("osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"} 1"));
        assert!(
            rendered
                .contains("osfu_rtp_forwarded_payload_bytes_total{destination=\"recording\"} 300")
        );
        assert!(rendered.contains(
            "osfu_worker_rtp_packets_total{media_worker_id=\"0\",direction=\"ingress\"} 1"
        ));
        assert!(rendered.contains(
            "osfu_worker_rtp_packets_total{media_worker_id=\"1\",direction=\"ingress\"} 1"
        ));
        assert!(rendered.contains(
            "osfu_worker_rtp_payload_bytes_total{media_worker_id=\"0\",direction=\"egress\"} 900"
        ));
        assert!(
            rendered
                .contains("osfu_worker_rtp_forwarded_packets_total{media_worker_id=\"0\",destination=\"local_rtc\"} 1")
        );
        assert!(
            rendered
                .contains("osfu_worker_rtp_forwarded_payload_bytes_total{media_worker_id=\"1\",destination=\"recording\"} 300")
        );
    }

    #[test]
    fn prometheus_export_renders_rtc_datagram_metric_families() {
        let rendered = render_prometheus(&sample_metrics());

        assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"indexed\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"scan\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_drops_total{reason=\"malformed\"} 1"));
        assert!(rendered.contains("osfu_rtc_datagram_fallback_scans_total 1"));
        assert!(rendered.contains("osfu_rtc_datagram_scan_users_total 4"));
        assert!(
            rendered.contains("osfu_rtc_route_control_total{outcome=\"route_gated_relay_drop\"} 1")
        );
        assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"layer_allowed\"} 1"));
        assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"layer_dropped\"} 1"));
    }
}
