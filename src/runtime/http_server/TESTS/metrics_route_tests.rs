use super::fixtures::*;
use crate::runtime::metrics::RuntimeMetricsSnapshot;

fn assert_metrics_payload(payload: &str) {
    assert_http_metrics_payload(payload);
    assert_transport_metrics_payload(payload);
}

fn assert_http_metrics_payload(payload: &str) {
    assert!(payload.contains("# TYPE osfu_http_noop_requests_total counter"));
    assert!(payload.contains("osfu_http_noop_requests_total 1"));
    assert!(payload.contains("osfu_http_disconnect_requests_total 1"));
    assert!(
        payload.contains("osfu_http_disconnect_responses_total{status=\"unprocessable_entity\"} 1")
    );
    assert!(payload.contains("osfu_http_metrics_requests_total 1"));
    assert!(payload.contains("# TYPE osfu_http_inflight_requests gauge"));
    assert!(payload.contains("osfu_http_inflight_requests{route=\"metrics\"} 1"));
    assert!(payload.contains("# TYPE osfu_http_request_duration_seconds histogram"));
    assert!(payload.contains("osfu_http_request_duration_seconds_count{route=\"noop\"} 1"));
    assert!(payload.contains("# TYPE osfu_ws_handshake_duration_seconds histogram"));
    assert!(payload.contains("osfu_ws_handshake_duration_seconds_count 0"));
    assert!(payload.contains("osfu_rooms_active 0"));
    assert!(payload.contains("osfu_users_active 0"));
    assert!(payload.contains("osfu_publications_active 0"));
    assert!(payload.contains("osfu_subscriptions_active 0"));
    assert!(payload.contains("osfu_recording_rooms_active 0"));
}

fn assert_transport_metrics_payload(payload: &str) {
    assert!(payload.contains("osfu_transport_users_active 0"));
    assert!(payload.contains("osfu_transport_health_users{state=\"connected\"} 0"));
    assert!(
        payload
            .contains("osfu_transport_health_transitions_total{from=\"unset\",to=\"connected\"} 0")
    );
    assert!(
        payload.contains("osfu_recording_actions_total{action=\"start\",outcome=\"accepted\"} 0")
    );
    assert!(payload.contains("osfu_rtp_packets_total{direction=\"ingress\"} 0"));
    assert!(payload.contains("osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"} 0"));
    assert!(payload.contains("osfu_transport_ice_state_changes_total{state=\"checking\"} 0"));
    assert!(payload.contains("osfu_transport_dtls_connected_total 0"));
    assert!(payload.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"1\"} 0"));
    assert!(payload.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"+Inf\"} 0"));
    assert!(payload.contains("osfu_transport_user_lifetime_seconds_sum 0.0"));
    assert!(payload.contains("osfu_transport_user_lifetime_seconds_count 0"));
    assert!(payload.contains("osfu_source_selection_updates_total{selector=\"encoding\"} 0"));
    assert!(payload.contains("osfu_budget_solver_outcomes_total{outcome=\"paused\"} 0"));
}

fn assert_metrics_snapshot(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.http_noop_requests(), 1);
    assert_eq!(snapshot.http_disconnect_requests(), 1);
    assert_eq!(snapshot.http_disconnect_unprocessable_entity(), 1);
    assert_eq!(snapshot.http_metrics_requests(), 1);
    assert_eq!(snapshot.http_inflight().metrics, 0);
    assert_eq!(snapshot.http_request_duration().noop.count, 1);
    assert_eq!(snapshot.http_request_duration().metrics.count, 1);
    assert_eq!(snapshot.ws_handshake_duration().count, 0);
    assert_eq!(snapshot.active_rooms(), 0);
    assert_eq!(snapshot.active_users(), 0);
    assert_eq!(snapshot.active_publications(), 0);
    assert_eq!(snapshot.active_subscriptions(), 0);
    assert_eq!(snapshot.active_recording_rooms(), 0);
    assert_eq!(snapshot.active_transport_users(), 0);
    assert_eq!(snapshot.connected_transport_users(), 0);
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_connected(),
        0
    );
    assert_eq!(snapshot.transport_ice_state_changes_checking(), 0);
    assert_eq!(snapshot.transport_dtls_connected(), 0);
    assert_eq!(snapshot.transport_user_lifetime_count(), 0);
    assert_eq!(snapshot.recording_start_accepted(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 0);
}

#[tokio::test]
async fn metrics_route_exports_prometheus_text_for_runtime_counters() -> TestResult {
    let state = test_state();

    route_status(
        &state,
        Request::get(NOOP_PATH),
        Body::empty(),
        StatusCode::OK,
        "noop request should complete",
    )
    .await?;

    route_status(
        &state,
        Request::post(DISCONNECT_PATH),
        Body::from("invalid-token"),
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid disconnect request should complete",
    )
    .await?;

    let metrics_response = route_response(
        &state,
        Request::get(METRICS_PATH),
        Body::empty(),
        StatusCode::OK,
        "metrics request should complete",
    )
    .await?;
    assert_eq!(
        metrics_response.headers().get(header::CONTENT_TYPE),
        Some(&header::HeaderValue::from_static(
            "text/plain; version=0.0.4; charset=utf-8"
        ))
    );
    let payload = response_text(metrics_response, "metrics payload should decode").await?;
    assert_metrics_payload(&payload);

    let snapshot = state.metrics.snapshot();
    assert_metrics_snapshot(&snapshot);
    Ok(())
}
