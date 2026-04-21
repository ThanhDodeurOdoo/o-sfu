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
    assert!(payload.contains("osfu_channels_active 0"));
    assert!(payload.contains("osfu_sessions_active 0"));
    assert!(payload.contains("osfu_publications_active 0"));
    assert!(payload.contains("osfu_subscriptions_active 0"));
    assert!(payload.contains("osfu_recording_channels_active 0"));
}

fn assert_transport_metrics_payload(payload: &str) {
    assert!(payload.contains("osfu_transport_sessions_active 0"));
    assert!(payload.contains("osfu_transport_health_sessions{state=\"connected\"} 0"));
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
    assert!(payload.contains("osfu_transport_session_lifetime_seconds_bucket{le=\"1\"} 0"));
    assert!(payload.contains("osfu_transport_session_lifetime_seconds_bucket{le=\"+Inf\"} 0"));
    assert!(payload.contains("osfu_transport_session_lifetime_seconds_sum 0.0"));
    assert!(payload.contains("osfu_transport_session_lifetime_seconds_count 0"));
}

fn assert_metrics_snapshot(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.http_noop_requests, 1);
    assert_eq!(snapshot.http_disconnect_requests, 1);
    assert_eq!(snapshot.http_disconnect_unprocessable_entity, 1);
    assert_eq!(snapshot.http_metrics_requests, 1);
    assert_eq!(snapshot.http_inflight.metrics, 0);
    assert_eq!(snapshot.http_request_duration.noop.count, 1);
    assert_eq!(snapshot.http_request_duration.metrics.count, 1);
    assert_eq!(snapshot.ws_handshake_duration.count, 0);
    assert_eq!(snapshot.active_channels, 0);
    assert_eq!(snapshot.active_sessions, 0);
    assert_eq!(snapshot.active_publications, 0);
    assert_eq!(snapshot.active_subscriptions, 0);
    assert_eq!(snapshot.active_recording_channels, 0);
    assert_eq!(snapshot.active_transport_sessions, 0);
    assert_eq!(snapshot.connected_transport_sessions, 0);
    assert_eq!(snapshot.transport_health_transitions_unset_to_connected, 0);
    assert_eq!(snapshot.transport_ice_state_changes_checking, 0);
    assert_eq!(snapshot.transport_dtls_connected, 0);
    assert_eq!(snapshot.transport_session_lifetime_count, 0);
    assert_eq!(snapshot.recording_start_accepted, 0);
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc, 0);
}

#[tokio::test]
async fn metrics_route_exports_prometheus_text_for_runtime_counters() {
    let state = test_state();

    let noop = build_request(Request::get(NOOP_PATH), Body::empty());
    assert!(noop.is_some());
    let Some(noop) = noop else {
        return;
    };
    let noop_response = app(state.clone()).oneshot(noop).await;
    assert!(noop_response.is_ok());
    let Some(noop_response) = noop_response.ok() else {
        return;
    };
    assert_eq!(noop_response.status(), StatusCode::OK);

    let invalid_disconnect =
        build_request(Request::post(DISCONNECT_PATH), Body::from("invalid-token"));
    assert!(invalid_disconnect.is_some());
    let Some(invalid_disconnect) = invalid_disconnect else {
        return;
    };
    let invalid_disconnect_response = app(state.clone()).oneshot(invalid_disconnect).await;
    assert!(invalid_disconnect_response.is_ok());
    let Some(invalid_disconnect_response) = invalid_disconnect_response.ok() else {
        return;
    };
    assert_eq!(
        invalid_disconnect_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let metrics_request = build_request(Request::get(METRICS_PATH), Body::empty());
    assert!(metrics_request.is_some());
    let Some(metrics_request) = metrics_request else {
        return;
    };
    let metrics_response = app(state.clone()).oneshot(metrics_request).await;
    assert!(metrics_response.is_ok());
    let Some(metrics_response) = metrics_response.ok() else {
        return;
    };
    assert_eq!(metrics_response.status(), StatusCode::OK);
    assert_eq!(
        metrics_response.headers().get(header::CONTENT_TYPE),
        Some(&header::HeaderValue::from_static(
            "text/plain; version=0.0.4; charset=utf-8"
        ))
    );
    let payload = parse_text(metrics_response).await;
    assert!(payload.is_some());
    let Some(payload) = payload else {
        return;
    };
    assert_metrics_payload(&payload);

    let snapshot = state.metrics.snapshot();
    assert_metrics_snapshot(&snapshot);
}
