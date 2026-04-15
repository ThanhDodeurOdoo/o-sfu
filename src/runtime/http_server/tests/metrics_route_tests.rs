use super::fixtures::*;

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

    assert!(payload.contains("# TYPE osfu_http_noop_requests_total counter"));
    assert!(payload.contains("osfu_http_noop_requests_total 1"));
    assert!(payload.contains("osfu_http_disconnect_requests_total 1"));
    assert!(
        payload.contains("osfu_http_disconnect_responses_total{status=\"unprocessable_entity\"} 1")
    );
    assert!(payload.contains("osfu_http_metrics_requests_total 1"));
    assert!(payload.contains("osfu_channels_active 0"));
    assert!(payload.contains("osfu_sessions_active 0"));
    assert!(payload.contains("osfu_recording_channels_active 0"));
    assert!(payload.contains("osfu_transport_sessions_active 0"));
    assert!(payload.contains("osfu_transport_health_sessions{state=\"connected\"} 0"));
    assert!(
        payload.contains("osfu_recording_actions_total{action=\"start\",outcome=\"accepted\"} 0")
    );
    assert!(payload.contains("osfu_rtp_packets_total{direction=\"ingress\"} 0"));

    let snapshot = state.metrics.snapshot();
    assert_eq!(snapshot.http_noop_requests, 1);
    assert_eq!(snapshot.http_disconnect_requests, 1);
    assert_eq!(snapshot.http_disconnect_unprocessable_entity, 1);
    assert_eq!(snapshot.http_metrics_requests, 1);
    assert_eq!(snapshot.active_channels, 0);
    assert_eq!(snapshot.active_sessions, 0);
    assert_eq!(snapshot.active_recording_channels, 0);
    assert_eq!(snapshot.active_transport_sessions, 0);
    assert_eq!(snapshot.connected_transport_sessions, 0);
    assert_eq!(snapshot.recording_start_accepted, 0);
}
